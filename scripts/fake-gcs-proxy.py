"""A proxy in front of fake-gcs-server that fills in two things GCS does and it does not.

- XML API multipart uploads. OpenDAL's gcs service writes anything larger than one chunk
  (5 MiB) as one: POST ?uploads, PUT ?partNumber=N&uploadId=I, then POST ?uploadId=I, or
  DELETE ?uploadId=I to abort. fake-gcs-server answers 400 "invalid uploadType". The proxy
  keeps the parts on disk and on completion uploads the object to fake-gcs-server in one JSON
  API request, with the headers the initiate request carried and the generation condition
  (x-goog-if-generation-match) the completion carried.
- Batch requests whose sub-requests end right after the request line, as Google's own
  examples write them and OpenDAL sends its batch deletes. fake-gcs-server reads each with
  Go's http.ReadRequest, which needs the empty line that ends a header section, and answers
  400 "unable to process request". The proxy adds that line.

Every other request passes through unchanged.

    python3 -I scripts/fake-gcs-proxy.py UPSTREAM_URL PORT_FILE

Listens on a free loopback port and writes it to PORT_FILE once it accepts connections.
"""

import hashlib
import http.client
import http.server
import json
import os
import shutil
import signal
import sys
import tempfile
import threading
import urllib.parse
import uuid
import xml.etree.ElementTree as ET
from xml.sax.saxutils import escape

UPSTREAM = urllib.parse.urlsplit(sys.argv[1])
PORT_FILE = sys.argv[2]
PARTS_DIR = tempfile.mkdtemp(prefix="fake-gcs-multipart-")
COPY_CHUNK = 1024 * 1024
# RFC 9110 hop-by-hop fields, which describe one connection and are never forwarded.
HOP_BY_HOP = {
    "connection",
    "keep-alive",
    "proxy-connection",
    "te",
    "trailer",
    "transfer-encoding",
    "upgrade",
}
# Headers of the initiate request and the JSON API object fields they become.
OBJECT_FIELDS = {
    "content-type": "contentType",
    "cache-control": "cacheControl",
    "content-encoding": "contentEncoding",
    "content-disposition": "contentDisposition",
    "content-language": "contentLanguage",
}
META_PREFIX = "x-goog-meta-"

# Upload ID -> (bucket, object name, JSON API object resource without the name).
uploads = {}
uploads_lock = threading.Lock()

# Each handler thread serves one client connection and keeps one upstream connection for it. A
# connection per request would leave every one in TIME_WAIT, and a test that writes, stats and
# deletes a thousand objects then runs the host out of ephemeral ports (EADDRNOTAVAIL).
local = threading.local()


def upstream(method, target, headers, body=None):
    """The upstream's response to a request on this thread's connection. `body` is bytes, or a
    function that returns an iterable of bytes, so that the request can be sent again.

    A reused connection the upstream has closed in the meantime fails before the request is
    read, so it is replaced and the request sent once more."""
    for attempt in (1, 2):
        conn = getattr(local, "conn", None)
        reused = conn is not None
        if not reused:
            conn = local.conn = http.client.HTTPConnection(
                UPSTREAM.hostname, UPSTREAM.port, timeout=300
            )
        try:
            conn.request(method, target, body=body() if callable(body) else body, headers=headers)
            return conn.getresponse()
        except ConnectionError:
            drop_upstream()
            if not reused or attempt == 2:
                raise


def drop_upstream():
    """Closes this thread's upstream connection, which a response not read to its end leaves
    unusable for the next request."""
    conn = getattr(local, "conn", None)
    if conn is not None:
        conn.close()
        local.conn = None


class Handler(http.server.BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"

    def log_message(self, format, *args):
        pass

    def do_GET(self):
        self.forward()

    def do_HEAD(self):
        self.forward()

    def do_PATCH(self):
        self.forward()

    def do_POST(self):
        query = self.query()
        if "uploads" in query:
            self.initiate()
        elif "uploadId" in query:
            self.complete(query["uploadId"][0])
        else:
            self.forward()

    def do_PUT(self):
        query = self.query()
        if "uploadId" in query and "partNumber" in query:
            self.upload_part(query["uploadId"][0], query["partNumber"][0])
        else:
            self.forward()

    def do_DELETE(self):
        query = self.query()
        if "uploadId" in query:
            self.abort(query["uploadId"][0])
        else:
            self.forward()

    def query(self):
        return urllib.parse.parse_qs(urllib.parse.urlsplit(self.path).query, keep_blank_values=True)

    def object_path(self):
        """Bucket and object name of an XML API path, /BUCKET/OBJECT."""
        path = urllib.parse.urlsplit(self.path).path
        bucket, _, name = path.lstrip("/").partition("/")
        return urllib.parse.unquote(bucket), urllib.parse.unquote(name)

    def read_body(self):
        if self.headers.get("Transfer-Encoding", "").lower() == "chunked":
            body = bytearray()
            while True:
                size = int(self.rfile.readline().split(b";")[0], 16)
                if size == 0:
                    while self.rfile.readline() not in (b"\r\n", b"\n", b""):
                        pass
                    return bytes(body)
                body += self.rfile.read(size)
                self.rfile.readline()
        return self.rfile.read(int(self.headers.get("Content-Length") or 0))

    def reply(self, status, body=b"", headers=()):
        self.send_response(status)
        for name, value in headers:
            self.send_header(name, value)
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        if body and self.command != "HEAD":
            self.wfile.write(body)

    def reply_xml(self, status, xml, headers=()):
        body = ('<?xml version="1.0" encoding="UTF-8"?>' + xml).encode()
        self.reply(status, body, [("Content-Type", "application/xml"), *headers])

    def no_such_upload(self):
        self.reply_xml(
            404,
            "<Error><Code>NoSuchUpload</Code>"
            "<Message>The requested upload was not found.</Message></Error>",
        )

    def initiate(self):
        self.read_body()
        bucket, name = self.object_path()
        resource = {}
        metadata = {}
        for header, value in self.headers.items():
            header = header.lower()
            if header in OBJECT_FIELDS:
                resource[OBJECT_FIELDS[header]] = value
            elif header.startswith(META_PREFIX):
                metadata[header[len(META_PREFIX):]] = value
        if metadata:
            resource["metadata"] = metadata
        upload_id = uuid.uuid4().hex
        os.mkdir(os.path.join(PARTS_DIR, upload_id))
        with uploads_lock:
            uploads[upload_id] = (bucket, name, resource)
        self.reply_xml(
            200,
            "<InitiateMultipartUploadResult>"
            f"<Bucket>{escape(bucket)}</Bucket><Key>{escape(name)}</Key>"
            f"<UploadId>{upload_id}</UploadId>"
            "</InitiateMultipartUploadResult>",
        )

    def part_file(self, upload_id, number):
        return os.path.join(PARTS_DIR, upload_id, f"{int(number):05d}")

    def upload_part(self, upload_id, number):
        data = self.read_body()
        with uploads_lock:
            known = upload_id in uploads
        if not known:
            self.no_such_upload()
            return
        with open(self.part_file(upload_id, number), "wb") as part:
            part.write(data)
        self.reply(200, headers=[("ETag", f'"{hashlib.md5(data).hexdigest()}"')])

    def complete(self, upload_id):
        request = ET.fromstring(self.read_body())
        numbers = [e.text.strip() for e in request.iter() if e.tag.endswith("PartNumber")]
        with uploads_lock:
            upload = uploads.get(upload_id)
        if upload is None:
            self.no_such_upload()
            return
        bucket, name, resource = upload
        files = [self.part_file(upload_id, n) for n in numbers]
        if not all(os.path.exists(f) for f in files):
            self.reply_xml(400, "<Error><Code>InvalidPart</Code></Error>")
            return

        # One JSON API multipart/related request: the object resource, then the content.
        boundary = uuid.uuid4().hex
        content_type = resource.get("contentType", "application/octet-stream")
        head = (
            f"--{boundary}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n"
            + json.dumps({**resource, "name": name})
            + f"\r\n--{boundary}\r\nContent-Type: {content_type}\r\n\r\n"
        ).encode()
        tail = f"\r\n--{boundary}--\r\n".encode()
        length = len(head) + sum(os.path.getsize(f) for f in files) + len(tail)

        def body():
            yield head
            for path in files:
                with open(path, "rb") as part:
                    while chunk := part.read(COPY_CHUNK):
                        yield chunk
            yield tail

        target = (
            f"/upload/storage/v1/b/{urllib.parse.quote(bucket, safe='')}/o?uploadType=multipart"
        )
        generation = self.headers.get("x-goog-if-generation-match")
        if generation is not None:
            target += f"&ifGenerationMatch={urllib.parse.quote(generation, safe='')}"
        response = upstream(
            "POST",
            target,
            {
                "Content-Type": f"multipart/related; boundary={boundary}",
                "Content-Length": str(length),
            },
            body,
        )
        answer = response.read()
        if response.status // 100 != 2:
            self.reply(response.status, answer, [("Content-Type", "application/json")])
            return
        etag = json.loads(answer).get("etag", "")
        with uploads_lock:
            uploads.pop(upload_id, None)
        shutil.rmtree(os.path.join(PARTS_DIR, upload_id), ignore_errors=True)
        self.reply_xml(
            200,
            "<CompleteMultipartUploadResult>"
            f"<Bucket>{escape(bucket)}</Bucket><Key>{escape(name)}</Key>"
            f"<ETag>&quot;{escape(etag)}&quot;</ETag>"
            "</CompleteMultipartUploadResult>",
            [("ETag", f'"{etag}"')],
        )

    def abort(self, upload_id):
        self.read_body()
        with uploads_lock:
            known = uploads.pop(upload_id, None) is not None
        if not known:
            self.no_such_upload()
            return
        shutil.rmtree(os.path.join(PARTS_DIR, upload_id), ignore_errors=True)
        self.reply(204)

    def forward(self):
        body = self.read_body() if self.command in ("POST", "PUT", "PATCH") else None
        if body and urllib.parse.urlsplit(self.path).path.startswith("/batch/"):
            body = end_header_sections(body, self.headers.get_boundary())
        headers = {
            name: value
            for name, value in self.headers.items()
            if name.lower() not in HOP_BY_HOP and name.lower() != "content-length"
        }
        if body is not None:
            headers["Content-Length"] = str(len(body))
        response = upstream(self.command, self.path, headers, body)
        try:
            self.relay(response)
        finally:
            if not response.isclosed():
                drop_upstream()

    def relay(self, response):
        self.send_response(response.status)
        for name, value in response.getheaders():
            if name.lower() not in HOP_BY_HOP and name.lower() != "content-length":
                self.send_header(name, value)

        declared = response.getheader("Content-Length")
        if self.command == "HEAD" or response.status in (204, 304):
            # No body, but only reading it marks the response done and frees the connection.
            response.read()
            if declared is not None:
                self.send_header("Content-Length", declared)
            self.end_headers()
        elif declared is None:
            data = response.read()
            self.send_header("Content-Length", str(len(data)))
            self.end_headers()
            self.wfile.write(data)
        else:
            # Passed on as it comes, a body shorter than declared included: the client then
            # sees the connection close early, as it would from fake-gcs-server itself.
            self.send_header("Content-Length", declared)
            self.end_headers()
            try:
                while chunk := response.read1(COPY_CHUNK):
                    self.wfile.write(chunk)
            except http.client.IncompleteRead as short:
                self.wfile.write(short.partial)
            if response.length:
                self.close_connection = True
                drop_upstream()


def end_header_sections(body, boundary):
    """The multipart/mixed batch `body` with an empty line after each sub-request that lacks
    one. A part is its own headers, an empty line, and the embedded request; the CRLF before
    the next delimiter belongs to the delimiter."""
    if not boundary:
        return body
    delimiter = b"--" + boundary.encode()
    parts = body.split(delimiter)
    for i, part in enumerate(parts[1:], start=1):
        if part.startswith(b"--"):
            break
        head, sep, request = part.partition(b"\r\n\r\n")
        request = request.removesuffix(b"\r\n")
        if sep and b"\r\n\r\n" not in request:
            request += b"\r\n" if request.endswith(b"\r\n") else b"\r\n\r\n"
            parts[i] = head + sep + request + b"\r\n"
    return delimiter.join(parts)


def main():
    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    server.daemon_threads = True
    signal.signal(signal.SIGTERM, lambda *_: sys.exit(0))
    with open(PORT_FILE, "w") as port_file:
        port_file.write(f"{server.server_address[1]}\n")
    try:
        server.serve_forever()
    finally:
        shutil.rmtree(PARTS_DIR, ignore_errors=True)


if __name__ == "__main__":
    main()
