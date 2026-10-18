//! Verification of an uploaded body while it streams: its length, `Content-MD5`, and
//! `x-amz-checksum-*` values, including those that arrive as aws-chunked trailers. PutObject and
//! UploadPart share it.
//!
//! Everything is checked before the writer is closed, so a body that fails any check is aborted
//! and never becomes visible.

use std::mem;

use base64_simd::STANDARD;
use hyper::HeaderMap;
use md5::{Digest, Md5};
use s3s::checksum::ChecksumHasher;
use s3s::dto::{Checksum, ChecksumAlgorithm, PutObjectInput};
use s3s::header::CONTENT_MD5;
use s3s::stream::aws_chunked_stream::AwsChunkedStreamError;
use s3s::stream::upload_stream::UploadStreamError;
use s3s::{
    BodySizeLimitExceeded, S3Error, S3ErrorCode, S3Result, StdError, TrailingHeaders, s3_error,
};

use crate::checksum::ALGORITHMS;
use crate::meta;

/// The request headers that describe an uploaded body.
#[derive(Default)]
pub(super) struct BodyHeaders {
    pub content_length: Option<i64>,
    pub content_md5: Option<String>,
    /// Values sent as headers. Trailer values arrive only after the body.
    pub checksum: Checksum,
    /// Named by `x-amz-sdk-checksum-algorithm`, or by the checksum in `x-amz-trailer`; s3s
    /// derives it from either.
    pub algorithm: Option<ChecksumAlgorithm>,
}

impl BodyHeaders {
    pub(super) fn from_put(input: &PutObjectInput) -> Self {
        Self {
            content_length: input.content_length,
            content_md5: input.content_md5.clone(),
            checksum: Checksum {
                checksum_crc32: input.checksum_crc32.clone(),
                checksum_crc32c: input.checksum_crc32c.clone(),
                checksum_crc64nvme: input.checksum_crc64nvme.clone(),
                checksum_sha1: input.checksum_sha1.clone(),
                checksum_sha256: input.checksum_sha256.clone(),
                checksum_sha512: input.checksum_sha512.clone(),
                checksum_md5: input.checksum_md5.clone(),
                checksum_xxhash64: input.checksum_xxhash64.clone(),
                checksum_xxhash3: input.checksum_xxhash3.clone(),
                checksum_xxhash128: input.checksum_xxhash128.clone(),
                ..Default::default()
            },
            algorithm: input.checksum_algorithm.clone(),
        }
    }
}

/// What a fully verified body proved.
pub(super) struct Verified {
    /// Hex MD5 of the body, when it was hashed.
    pub md5_hex: Option<String>,
    /// The checksum the object keeps, computed over the body. It equals the client's value when
    /// one was sent; S3 echoes it in the response.
    pub checksum: Checksum,
}

pub(super) struct BodyCheck {
    declared_len: Option<u64>,
    received: u64,
    content_md5: Option<String>,
    md5: Option<Md5>,
    hasher: ChecksumHasher,
    /// Header values only, known before the body; `expected` gains the trailer values.
    declared: Checksum,
    expected: Checksum,
    /// The algorithm of the one checksum the object keeps; see [`Self::new`].
    keep: Option<&'static str>,
}

impl BodyCheck {
    /// `hash_md5` asks for the body's MD5 even without `Content-MD5`, for a buffered ETag.
    pub(super) fn new(headers: &BodyHeaders, hash_md5: bool) -> S3Result<Self> {
        let content_md5 = headers
            .content_md5
            .as_deref()
            .map(content_md5_hex)
            .transpose()?;

        let algorithm = headers.algorithm.as_ref().map(|a| a.as_str());
        if algorithm.is_some_and(|a| !ALGORITHMS.iter().any(|(name, ..)| *name == a)) {
            return Err(s3_error!(
                InvalidRequest,
                "Value for x-amz-checksum-algorithm header is invalid."
            ));
        }

        let mut declared = headers.checksum.clone();
        // An object has one checksum of its own; S3 refuses to guess which of several is it.
        let sent = ALGORITHMS
            .iter()
            .filter(|(_, field, _)| field(&mut declared).is_some())
            .count();
        if sent > 1 {
            return Err(s3_error!(
                InvalidRequest,
                "Expecting a single x-amz-checksum- header. Multiple checksum Types are not allowed."
            ));
        }
        let mut hasher = ChecksumHasher::default();
        let mut keep = None;
        for (name, field, enable) in ALGORITHMS {
            if field(&mut declared).is_some() || algorithm == Some(name) {
                enable(&mut hasher);
            }
            if algorithm == Some(name) || (algorithm.is_none() && field(&mut declared).is_some()) {
                keep = Some(name);
            }
        }

        Ok(Self {
            declared_len: headers.content_length.and_then(|n| u64::try_from(n).ok()),
            received: 0,
            md5: (hash_md5 || content_md5.is_some()).then(Md5::new),
            content_md5,
            hasher,
            expected: declared.clone(),
            declared,
            keep,
        })
    }

    /// Hex MD5 from `Content-MD5`, known before the body is read.
    pub(super) fn content_md5(&self) -> Option<&str> {
        self.content_md5.as_deref()
    }

    /// The checksum the object keeps, when it was sent as a header and so is known before the
    /// body is read. It is verified before the writer closes, so it may be stored when it opens.
    pub(super) fn declared_checksum(&self) -> Checksum {
        only(self.declared.clone(), self.keep)
    }

    pub(super) fn received(&self) -> u64 {
        self.received
    }

    pub(super) fn update(&mut self, bytes: &[u8]) -> S3Result<()> {
        self.received += bytes.len() as u64;
        // Fail as soon as the body outgrows Content-Length; the buffered path relies on this
        // to keep memory bounded by the declared size.
        if self.declared_len.is_some_and(|n| self.received > n) {
            return Err(incomplete_body());
        }
        if let Some(md5) = &mut self.md5 {
            md5.update(bytes);
        }
        self.hasher.update(bytes);
        Ok(())
    }

    /// Runs every check once the body has ended. `trailer` looks up an aws-chunked trailer,
    /// which s3s only exposes after the body stream is exhausted.
    pub(super) fn finish(
        &mut self,
        trailer: impl Fn(&str) -> Option<String>,
    ) -> S3Result<Verified> {
        if self.declared_len.is_some_and(|n| self.received != n) {
            return Err(incomplete_body());
        }

        let md5_hex = self.md5.take().map(|h| meta::hex(&h.finalize()));
        if let Some(expected) = &self.content_md5
            && md5_hex.as_ref() != Some(expected)
        {
            return Err(s3_error!(
                BadDigest,
                "The Content-MD5 you specified did not match what we received."
            ));
        }

        let mut computed = mem::take(&mut self.hasher).finalize();
        for (name, field, _) in ALGORITHMS {
            let header = format!("x-amz-checksum-{}", name.to_ascii_lowercase());
            if let Some(value) = trailer(&header) {
                *field(&mut self.expected) = Some(value);
            }
            let expected = field(&mut self.expected);
            if expected.is_some() && field(&mut computed) != expected {
                return Err(S3Error::with_message(
                    S3ErrorCode::BadDigest,
                    format!("The {name} you specified did not match the calculated checksum."),
                ));
            }
        }

        Ok(Verified {
            md5_hex,
            checksum: only(computed, self.keep),
        })
    }
}

/// `checksum` with the value of `algorithm` alone. Every checksum a body brings is verified, but
/// an object keeps one: of the algorithm named, which for a copy is the one it asks for, as on
/// AWS, or else of the value sent. A copy that kept its source's as well could never be copied
/// again, because a copy presents the stored checksums as its body's, and a body may have one.
fn only(mut checksum: Checksum, algorithm: Option<&str>) -> Checksum {
    for (name, field, _) in ALGORITHMS {
        if algorithm != Some(name) {
            *field(&mut checksum) = None;
        }
    }
    checksum
}

/// Looks up aws-chunked trailers, which s3s fills in only once the body stream has ended. Each
/// lookup reads them then, so the lookup can be made before the body is read: the streamed
/// paths make it up front, and trailers taken at that point were always absent, which left
/// every trailer checksum on them unchecked.
pub(super) fn trailer_lookup(
    trailers: Option<&TrailingHeaders>,
) -> impl Fn(&str) -> Option<String> + '_ {
    move |name| {
        trailers?
            .read(|headers| headers.get(name)?.to_str().ok().map(str::to_owned))
            .flatten()
    }
}

fn incomplete_body() -> S3Error {
    s3_error!(
        IncompleteBody,
        "You did not provide the number of bytes specified by the Content-Length HTTP header."
    )
}

/// The client's `Content-MD5`. s3s parses an empty header as an absent one, but AWS refuses it
/// with InvalidDigest rather than skip the check the client asked for; kept as an empty value,
/// it fails validation in [`BodyCheck::new`]. Only an empty header stands for one: a PostObject
/// takes its digest from the form, and a header there covers the whole form, not the file.
pub(super) fn content_md5(parsed: Option<String>, headers: &HeaderMap) -> Option<String> {
    parsed.or_else(|| {
        headers
            .get(CONTENT_MD5)
            .filter(|value| value.is_empty())
            .map(|_| String::new())
    })
}

/// `Content-MD5` is base64 of the 16-byte digest; s3dal stores and reports it as hex.
fn content_md5_hex(header: &str) -> S3Result<String> {
    match STANDARD.decode_to_vec(header.trim()) {
        Ok(digest) if digest.len() == 16 => Ok(meta::hex(&digest)),
        _ => Err(s3_error!(
            InvalidDigest,
            "The Content-MD5 you specified was not valid."
        )),
    }
}

/// s3s reports its own body verification failures (signed payload hash, chunk signatures,
/// decoded length) as typed errors with a precise S3 code. Anything else is the client's
/// connection failing mid-body, which is not a server fault.
pub(super) fn body_error(err: StdError) -> S3Error {
    let code = if let Some(e) = err.downcast_ref::<UploadStreamError>() {
        e.to_s3_error_code()
    } else if let Some(e) = err.downcast_ref::<AwsChunkedStreamError>() {
        e.to_s3_error_code()
    } else if err.is::<BodySizeLimitExceeded>() {
        S3ErrorCode::EntityTooLarge
    } else {
        S3ErrorCode::IncompleteBody
    };
    S3Error::with_source(code, err)
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use super::*;

    const BODY: &[u8] = b"hello world";
    // CRC32 (ISO-HDLC) of BODY, base64 of the big-endian digest.
    const BODY_CRC32: &str = "DUoRhQ==";

    fn input(f: impl FnOnce(&mut PutObjectInput)) -> PutObjectInput {
        let mut input = PutObjectInput {
            content_length: Some(BODY.len() as i64),
            ..Default::default()
        };
        f(&mut input);
        input
    }

    fn no_trailer(_: &str) -> Option<String> {
        None
    }

    fn run(input: &PutObjectInput, trailers: &[(&str, &str)]) -> S3Result<Verified> {
        let trailers: HashMap<String, String> = trailers
            .iter()
            .map(|(k, v)| ((*k).to_owned(), (*v).to_owned()))
            .collect();
        let mut check = BodyCheck::new(&BodyHeaders::from_put(input), false)?;
        check.update(BODY)?;
        check.finish(|name| trailers.get(name).cloned())
    }

    fn code(result: S3Result<Verified>) -> S3ErrorCode {
        match result {
            Ok(_) => panic!("expected an error"),
            Err(err) => err.code().clone(),
        }
    }

    #[test]
    fn trailer_checksum_is_verified_against_the_body() {
        // Current AWS SDKs send CRC32 as an aws-chunked trailer by default; a corrupted upload
        // must be refused even though its checksum arrives after the data.
        let declared =
            input(|i| i.checksum_algorithm = Some(ChecksumAlgorithm::from_static("CRC32")));
        let ok = run(&declared, &[("x-amz-checksum-crc32", BODY_CRC32)]).unwrap();
        assert_eq!(ok.checksum.checksum_crc32.as_deref(), Some(BODY_CRC32));

        let bad = run(&declared, &[("x-amz-checksum-crc32", "AAAAAA==")]);
        assert_eq!(code(bad), S3ErrorCode::BadDigest);
    }

    #[test]
    fn header_checksum_is_verified_against_the_body() {
        let bad = input(|i| i.checksum_crc32 = Some("AAAAAA==".to_owned()));
        assert_eq!(code(run(&bad, &[])), S3ErrorCode::BadDigest);
        let good = input(|i| i.checksum_crc32 = Some(BODY_CRC32.to_owned()));
        assert!(run(&good, &[]).is_ok());
    }

    #[test]
    fn algorithm_without_a_value_is_computed_for_the_response() {
        // S3 computes and returns the checksum when only the algorithm is named.
        let declared =
            input(|i| i.checksum_algorithm = Some(ChecksumAlgorithm::from_static("CRC32")));
        let verified = run(&declared, &[]).unwrap();
        assert_eq!(
            verified.checksum.checksum_crc32.as_deref(),
            Some(BODY_CRC32)
        );
    }

    #[test]
    fn a_body_keeps_one_checksum_and_verifies_every_one() {
        // A copy names the algorithm it asks for and declares its source's stored checksum, so
        // both are hashed. The source's must still match, but the copy keeps only the one asked
        // for: with two, every later copy of it would be refused.
        const BODY_SHA256: &str = "uU0nuZNNPgilLlLX2n2r+sSE7+N6U4DukIj3rOLvzek=";
        let both = input(|i| {
            i.checksum_algorithm = Some(ChecksumAlgorithm::from_static("SHA256"));
            i.checksum_crc32 = Some(BODY_CRC32.to_owned());
        });
        let check = BodyCheck::new(&BodyHeaders::from_put(&both), false).unwrap();
        assert!(check.declared_checksum() == Checksum::default());
        let verified = run(&both, &[]).unwrap();
        assert!(
            verified.checksum
                == Checksum {
                    checksum_sha256: Some(BODY_SHA256.to_owned()),
                    ..Default::default()
                },
            "{:?}",
            verified.checksum
        );
        let stale = input(|i| {
            i.checksum_algorithm = Some(ChecksumAlgorithm::from_static("SHA256"));
            i.checksum_crc32 = Some("AAAAAA==".to_owned());
        });
        assert_eq!(code(run(&stale, &[])), S3ErrorCode::BadDigest);
        // Without a name, the value sent is the one kept.
        let crc32 = input(|i| i.checksum_crc32 = Some(BODY_CRC32.to_owned()));
        let check = BodyCheck::new(&BodyHeaders::from_put(&crc32), false).unwrap();
        assert_eq!(
            check.declared_checksum().checksum_crc32.as_deref(),
            Some(BODY_CRC32)
        );
    }

    #[test]
    fn content_md5_mismatch_is_bad_digest() {
        let wrong =
            input(|i| i.content_md5 = Some(STANDARD.encode_to_string(Md5::digest(b"other"))));
        assert_eq!(code(run(&wrong, &[])), S3ErrorCode::BadDigest);
        let right = input(|i| i.content_md5 = Some(STANDARD.encode_to_string(Md5::digest(BODY))));
        assert_eq!(
            run(&right, &[]).unwrap().md5_hex.as_deref(),
            Some(meta::md5_hex(BODY).as_str())
        );
    }

    #[test]
    fn malformed_content_md5_is_invalid_digest() {
        for bad in ["not base64!", "AAAA"] {
            let input = input(|i| i.content_md5 = Some(bad.to_owned()));
            assert_eq!(
                *BodyCheck::new(&BodyHeaders::from_put(&input), false)
                    .err()
                    .unwrap()
                    .code(),
                S3ErrorCode::InvalidDigest,
                "{bad}"
            );
        }
    }

    #[test]
    fn body_shorter_or_longer_than_content_length_is_incomplete() {
        // A truncated upload must never be committed as if it were the whole object.
        let mut short = BodyCheck::new(&BodyHeaders::from_put(&input(|_| {})), false).unwrap();
        short.update(&BODY[..3]).unwrap();
        assert_eq!(
            *short.finish(no_trailer).err().unwrap().code(),
            S3ErrorCode::IncompleteBody
        );

        let mut long = BodyCheck::new(&BodyHeaders::from_put(&input(|_| {})), false).unwrap();
        long.update(BODY).unwrap();
        assert_eq!(
            *long.update(b"!").unwrap_err().code(),
            S3ErrorCode::IncompleteBody
        );
    }

    #[test]
    fn unknown_checksum_algorithm_is_refused() {
        let input = input(|i| i.checksum_algorithm = Some(ChecksumAlgorithm::from_static("CRC9")));
        assert_eq!(
            *BodyCheck::new(&BodyHeaders::from_put(&input), false)
                .err()
                .unwrap()
                .code(),
            S3ErrorCode::InvalidRequest
        );
    }

    #[test]
    fn s3s_stream_failures_keep_their_s3_code() {
        let err = body_error(Box::new(UploadStreamError::Sha256Mismatch));
        assert_eq!(*err.code(), S3ErrorCode::BadDigest);
        let err = body_error(Box::new(std::io::Error::from(
            std::io::ErrorKind::ConnectionReset,
        )));
        assert_eq!(*err.code(), S3ErrorCode::IncompleteBody);
    }
}
