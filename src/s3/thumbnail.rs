//! Thumbnails of images, item 6 of features-2026-11.md: `GET /bucket/key?x-s3dal-thumbnail=256`
//! answers with the object scaled down to fit 256×256, in JPEG, or in PNG when it has
//! transparent pixels. It is GetObject with one more query parameter, so a presigned URL signs
//! the size along with the rest, and every access rule of a download applies as it is: a
//! write-only pair gets none, an unsigned request only from a public bucket.
//!
//! Made on the first request and kept in the backend at
//! `.s3dal/thumbs/<bucket>/<E(key)>!!/<etag>-<size>.<ext>` (`E` is the versions tree's escape), so
//! every instance sharing the backend serves the one made, and an overwrite, which changes the
//! ETag, is never served the old one. The background job `thumbnail-sweep` removes those whose
//! object is gone or has another ETag now.

#[cfg(feature = "thumbnail")]
use std::borrow::Cow;

use s3s::{S3Result, s3_error};

#[cfg(feature = "thumbnail")]
use super::version::escape;
use crate::backend::RESERVED_ROOT;

/// The query parameter, named so no S3 parameter can ever take it.
pub(super) const PARAM: &str = "x-s3dal-thumbnail";

/// The sizes a client may ask for: a fixed few, so an object has at most this many thumbnails
/// in the cache, whatever clients ask.
pub(super) const SIZES: [u32; 4] = [128, 256, 512, 1024];

/// The size the request asks for, `None` for a plain GetObject. s3s routes a GET of an object
/// with a query parameter it does not know to GetObject and leaves the parameter in the URI, so
/// it is read from there.
pub(super) fn requested(uri: &hyper::Uri) -> S3Result<Option<u32>> {
    let mut found = None;
    for pair in uri.query().unwrap_or_default().split('&') {
        let (name, value) = pair.split_once('=').unwrap_or((pair, ""));
        if name != PARAM {
            continue;
        }
        // Two sizes leave the one meant unknown; a presigned URL holding both could be read
        // either way by whatever proxy sits in front.
        if found.replace(value).is_some() {
            return Err(s3_error!(InvalidArgument, "{PARAM} may be given only once"));
        }
    }
    let Some(value) = found else {
        return Ok(None);
    };
    match value.parse::<u32>() {
        Ok(size) if SIZES.contains(&size) => Ok(Some(size)),
        _ => Err(s3_error!(
            InvalidArgument,
            "{PARAM} must be 128, 256, 512 or 1024"
        )),
    }
}

/// The bucket's thumbnails, removed with the bucket.
pub(super) fn dir(bucket: &str) -> String {
    format!("{RESERVED_ROOT}thumbs/{bucket}/")
}

/// The directory of the key's thumbnails. `!!` ends the escaped key, as in the versions tree,
/// so `a` and `a/b` never share one, also on fs, where `a` cannot be a file and a directory.
#[cfg(feature = "thumbnail")]
fn holder(bucket: &str, key: &str) -> String {
    format!("{}{}!!/", dir(bucket), escape(key))
}

/// The ETag as it goes into a file name. Backends' own ETags are not all name-safe (a GCS one is
/// base64, with `/` and `+`), so such a one is replaced by its MD5, still one name per ETag.
#[cfg(feature = "thumbnail")]
fn etag_name(etag: &str) -> Cow<'_, str> {
    let safe = |b: u8| b.is_ascii_alphanumeric() || matches!(b, b'-' | b'_');
    if !etag.is_empty() && etag.bytes().all(safe) {
        Cow::Borrowed(etag)
    } else {
        Cow::Owned(crate::meta::md5_hex(etag.as_bytes()))
    }
}

/// `<etag>-<size>`, the name of a thumbnail without its extension.
#[cfg(feature = "thumbnail")]
fn stem(etag: &str, size: u32) -> String {
    format!("{}-{size}", etag_name(etag))
}

#[cfg(feature = "thumbnail")]
pub(super) use made::{respond, sweep};

#[cfg(not(feature = "thumbnail"))]
pub(super) async fn respond(
    _: &super::S3Dal,
    _: Source<'_>,
    _: u32,
) -> S3Result<s3s::S3Response<s3s::dto::GetObjectOutput>> {
    Err(s3_error!(
        NotImplemented,
        "this s3dal was built without thumbnails (feature `thumbnail`)"
    ))
}

/// The object a thumbnail is made of, as GetObject found it.
#[cfg_attr(not(feature = "thumbnail"), allow(dead_code))]
pub(super) struct Source<'a> {
    pub bucket: &'a str,
    pub key: &'a str,
    /// Its backend path: the key's, or an archived version's.
    pub path: &'a str,
    pub obj: &'a crate::meta::ObjectMeta,
    /// The backend's own ETag of what was stat'ed, to pin the read to it.
    pub version: Option<&'a str>,
    /// Its content, where the stat read it too (small objects on fs).
    pub content: Option<bytes::Bytes>,
    /// Where a cold object's bytes are.
    pub cold: Option<&'a super::tier::Cold>,
    /// The `x-amz-version-id` GetObject answers with.
    pub version_id: Option<String>,
}

#[cfg(feature = "thumbnail")]
mod made {
    use std::io::Cursor;
    use std::sync::{Arc, LazyLock};
    use std::time::Duration;

    use bytes::Bytes;
    use futures::TryStreamExt;
    use image::codecs::jpeg::JpegEncoder;
    use image::codecs::png::PngEncoder;
    use image::metadata::Orientation;
    use image::{
        DynamicImage, ExtendedColorType, ImageDecoder, ImageEncoder, ImageError, ImageFormat,
        ImageReader, Limits,
    };
    use opendal::ErrorKind;
    use s3s::dto::{ETag, GetObjectOutput, StreamingBlob, Timestamp};
    use s3s::{S3Error, S3ErrorCode, S3Response, S3Result};
    use tokio::sync::Semaphore;

    use super::{Source, holder, stem};
    use crate::backend::RESERVED_ROOT;
    use crate::error::{Ctx, map};
    use crate::meta;
    use crate::s3::{S3Dal, naming, version};

    /// The largest object a thumbnail is made of: it is read into memory whole.
    pub(in crate::s3) const MAX_SOURCE: u64 = 64 << 20;

    /// The most pixels decoded: 50 MP holds every phone camera's photo, and decodes to 200 MB
    /// in RGBA.
    pub(in crate::s3) const MAX_PIXELS: u64 = 50_000_000;

    /// What a decoder may allocate: 50 MP at 16 bits per channel with alpha, the most a PNG
    /// within `MAX_PIXELS` needs.
    const MAX_ALLOC: u64 = MAX_PIXELS * 8;

    /// From the request's arrival at the decode queue to the thumbnail made. A 50 MP JPEG takes
    /// about a second; the rest is waiting for a decode slot behind others.
    pub(in crate::s3) const TIMEOUT: Duration = Duration::from_secs(20);

    /// JPEG quality: at thumbnail sizes 80 cannot be told from higher by eye, at half the bytes
    /// of 95.
    const QUALITY: u8 = 80;

    /// Decodes at once, half the cores: each holds a decoded image of up to `MAX_ALLOC` bytes,
    /// and requests for small objects must keep threads to run on while a grid of photos is
    /// being thumbnailed. A decode that times out keeps its slot until it ends: the request has
    /// been answered, but its memory is still held.
    static DECODES: LazyLock<Arc<Semaphore>> = LazyLock::new(|| {
        let cores = std::thread::available_parallelism().map_or(2, |n| n.get());
        Arc::new(Semaphore::new((cores / 2).max(1)))
    });

    /// The output formats, in the order the cache is looked in.
    #[derive(Clone, Copy)]
    enum Ext {
        Jpg,
        Png,
    }

    impl Ext {
        const ALL: [Self; 2] = [Self::Jpg, Self::Png];

        fn name(self) -> &'static str {
            match self {
                Self::Jpg => "jpg",
                Self::Png => "png",
            }
        }

        fn content_type(self) -> &'static str {
            match self {
                Self::Jpg => "image/jpeg",
                Self::Png => "image/png",
            }
        }
    }

    struct Thumbnail {
        bytes: Bytes,
        ext: Ext,
    }

    /// GetObject's answer with the thumbnail of `source` in place of its content. The response
    /// carries the object's ETag and Last-Modified, which name the thumbnail as well as the
    /// object (it changes with them and only with them), so a browser's If-None-Match revalidates
    /// it with a 304. Content headers and checksums describe the object's bytes and are left out.
    pub(in crate::s3) async fn respond(
        dal: &S3Dal,
        source: Source<'_>,
        size: u32,
    ) -> S3Result<S3Response<GetObjectOutput>> {
        let thumb = get(dal, &source, size).await?;
        let length = i64::try_from(thumb.bytes.len()).map_err(S3Error::internal_error)?;
        Ok(S3Response::new(GetObjectOutput {
            content_length: Some(length),
            content_type: Some(thumb.ext.content_type().to_owned()),
            e_tag: Some(ETag::Strong(source.obj.etag.clone())),
            last_modified: source.obj.last_modified.map(Timestamp::from),
            version_id: source.version_id,
            body: Some(StreamingBlob::from(thumb.bytes)),
            ..Default::default()
        }))
    }

    /// The cached thumbnail, or a new one, cached for the next request.
    async fn get(dal: &S3Dal, source: &Source<'_>, size: u32) -> S3Result<Thumbnail> {
        // Before the cache, which cannot hold one, and before a byte is read.
        if source.obj.content_length > MAX_SOURCE {
            return Err(S3Error::with_message(
                S3ErrorCode::EntityTooLarge,
                "the object is larger than 64 MiB, the most a thumbnail is made of",
            ));
        }
        let holder = holder(source.bucket, source.key);
        let stem = stem(&source.obj.etag, size);
        for ext in Ext::ALL {
            match dal.op.read(&format!("{holder}{stem}.{}", ext.name())).await {
                Ok(buf) => {
                    return Ok(Thumbnail {
                        bytes: buf.to_bytes(),
                        ext,
                    });
                }
                Err(err) if err.kind() == ErrorKind::NotFound => {}
                Err(err) => return Err(map(err, Ctx::Other)),
            }
        }
        let content = match &source.content {
            Some(content) => content.clone(),
            None => read(dal, source).await?,
        };
        let thumb = render(content, size, TIMEOUT).await?;
        // Kept for the next request; one that cannot be kept, a key too long for the backend's
        // names say, is still served, and made again next time.
        let path = format!("{holder}{stem}.{}", thumb.ext.name());
        if let Err(err) = dal.op.write(&path, thumb.bytes.clone()).await {
            tracing::warn!(error = %err, path, "cannot keep a thumbnail");
        }
        Ok(thumb)
    }

    /// The object's content, pinned to what was stat'ed where the backend can (as GetObject
    /// pins it), so a thumbnail is never kept under one ETag with the content of another.
    async fn read(dal: &S3Dal, source: &Source<'_>) -> S3Result<Bytes> {
        if let Some(cold) = source.cold {
            return cold.read(dal, 0..source.obj.content_length).await;
        }
        let read = match source
            .version
            .filter(|_| dal.op.info().capability().read_with_if_match)
        {
            Some(version) => dal.op.read_with(source.path).if_match(version).await,
            None => dal.op.read(source.path).await,
        };
        read.map(|buf| buf.to_bytes())
            .map_err(|err| map(err, Ctx::ObjectRead))
    }

    /// Decodes and scales `content` on the blocking pool, with at most [`DECODES`] at once, and
    /// gives up after `timeout`.
    async fn render(content: Bytes, size: u32, timeout: Duration) -> S3Result<Thumbnail> {
        let work = async {
            let slot = DECODES
                .clone()
                .acquire_owned()
                .await
                .map_err(S3Error::internal_error)?;
            tokio::task::spawn_blocking(move || {
                let _slot = slot;
                draw(&content, size)
            })
            .await
            .map_err(S3Error::internal_error)?
        };
        tokio::time::timeout(timeout, work)
            .await
            .unwrap_or_else(|_| {
                // Retryable: the time went to waiting for a slot as much as to the decode, and an
                // SDK retries 503.
                Err(S3Error::with_message(
                    S3ErrorCode::ServiceUnavailable,
                    "the thumbnail was not made in time; try again",
                ))
            })
    }

    fn draw(content: &[u8], size: u32) -> S3Result<Thumbnail> {
        let format = image::guess_format(content)
            .ok()
            .filter(|f| {
                matches!(
                    f,
                    ImageFormat::Jpeg | ImageFormat::Png | ImageFormat::Gif | ImageFormat::WebP
                )
            })
            .ok_or_else(unsupported)?;
        let mut reader = ImageReader::with_format(Cursor::new(content), format);
        let mut limits = Limits::default();
        limits.max_alloc = Some(MAX_ALLOC);
        reader.limits(limits);
        let mut decoder = reader.into_decoder().map_err(refused)?;
        // From the header, before the pixels are decoded.
        let (width, height) = decoder.dimensions();
        if u64::from(width) * u64::from(height) > MAX_PIXELS {
            return Err(too_many_pixels());
        }
        // Phone cameras store a portrait photo sideways with an EXIF rotation; a thumbnail
        // shown as stored would lie on its side.
        let orientation = decoder.orientation().unwrap_or(Orientation::NoTransforms);
        let image = DynamicImage::from_decoder(decoder).map_err(refused)?;
        // Never scaled up: a small image is its own thumbnail.
        let mut thumb = if width > size || height > size {
            image.thumbnail(size, size)
        } else {
            image
        };
        // After scaling, on the small image: rotating the decoded one would copy it whole.
        thumb.apply_orientation(orientation);
        let (w, h) = (thumb.width(), thumb.height());
        let rgba = thumb.to_rgba8();
        // An alpha channel whose pixels are all opaque, as screenshots and GIFs have, is no
        // reason for a PNG several times the JPEG's size.
        let transparent = thumb.color().has_alpha() && rgba.pixels().any(|p| p.0[3] < u8::MAX);
        let mut out = Vec::new();
        let ext = if transparent {
            PngEncoder::new(&mut out)
                .write_image(&rgba, w, h, ExtendedColorType::Rgba8)
                .map_err(S3Error::internal_error)?;
            Ext::Png
        } else {
            JpegEncoder::new_with_quality(&mut out, QUALITY)
                .write_image(&thumb.to_rgb8(), w, h, ExtendedColorType::Rgb8)
                .map_err(S3Error::internal_error)?;
            Ext::Jpg
        };
        Ok(Thumbnail {
            bytes: Bytes::from(out),
            ext,
        })
    }

    fn unsupported() -> S3Error {
        S3Error::with_message(
            S3ErrorCode::NotImplemented,
            "thumbnails are made of JPEG, PNG, GIF and WebP images only",
        )
    }

    fn too_many_pixels() -> S3Error {
        S3Error::with_message(
            S3ErrorCode::EntityTooLarge,
            "the image has more than 50 megapixels, the most a thumbnail is made of",
        )
    }

    fn refused(err: ImageError) -> S3Error {
        match err {
            ImageError::Limits(_) => too_many_pixels(),
            ImageError::Unsupported(_) => unsupported(),
            err => S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                format!("the object is not an image a thumbnail can be made of: {err}"),
            ),
        }
    }

    /// Removes the thumbnails whose object is gone or has another ETag now; the background job
    /// `thumbnail-sweep`. Returns how many it removed. A key whose object cannot be stat'ed
    /// keeps its thumbnails until the next sweep.
    pub(in crate::s3) async fn sweep(dal: &S3Dal) -> anyhow::Result<usize> {
        let top = format!("{RESERVED_ROOT}thumbs/");
        let mut lister = match dal.op.lister_with(&top).recursive(true).await {
            Ok(lister) => lister,
            Err(err) if err.kind() == ErrorKind::NotFound => return Ok(0),
            Err(err) => return Err(err.into()),
        };
        let mut removed = 0;
        let mut emptied = Vec::new();
        // A key's thumbnails list together, one directory, so they are settled a key at a time,
        // each after all of them were listed: a thumbnail made after its object was stat'ed is
        // not among them, and is not taken for a stale one.
        let mut key: Option<Thumbs> = None;
        while let Some(entry) = lister.try_next().await? {
            if !entry.metadata().is_file() {
                continue;
            }
            let Some((bucket, k, holder, stem)) = parse(&top, entry.path()) else {
                continue;
            };
            if key.as_ref().is_some_and(|t| t.holder != holder) {
                removed += settle(dal, key.take().expect("checked"), &mut emptied).await;
            }
            key.get_or_insert_with(|| Thumbs {
                bucket,
                key: k,
                holder: holder.to_owned(),
                files: Vec::new(),
            })
            .files
            .push((entry.path().to_owned(), stem.to_owned()));
        }
        if let Some(thumbs) = key {
            removed += settle(dal, thumbs, &mut emptied).await;
        }
        // Once the listing is done: on fs it walks directories still to be pruned.
        for holder in emptied {
            version::prune_up_to(dal, &top, holder).await;
        }
        Ok(removed)
    }

    /// The thumbnails of one key.
    struct Thumbs {
        bucket: String,
        key: String,
        holder: String,
        files: Vec<(String, String)>,
    }

    /// `(bucket, key, holder, stem)` of a thumbnail's path; `None` for a file the cache cannot
    /// have written.
    fn parse<'a>(top: &str, path: &'a str) -> Option<(String, String, &'a str, &'a str)> {
        let rel = path.strip_prefix(top)?;
        let (bucket, rest) = rel.split_once('/')?;
        // `!!` never occurs inside an escaped key, so the first one ends it.
        let end = rest.find("!!/")?;
        let key = version::unescape(&rest[..end])?;
        let name = &rest[end + 3..];
        let (stem, _ext) = name.rsplit_once('.')?;
        if name.contains('/') {
            return None;
        }
        let holder = &path[..top.len() + bucket.len() + 1 + end + 3];
        Some((bucket.to_owned(), key, holder, stem))
    }

    /// Removes the key's thumbnails that are not of its object as it is now. Returns how many,
    /// and records a holder left empty.
    async fn settle(dal: &S3Dal, thumbs: Thumbs, emptied: &mut Vec<String>) -> usize {
        let current = match dal
            .op
            .stat(&naming::object_path(&thumbs.bucket, &thumbs.key))
            .await
        {
            Ok(m) if m.is_file() => Some(meta::resolve(&dal.profile, &m).etag),
            Ok(_) => None,
            Err(err) if err.kind() == ErrorKind::NotFound => None,
            Err(err) => {
                tracing::warn!(error = %err, bucket = thumbs.bucket, key = thumbs.key, "cannot look up the object of thumbnails");
                return 0;
            }
        };
        let mut removed = 0;
        let total = thumbs.files.len();
        for (path, stem) in thumbs.files {
            let kept = current.as_deref().is_some_and(|etag| {
                stem.rsplit_once('-')
                    .is_some_and(|(name, _)| name == super::etag_name(etag))
            });
            if kept {
                continue;
            }
            match dal.op.delete(&path).await {
                Ok(()) => removed += 1,
                Err(err) => tracing::warn!(error = %err, path, "cannot remove a stale thumbnail"),
            }
        }
        if removed == total {
            emptied.push(thumbs.holder);
        }
        removed
    }

    #[cfg(test)]
    mod tests {
        use super::super::dir;
        use super::*;

        fn encode(img: &DynamicImage, format: ImageFormat) -> Vec<u8> {
            let mut out = Cursor::new(Vec::new());
            img.write_to(&mut out, format).unwrap();
            out.into_inner()
        }

        fn decode(bytes: &[u8]) -> DynamicImage {
            image::load_from_memory(bytes).unwrap()
        }

        #[tokio::test]
        async fn a_decode_over_its_time_is_answered_with_a_retryable_error() {
            // A request must not wait without bound on a decode, or on the queue in front of it,
            // and the client must be told to retry rather than that the image is bad.
            let png = encode(&DynamicImage::new_rgb8(64, 64), ImageFormat::Png);
            // Every decode slot busy, as behind a grid of large photos: the wait counts.
            let busy = DECODES
                .clone()
                .acquire_many_owned(u32::try_from(DECODES.available_permits()).unwrap())
                .await
                .unwrap();
            let waited = render(Bytes::from(png.clone()), 128, Duration::from_millis(50));
            let err = tokio::time::timeout(Duration::from_secs(10), waited)
                .await
                .expect("the request waited past its timeout")
                .err()
                .expect("timed out");
            assert_eq!(*err.code(), S3ErrorCode::ServiceUnavailable);
            assert_eq!(err.status_code().map(|s| s.as_u16()), Some(503));
            // With a slot free, the same image in time is made.
            drop(busy);
            let thumb = render(Bytes::from(png), 128, TIMEOUT).await.unwrap();
            assert_eq!(decode(&thumb.bytes).width(), 64);
        }

        #[test]
        fn an_exif_rotation_is_applied_to_the_thumbnail() {
            // Phone portraits are stored landscape with a rotation tag; a grid of sideways
            // thumbnails is what users would see otherwise.
            let mut img = DynamicImage::new_rgb8(400, 200);
            // Mark the top-left corner to see where it went.
            img.as_mut_rgb8()
                .unwrap()
                .put_pixel(0, 0, image::Rgb([255, 255, 255]));
            let mut jpeg = Vec::new();
            let mut encoder = JpegEncoder::new_with_quality(&mut jpeg, 95);
            // EXIF in TIFF form, little endian, one IFD entry: Orientation (0x0112) = 6, rotate
            // 90° clockwise to display.
            let exif: Vec<u8> = [
                b"II*\0".as_slice(),
                &8u32.to_le_bytes(),
                &1u16.to_le_bytes(),
                &0x0112u16.to_le_bytes(),
                &3u16.to_le_bytes(),
                &1u32.to_le_bytes(),
                &6u16.to_le_bytes(),
                &[0, 0],
                &0u32.to_le_bytes(),
            ]
            .concat();
            encoder.set_exif_metadata(exif).unwrap();
            encoder
                .write_image(img.as_bytes(), 400, 200, ExtendedColorType::Rgb8)
                .unwrap();
            let thumb = draw(&jpeg, 128).unwrap();
            let out = decode(&thumb.bytes);
            assert_eq!((out.width(), out.height()), (64, 128));
        }

        #[test]
        fn opaque_alpha_gives_a_jpeg_and_transparency_a_png() {
            // A PNG thumbnail is several times a JPEG's size; only a transparent image needs one.
            let mut opaque = image::RgbaImage::from_pixel(32, 32, image::Rgba([10, 20, 30, 255]));
            let png = encode(&DynamicImage::ImageRgba8(opaque.clone()), ImageFormat::Png);
            assert!(matches!(draw(&png, 128).unwrap().ext, Ext::Jpg));
            opaque.put_pixel(3, 3, image::Rgba([0, 0, 0, 0]));
            let png = encode(&DynamicImage::ImageRgba8(opaque), ImageFormat::Png);
            assert!(matches!(draw(&png, 128).unwrap().ext, Ext::Png));
        }

        #[test]
        fn a_thumbnail_path_parses_back_to_its_key() {
            // The sweep finds a thumbnail's object by its path alone; a key read wrongly would
            // keep stale thumbnails, or remove good ones.
            let top = format!("{RESERVED_ROOT}thumbs/");
            for key in ["a.jpg", "dir/sub/b c!.png", "x!!y"] {
                let path = format!("{}{}.jpg", holder("bkt", key), stem("abc-2", 256));
                let (bucket, k, h, s) = parse(&top, &path).unwrap();
                assert_eq!((bucket.as_str(), k.as_str(), s), ("bkt", key, "abc-2-256"));
                assert_eq!(h, holder("bkt", key));
            }
            assert!(parse(&top, ".s3dal/thumbs/bkt/stray.jpg").is_none());
        }

        #[test]
        fn dir_holds_every_holder() {
            assert!(holder("b", "k").starts_with(&dir("b")));
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn uri(q: &str) -> hyper::Uri {
        format!("/b/k?{q}").parse().unwrap()
    }

    #[test]
    fn only_the_listed_sizes_are_taken() {
        // Any size a client could ask for would grow the cache without bound, one file per size.
        for size in SIZES {
            assert_eq!(
                requested(&uri(&format!("{PARAM}={size}"))).unwrap(),
                Some(size)
            );
        }
        for bad in ["0", "64", "255", "257", "2048", "", "256px", "-256"] {
            let err = requested(&uri(&format!("{PARAM}={bad}"))).unwrap_err();
            assert_eq!(*err.code(), s3s::S3ErrorCode::InvalidArgument, "{bad}");
        }
        let err = requested(&uri(&format!("{PARAM}=128&{PARAM}=256"))).unwrap_err();
        assert_eq!(*err.code(), s3s::S3ErrorCode::InvalidArgument);
        assert_eq!(requested(&uri("versionId=null")).unwrap(), None);
        assert_eq!(requested(&"/b/k".parse().unwrap()).unwrap(), None);
    }

    #[cfg(feature = "thumbnail")]
    #[test]
    fn an_etag_that_is_not_name_safe_becomes_its_md5() {
        // A GCS ETag is base64; a `/` in it would put the thumbnail in another directory.
        assert_eq!(etag_name("abc-3"), "abc-3");
        let gcs = etag_name("CJ+a/b==");
        assert_eq!(gcs.len(), 32);
        assert!(gcs.bytes().all(|b| b.is_ascii_hexdigit()));
        assert_ne!(etag_name("CJ+a/c=="), gcs);
    }
}
