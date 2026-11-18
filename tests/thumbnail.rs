//! Thumbnails: `GET /bucket/key?x-s3dal-thumbnail=<size>` (features-2026-11.md item 6).
#![cfg(feature = "thumbnail")]

mod common;

use std::io::Cursor;
use std::time::Duration;

use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::presigning::PresigningConfig;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{BucketVersioningStatus, VersioningConfiguration};
use common::{Backend, TestServer, client, raw_request};
use image::codecs::gif::GifEncoder;
use image::codecs::webp::WebPEncoder;
use image::{DynamicImage, Frame, ImageFormat, Rgba, RgbaImage};
use s3dal::Settings;
use s3dal::config::KeyPair;

const PARAM: &str = "x-s3dal-thumbnail";

/// A `w`×`h` image in two halves, red on the left and blue on the right, so a thumbnail shows
/// which way it was scaled and turned.
fn picture(w: u32, h: u32) -> RgbaImage {
    RgbaImage::from_fn(w, h, |x, _| {
        if x < w / 2 {
            Rgba([255, 0, 0, 255])
        } else {
            Rgba([0, 0, 255, 255])
        }
    })
}

fn encode(img: RgbaImage, format: ImageFormat) -> Vec<u8> {
    let mut out = Cursor::new(Vec::new());
    let img = match format {
        // JPEG has no alpha channel to encode.
        ImageFormat::Jpeg => DynamicImage::ImageRgb8(DynamicImage::ImageRgba8(img).to_rgb8()),
        _ => DynamicImage::ImageRgba8(img),
    };
    img.write_to(&mut out, format).unwrap();
    out.into_inner()
}

fn webp(img: RgbaImage) -> Vec<u8> {
    let mut out = Vec::new();
    let (w, h) = img.dimensions();
    WebPEncoder::new_lossless(&mut out)
        .encode(&img, w, h, image::ExtendedColorType::Rgba8)
        .unwrap();
    out
}

/// A GIF of two frames: `first`, then one all green.
fn gif(first: RgbaImage) -> Vec<u8> {
    let (w, h) = first.dimensions();
    let mut out = Vec::new();
    {
        let mut encoder = GifEncoder::new(&mut out);
        encoder.encode_frame(Frame::new(first)).unwrap();
        let green = RgbaImage::from_pixel(w, h, Rgba([0, 255, 0, 255]));
        encoder.encode_frame(Frame::new(green)).unwrap();
    }
    out
}

/// A one-pixel GIF whose header claims a canvas of `w`×`h`: GIF has no checksum, so the header
/// can say anything, and a decoder sizes its output by it.
fn gif_claiming(w: u16, h: u16) -> Vec<u8> {
    let mut bytes = gif(picture(1, 1));
    bytes[6..8].copy_from_slice(&w.to_le_bytes());
    bytes[8..10].copy_from_slice(&h.to_le_bytes());
    bytes
}

async fn put(server: &TestServer, bucket: &str, key: &str, body: Vec<u8>) -> String {
    server
        .client
        .put_object()
        .bucket(bucket)
        .key(key)
        .body(ByteStream::from(body))
        .send()
        .await
        .unwrap()
        .e_tag()
        .unwrap()
        .trim_matches('"')
        .to_owned()
}

/// A thumbnail through the SDK, the query parameter added before the request is signed.
async fn thumbnail(
    client: &Client,
    bucket: &str,
    key: &str,
    size: &str,
    version: Option<&str>,
) -> Result<(String, DynamicImage), String> {
    let param = format!("{PARAM}={size}");
    let out = client
        .get_object()
        .bucket(bucket)
        .key(key)
        .set_version_id(version.map(str::to_owned))
        .customize()
        .mutate_request(move |req| {
            let sep = if req.uri().contains('?') { '&' } else { '?' };
            let uri = format!("{}{sep}{param}", req.uri());
            req.set_uri(uri).unwrap();
        })
        .send()
        .await
        .map_err(|e| e.code().unwrap_or("none").to_owned())?;
    let content_type = out.content_type().unwrap().to_owned();
    let bytes = out.body.collect().await.unwrap().into_bytes();
    Ok((content_type, image::load_from_memory(&bytes).unwrap()))
}

/// The files in the cache for `bucket/key`, sorted.
async fn cached(server: &TestServer, bucket: &str, key: &str) -> Vec<String> {
    server
        .names(&format!(".s3dal/thumbs/{bucket}/{key}!!"))
        .await
}

fn left_and_right(img: &DynamicImage) -> ([u8; 4], [u8; 4]) {
    let rgba = img.to_rgba8();
    let (w, h) = rgba.dimensions();
    (rgba.get_pixel(2, h / 2).0, rgba.get_pixel(w - 3, h / 2).0)
}

fn near(a: [u8; 4], b: [u8; 4]) -> bool {
    a.iter().zip(b).all(|(x, y)| x.abs_diff(y) < 48)
}

const RED: [u8; 4] = [255, 0, 0, 255];
const BLUE: [u8; 4] = [0, 0, 255, 255];

#[tokio::test]
async fn each_input_format_gets_a_thumbnail_that_fits_the_size() {
    // Browsers show these four everywhere; a grid of photos must get a small image of each,
    // the picture's own proportions kept, and a GIF its first frame.
    let server = TestServer::start().await;
    server
        .client
        .create_bucket()
        .bucket("pics")
        .send()
        .await
        .unwrap();
    let sources = [
        ("p.jpg", encode(picture(600, 300), ImageFormat::Jpeg)),
        ("p.png", encode(picture(600, 300), ImageFormat::Png)),
        ("p.gif", gif(picture(600, 300))),
        ("p.webp", webp(picture(600, 300))),
    ];
    for (key, body) in sources {
        put(&server, "pics", key, body).await;
        let (content_type, img) = thumbnail(&server.client, "pics", key, "256", None)
            .await
            .unwrap();
        assert_eq!(content_type, "image/jpeg", "{key}");
        assert_eq!((img.width(), img.height()), (256, 128), "{key}");
        let (left, right) = left_and_right(&img);
        assert!(
            near(left, RED) && near(right, BLUE),
            "{key}: {left:?} {right:?}"
        );
    }
    // A transparent image keeps its transparency, which a JPEG cannot carry.
    let mut clear = picture(600, 300);
    clear.put_pixel(0, 0, Rgba([0, 0, 0, 0]));
    put(
        &server,
        "pics",
        "clear.png",
        encode(clear, ImageFormat::Png),
    )
    .await;
    let (content_type, img) = thumbnail(&server.client, "pics", "clear.png", "128", None)
        .await
        .unwrap();
    assert_eq!(content_type, "image/png");
    assert_eq!((img.width(), img.height()), (128, 64));
    // Smaller than the size asked for: never scaled up.
    put(
        &server,
        "pics",
        "tiny.png",
        encode(picture(40, 20), ImageFormat::Png),
    )
    .await;
    let (_, img) = thumbnail(&server.client, "pics", "tiny.png", "1024", None)
        .await
        .unwrap();
    assert_eq!((img.width(), img.height()), (40, 20));
}

#[tokio::test]
async fn only_the_listed_sizes_are_made() {
    // A size per request would let any client fill the cache with variants of every object.
    let server = TestServer::start().await;
    server
        .client
        .create_bucket()
        .bucket("pics")
        .send()
        .await
        .unwrap();
    put(
        &server,
        "pics",
        "p.png",
        encode(picture(600, 300), ImageFormat::Png),
    )
    .await;
    for size in ["128", "256", "512", "1024"] {
        assert!(
            thumbnail(&server.client, "pics", "p.png", size, None)
                .await
                .is_ok(),
            "{size}"
        );
    }
    for size in ["100", "2048", "abc", ""] {
        let err = thumbnail(&server.client, "pics", "p.png", size, None)
            .await
            .unwrap_err();
        assert_eq!(err, "InvalidArgument", "{size}");
    }
    assert_eq!(cached(&server, "pics", "p.png").await.len(), 4);
}

#[tokio::test]
async fn a_second_request_is_served_from_the_cache() {
    // Decoding a photo costs a core for a second; a grid opened twice must not pay it twice,
    // and every instance sharing the backend must find what one of them made.
    let server = TestServer::start().await;
    let other = TestServer::start_on(Backend::sharing(&server), Settings::default()).await;
    server
        .client
        .create_bucket()
        .bucket("pics")
        .send()
        .await
        .unwrap();
    let etag = put(
        &server,
        "pics",
        "p.jpg",
        encode(picture(600, 300), ImageFormat::Jpeg),
    )
    .await;
    thumbnail(&server.client, "pics", "p.jpg", "256", None)
        .await
        .unwrap();
    let names = cached(&server, "pics", "p.jpg").await;
    assert_eq!(names.len(), 1, "{names:?}");
    assert!(names[0].ends_with("-256.jpg"), "{names:?}");
    if etag.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-') {
        assert_eq!(names[0], format!("{etag}-256.jpg"));
    }
    // Replaced behind s3dal's back by an image no decode of the object could give: what comes
    // back now came from the cache, not from the object.
    let path = format!(".s3dal/thumbs/pics/p.jpg!!/{}", names[0]);
    server
        .op
        .write(&path, encode(picture(7, 7), ImageFormat::Jpeg))
        .await
        .unwrap();
    for s in [&server, &other] {
        let (_, img) = thumbnail(&s.client, "pics", "p.jpg", "256", None)
            .await
            .unwrap();
        assert_eq!((img.width(), img.height()), (7, 7));
    }
}

#[tokio::test]
async fn an_overwrite_is_never_served_the_old_thumbnail() {
    // The cache is keyed by ETag, so a replaced photo shows as itself at once, on every
    // instance, without any instance being told.
    let server = TestServer::start().await;
    server
        .client
        .create_bucket()
        .bucket("pics")
        .send()
        .await
        .unwrap();
    put(
        &server,
        "pics",
        "p.png",
        encode(picture(600, 300), ImageFormat::Png),
    )
    .await;
    let (_, img) = thumbnail(&server.client, "pics", "p.png", "256", None)
        .await
        .unwrap();
    assert_eq!((img.width(), img.height()), (256, 128));
    put(
        &server,
        "pics",
        "p.png",
        encode(picture(300, 600), ImageFormat::Png),
    )
    .await;
    let (_, img) = thumbnail(&server.client, "pics", "p.png", "256", None)
        .await
        .unwrap();
    assert_eq!((img.width(), img.height()), (128, 256));
    // The old one is still there, for the sweep.
    assert_eq!(cached(&server, "pics", "p.png").await.len(), 2);
}

#[tokio::test]
async fn a_version_named_gets_its_own_thumbnail() {
    // GetObject serves the version a versionId names; its thumbnail must be of that version.
    let server = TestServer::start().await;
    server
        .client
        .create_bucket()
        .bucket("pics")
        .send()
        .await
        .unwrap();
    server
        .client
        .put_bucket_versioning()
        .bucket("pics")
        .versioning_configuration(
            VersioningConfiguration::builder()
                .status(BucketVersioningStatus::Enabled)
                .build(),
        )
        .send()
        .await
        .unwrap();
    let first = server
        .client
        .put_object()
        .bucket("pics")
        .key("p.png")
        .body(ByteStream::from(encode(
            picture(600, 300),
            ImageFormat::Png,
        )))
        .send()
        .await
        .unwrap();
    put(
        &server,
        "pics",
        "p.png",
        encode(picture(300, 600), ImageFormat::Png),
    )
    .await;
    let (_, img) = thumbnail(&server.client, "pics", "p.png", "256", first.version_id())
        .await
        .unwrap();
    assert_eq!((img.width(), img.height()), (256, 128));
    let (_, img) = thumbnail(&server.client, "pics", "p.png", "256", None)
        .await
        .unwrap();
    assert_eq!((img.width(), img.height()), (128, 256));
}

#[tokio::test]
async fn the_sweep_removes_thumbnails_of_objects_gone_or_changed() {
    // A deleted or replaced photo's thumbnails are never served again; left alone, they would
    // take space for ever. The ones still current must stay, or every grid is made again.
    let server = TestServer::start().await;
    server
        .client
        .create_bucket()
        .bucket("pics")
        .send()
        .await
        .unwrap();
    for key in ["kept.png", "dir/changed.png", "gone.png"] {
        put(
            &server,
            "pics",
            key,
            encode(picture(600, 300), ImageFormat::Png),
        )
        .await;
        for size in ["128", "256"] {
            thumbnail(&server.client, "pics", key, size, None)
                .await
                .unwrap();
        }
    }
    put(
        &server,
        "pics",
        "dir/changed.png",
        encode(picture(300, 600), ImageFormat::Png),
    )
    .await;
    server
        .client
        .delete_object()
        .bucket("pics")
        .key("gone.png")
        .send()
        .await
        .unwrap();

    assert_eq!(server.dal.sweep_thumbnails().await.unwrap(), 4);
    assert_eq!(cached(&server, "pics", "kept.png").await.len(), 2);
    assert_eq!(
        cached(&server, "pics", "dir/changed.png").await,
        Vec::<String>::new()
    );
    assert!(!server.exists(".s3dal/thumbs/pics/gone.png!!/").await);
    // Nothing left to remove.
    assert_eq!(server.dal.sweep_thumbnails().await.unwrap(), 0);
    assert_eq!(cached(&server, "pics", "kept.png").await.len(), 2);
}

#[tokio::test]
async fn deleting_the_bucket_removes_its_thumbnails() {
    // A bucket created again under the name must not find thumbnails of the old one's objects.
    let server = TestServer::start().await;
    server
        .client
        .create_bucket()
        .bucket("pics")
        .send()
        .await
        .unwrap();
    put(
        &server,
        "pics",
        "p.png",
        encode(picture(60, 30), ImageFormat::Png),
    )
    .await;
    thumbnail(&server.client, "pics", "p.png", "128", None)
        .await
        .unwrap();
    server
        .client
        .delete_object()
        .bucket("pics")
        .key("p.png")
        .send()
        .await
        .unwrap();
    server
        .client
        .delete_bucket()
        .bucket("pics")
        .send()
        .await
        .unwrap();
    assert_eq!(
        server.names(".s3dal/thumbs/pics").await,
        Vec::<String>::new()
    );
}

#[tokio::test]
async fn sources_over_a_bound_or_of_another_format_are_refused() {
    // A thumbnail holds its decoded source in memory; an object past the bounds would take
    // hundreds of megabytes per request, and a format without a decoder is no image here.
    let server = TestServer::start().await;
    server
        .client
        .create_bucket()
        .bucket("pics")
        .send()
        .await
        .unwrap();
    // Nothing of it is read: the size alone refuses it.
    server
        .plant("pics", "huge.jpg", &vec![0; (64 << 20) + 1])
        .await;
    put(&server, "pics", "wide.gif", gif_claiming(10_000, 5_001)).await;
    // Within the bound by its pixels, but past what a decoder may allocate.
    put(&server, "pics", "vast.gif", gif_claiming(65_535, 65_535)).await;
    put(&server, "pics", "notes.txt", b"not an image".to_vec()).await;
    // An image, in a format this build has no decoder for: a BMP file's header.
    put(
        &server,
        "pics",
        "p.bmp",
        b"BM\x36\0\0\0\0\0\0\0\x36\0\0\0\x28\0".to_vec(),
    )
    .await;
    let mut broken = encode(picture(600, 300), ImageFormat::Jpeg);
    broken.truncate(300);
    put(&server, "pics", "broken.jpg", broken).await;

    for (key, code) in [
        ("huge.jpg", "EntityTooLarge"),
        ("wide.gif", "EntityTooLarge"),
        ("vast.gif", "EntityTooLarge"),
        ("notes.txt", "NotImplemented"),
        ("p.bmp", "NotImplemented"),
        ("broken.jpg", "InvalidRequest"),
    ] {
        let err = thumbnail(&server.client, "pics", key, "256", None)
            .await
            .unwrap_err();
        assert_eq!(err, code, "{key}");
        assert_eq!(
            cached(&server, "pics", key).await,
            Vec::<String>::new(),
            "{key}"
        );
    }
    // The bound is on pixels, not on either side: a strip wider than the widest 50 MP square
    // is made.
    put(&server, "pics", "strip.gif", gif_claiming(12_000, 300)).await;
    thumbnail(&server.client, "pics", "strip.gif", "128", None)
        .await
        .unwrap();
}

#[tokio::test]
async fn a_thumbnail_is_read_access_as_a_download_is() {
    // A drop box must not see what was dropped, not even small; an unsigned request gets a
    // thumbnail only where it would get the object.
    let mut dropper = KeyPair::new("dropper", "dropper-secret");
    dropper.allow = Some(vec!["pics".into()]);
    dropper.access = s3dal::access::Access::WriteOnly;
    let keys = [KeyPair::new("admin", "admin-secret"), dropper];
    let server =
        TestServer::start_serving(Backend::from_env().await, Settings::default(), &keys, None)
            .await;
    server
        .client
        .create_bucket()
        .bucket("pics")
        .send()
        .await
        .unwrap();
    put(
        &server,
        "pics",
        "p.png",
        encode(picture(600, 300), ImageFormat::Png),
    )
    .await;

    let dropper = client(&server.endpoint, "dropper", "dropper-secret");
    let err = thumbnail(&dropper, "pics", "p.png", "256", None)
        .await
        .unwrap_err();
    assert_eq!(err, "AccessDenied");

    let unsigned = format!("/pics/p.png?{PARAM}=256");
    let (status, body) = raw_request(&server.endpoint, "GET", &unsigned, &[], b"").await;
    assert_eq!(status, 403, "{}", String::from_utf8_lossy(&body));
    assert_eq!(cached(&server, "pics", "p.png").await, Vec::<String>::new());
    server.dal.set_bucket_public("pics", true).await.unwrap();
    let (status, body) = raw_request(&server.endpoint, "GET", &unsigned, &[], b"").await;
    assert_eq!(status, 200);
    let img = image::load_from_memory(&body).unwrap();
    assert_eq!((img.width(), img.height()), (256, 128));
}

#[tokio::test]
async fn a_presigned_url_signs_the_size() {
    // Apps hand a browser a presigned thumbnail link; the size is part of what was signed, so
    // the link cannot be turned into a bigger thumbnail, or into the original.
    let server = TestServer::start().await;
    server
        .client
        .create_bucket()
        .bucket("pics")
        .send()
        .await
        .unwrap();
    put(
        &server,
        "pics",
        "p.png",
        encode(picture(600, 300), ImageFormat::Png),
    )
    .await;
    let presigned = server
        .client
        .get_object()
        .bucket("pics")
        .key("p.png")
        .customize()
        .mutate_request(|req| {
            let sep = if req.uri().contains('?') { '&' } else { '?' };
            let uri = format!("{}{sep}{PARAM}=128", req.uri());
            req.set_uri(uri).unwrap();
        })
        .presigned(PresigningConfig::expires_in(Duration::from_secs(300)).unwrap())
        .await
        .unwrap();
    let signed = presigned.uri().strip_prefix(&server.endpoint).unwrap();
    assert!(signed.contains(&format!("{PARAM}=128")), "{signed}");
    let (status, body) = raw_request(&server.endpoint, "GET", signed, &[], b"").await;
    assert_eq!(status, 200, "{}", String::from_utf8_lossy(&body));
    let img = image::load_from_memory(&body).unwrap();
    assert_eq!((img.width(), img.height()), (128, 64));
    for tampered in [
        signed.replace(&format!("{PARAM}=128"), &format!("{PARAM}=1024")),
        signed
            .replace(&format!("&{PARAM}=128"), "")
            .replace(&format!("{PARAM}=128&"), ""),
    ] {
        let (status, body) = raw_request(&server.endpoint, "GET", &tampered, &[], b"").await;
        let body = String::from_utf8_lossy(&body);
        assert_eq!(status, 403, "{tampered}: {body}");
        assert!(body.contains("SignatureDoesNotMatch"), "{body}");
    }
}
