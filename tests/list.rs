mod common;

use std::collections::{BTreeMap, BTreeSet};

use aws_sdk_s3::Client;
use aws_sdk_s3::error::ProvideErrorMetadata;
use aws_sdk_s3::operation::list_object_versions::ListObjectVersionsOutput;
use aws_sdk_s3::operation::list_objects_v2::ListObjectsV2Output;
use aws_sdk_s3::primitives::ByteStream;
use aws_sdk_s3::types::{CommonPrefix, Delete, EncodingType, Object, ObjectIdentifier};
use common::TestServer;
use s3dal::meta;

#[derive(Debug, Default, Clone, Copy)]
struct Params<'a> {
    prefix: Option<&'a str>,
    delimiter: Option<&'a str>,
    start_after: Option<&'a str>,
    max_keys: Option<i32>,
}

async fn list(
    c: &Client,
    bucket: &str,
    p: Params<'_>,
    token: Option<String>,
) -> ListObjectsV2Output {
    c.list_objects_v2()
        .bucket(bucket)
        .set_prefix(p.prefix.map(str::to_owned))
        .set_delimiter(p.delimiter.map(str::to_owned))
        .set_start_after(p.start_after.map(str::to_owned))
        .set_max_keys(p.max_keys)
        .set_continuation_token(token)
        .send()
        .await
        .unwrap()
}

/// The names of one response in listing order. S3 returns objects and CommonPrefixes as two
/// lists, each in key order. A CommonPrefix ends with the delimiter; a listed key never ends
/// with `/`, since folder markers are not listed.
fn entries(out: &ListObjectsV2Output) -> Vec<String> {
    let keys: Vec<String> = out
        .contents()
        .iter()
        .map(|o| o.key().unwrap().to_owned())
        .collect();
    let prefixes: Vec<String> = out
        .common_prefixes()
        .iter()
        .map(|p| p.prefix().unwrap().to_owned())
        .collect();
    assert!(keys.is_sorted(), "objects out of order: {keys:?}");
    assert!(prefixes.is_sorted(), "prefixes out of order: {prefixes:?}");
    let delimiter = out.delimiter().unwrap_or_default();
    assert!(keys.iter().all(|k| !k.ends_with('/')), "{keys:?}");
    assert!(
        prefixes
            .iter()
            .all(|p| !delimiter.is_empty() && p.ends_with(delimiter)),
        "{prefixes:?}"
    );
    let mut all = keys;
    all.extend(prefixes);
    all.sort();
    assert_eq!(out.key_count(), Some(all.len() as i32), "KeyCount");
    all
}

struct Listing {
    pages: Vec<Vec<String>>,
    sizes: BTreeMap<String, i64>,
}

impl Listing {
    fn names(&self) -> Vec<String> {
        self.pages.concat()
    }

    /// The names with each CommonPrefix as `CP:<prefix>`. With a delimiter other than `/`,
    /// a CommonPrefix and a key look alike; only keys have a size.
    fn tagged(&self) -> Vec<String> {
        self.names()
            .into_iter()
            .map(|name| {
                if self.sizes.contains_key(&name) {
                    name
                } else {
                    format!("CP:{name}")
                }
            })
            .collect()
    }
}

/// Follows continuation tokens to the end, as SDK paginators do, checking what each page
/// promises: a truncated page is full and carries a token, the last one carries none.
async fn list_all(c: &Client, bucket: &str, p: Params<'_>) -> Listing {
    let max = p.max_keys.map_or(1000, |m| m as usize);
    let mut listing = Listing {
        pages: Vec::new(),
        sizes: BTreeMap::new(),
    };
    let mut token = None;
    loop {
        let out = list(c, bucket, p, token.take()).await;
        let page = entries(&out);
        let truncated = out.is_truncated() == Some(true);
        assert_eq!(truncated, out.next_continuation_token().is_some(), "{p:?}");
        if truncated {
            assert_eq!(page.len(), max, "a truncated page must be full: {p:?}");
        } else {
            assert!(page.len() <= max, "{p:?}");
        }
        for o in out.contents() {
            listing
                .sizes
                .insert(o.key().unwrap().to_owned(), o.size().unwrap());
        }
        listing.pages.push(page);
        if !truncated {
            return listing;
        }
        assert!(listing.pages.len() < 10_000, "pagination never ends: {p:?}");
        token = out.next_continuation_token().map(str::to_owned);
    }
}

async fn with_keys(server: &TestServer, bucket: &str, keys: &[&str]) {
    server.create_bucket_dir(bucket).await;
    for key in keys {
        server.plant(bucket, key, b"x").await;
    }
}

#[tokio::test]
async fn keys_are_listed_in_utf8_byte_order() {
    // Clients merge and resume listings by comparing keys bytewise; any other order makes them
    // skip or repeat keys. A walk that sorts a directory `a` by its bare name puts `a/…` before
    // `a-` and `a.txt`, which byte order puts first (`-` 0x2D, `.` 0x2E, `/` 0x2F, `0` 0x30).
    let server = TestServer::start().await;
    let keys = [
        "😀", "a0", "z", "a/b", "中", "a-", "a.txt", "ø", "a/a/a", "b",
    ];
    with_keys(&server, "order", &keys).await;
    let c = &server.client;

    let flat = list_all(c, "order", Params::default()).await.names();
    let expected = [
        "a-", "a.txt", "a/a/a", "a/b", "a0", "b", "z", "ø", "中", "😀",
    ];
    assert_eq!(flat, expected);

    let slash = Params {
        delimiter: Some("/"),
        ..Params::default()
    };
    let grouped = list_all(c, "order", slash).await.names();
    assert_eq!(
        grouped,
        ["a-", "a.txt", "a/", "a0", "b", "z", "ø", "中", "😀"]
    );
}

#[tokio::test]
async fn delimiter_folds_every_key_below_a_directory_into_one_common_prefix() {
    // This is how `aws s3 ls` and file browsers show folders; a folder listed twice, or its
    // contents listed alongside it, shows up as duplicate or misplaced entries.
    let server = TestServer::start().await;
    with_keys(
        &server,
        "tree",
        &["a/1", "a/2", "a/b/c", "a/b/d/e", "x", "y/z"],
    )
    .await;
    let c = &server.client;
    let at = |prefix| Params {
        prefix,
        delimiter: Some("/"),
        ..Params::default()
    };

    assert_eq!(
        list_all(c, "tree", at(None)).await.names(),
        ["a/", "x", "y/"]
    );
    assert_eq!(
        list_all(c, "tree", at(Some("a/"))).await.names(),
        ["a/1", "a/2", "a/b/"]
    );
    assert_eq!(
        list_all(c, "tree", at(Some("a/b/"))).await.names(),
        ["a/b/c", "a/b/d/"]
    );
}

#[tokio::test]
async fn a_prefix_may_end_mid_name_or_mid_directory() {
    // S3 prefixes are plain string matches, not directory paths; autocompletion in clients
    // and date-partitioned layouts (`logs/2021-0`) depend on that.
    let server = TestServer::start().await;
    let keys = [
        "photos/2020/jan.jpg",
        "photos/2021/feb.jpg",
        "photos/2021x.jpg",
        "photos/readme",
        "photosynthesis",
        "other",
    ];
    with_keys(&server, "pfx", &keys).await;
    let c = &server.client;
    let p = |prefix, delimiter| Params {
        prefix: Some(prefix),
        delimiter,
        ..Params::default()
    };

    assert_eq!(
        list_all(c, "pfx", p("photo", None)).await.names(),
        keys[..5]
    );
    assert_eq!(
        list_all(c, "pfx", p("photo", Some("/"))).await.names(),
        ["photos/", "photosynthesis"]
    );
    assert_eq!(
        list_all(c, "pfx", p("photos/202", None)).await.names(),
        keys[..3]
    );
    assert_eq!(
        list_all(c, "pfx", p("photos/202", Some("/"))).await.names(),
        ["photos/2020/", "photos/2021/", "photos/2021x.jpg"]
    );
    assert_eq!(
        list_all(c, "pfx", p("photos/2021/f", Some("/")))
            .await
            .names(),
        ["photos/2021/feb.jpg"]
    );
    assert!(
        list_all(c, "pfx", p("photos/2022", None))
            .await
            .names()
            .is_empty()
    );

    let out = list(c, "pfx", p("photos/202", Some("/")), None).await;
    assert_eq!(out.prefix(), Some("photos/202"));
    assert_eq!(out.delimiter(), Some("/"));
    assert_eq!(out.name(), Some("pfx"));
}

#[tokio::test]
async fn continuation_tokens_visit_every_entry_exactly_once() {
    // SDK paginators follow NextContinuationToken until IsTruncated is false. A token must
    // carry any key (`+`, spaces, `%`, multi-byte UTF-8) exactly, and a token issued after a
    // CommonPrefix must skip that prefix's whole subtree.
    let server = TestServer::start().await;
    let keys = [
        "%", "a/1", "a/2", "a/3", "b", "c/1", "e+f", "g h", "ø/1", "😀",
    ];
    with_keys(&server, "pages", &keys).await;
    let c = &server.client;

    for delimiter in [None, Some("/")] {
        let expected = list_all(
            c,
            "pages",
            Params {
                delimiter,
                ..Params::default()
            },
        )
        .await
        .names();
        assert_eq!(expected.len(), if delimiter.is_some() { 8 } else { 10 });
        for max_keys in [1, 2, 3] {
            let p = Params {
                delimiter,
                max_keys: Some(max_keys),
                ..Params::default()
            };
            let listing = list_all(c, "pages", p).await;
            assert_eq!(listing.names(), expected, "{p:?}");
            assert_eq!(
                listing.pages.len(),
                expected.len().div_ceil(max_keys as usize),
                "{p:?}"
            );
        }
    }

    let slash_by_one = Params {
        delimiter: Some("/"),
        max_keys: Some(1),
        ..Params::default()
    };
    let pages = list_all(c, "pages", slash_by_one).await.pages;
    assert_eq!(pages[1], ["a/"]);
    assert_eq!(
        pages[2],
        ["b"],
        "the page after `a/` resumes past its subtree"
    );
}

#[tokio::test]
async fn start_after_resumes_strictly_after_the_given_name() {
    // Sync tools resume an interrupted listing with start-after; the named key itself, or
    // anything before it, must not come back.
    let server = TestServer::start().await;
    with_keys(&server, "resume", &["a/1", "a/2", "b", "c/1", "d"]).await;
    let c = &server.client;
    let after = |start_after, delimiter| Params {
        start_after: Some(start_after),
        delimiter,
        ..Params::default()
    };

    assert_eq!(
        list_all(c, "resume", after("a/1", None)).await.names(),
        ["a/2", "b", "c/1", "d"]
    );
    assert_eq!(
        list_all(c, "resume", after("b", None)).await.names(),
        ["c/1", "d"]
    );
    assert_eq!(
        list_all(c, "resume", after("bb", None)).await.names(),
        ["c/1", "d"],
        "start-after need not name an existing key"
    );
    assert_eq!(
        list_all(c, "resume", after("a/", Some("/"))).await.names(),
        ["b", "c/", "d"]
    );
    assert!(
        list_all(c, "resume", after("zzz", None))
            .await
            .names()
            .is_empty()
    );

    // Paginating from a start-after position keeps honoring it on every page.
    let p = Params {
        max_keys: Some(1),
        ..after("a/1", None)
    };
    assert_eq!(
        list_all(c, "resume", p).await.names(),
        ["a/2", "b", "c/1", "d"]
    );
    let out = list(c, "resume", p, None).await;
    assert_eq!(out.start_after(), Some("a/1"));
}

#[tokio::test]
async fn max_keys_zero_returns_an_empty_page_that_is_not_truncated() {
    // AWS answers max-keys=0 this way; a token here would send paginators into an endless
    // loop of empty pages.
    let server = TestServer::start().await;
    with_keys(&server, "zero", &["a", "b"]).await;
    let p = Params {
        max_keys: Some(0),
        ..Params::default()
    };
    let out = list(&server.client, "zero", p, None).await;
    assert!(entries(&out).is_empty());
    assert_eq!(out.is_truncated(), Some(false));
    assert_eq!(out.next_continuation_token(), None);
    assert_eq!(out.max_keys(), Some(0));
}

#[tokio::test]
async fn max_keys_one_returns_one_entry_per_page() {
    let server = TestServer::start().await;
    with_keys(&server, "one", &["a", "b/c", "d"]).await;
    let p = Params {
        max_keys: Some(1),
        delimiter: Some("/"),
        ..Params::default()
    };
    let listing = list_all(&server.client, "one", p).await;
    assert_eq!(listing.pages, [["a"], ["b/"], ["d"]]);
}

#[tokio::test]
async fn max_keys_is_capped_at_1000() {
    // A client asking for more must still get a bounded response, and must be told that more
    // remains rather than silently receive a partial listing.
    let server = TestServer::start().await;
    server.create_bucket_dir("many").await;
    for i in 0..=1000 {
        server.plant("many", &format!("k{i:04}"), b"").await;
    }
    let c = &server.client;
    for max_keys in [None, Some(5000)] {
        let p = Params {
            max_keys,
            ..Params::default()
        };
        let out = list(c, "many", p, None).await;
        assert_eq!(out.contents().len(), 1000);
        assert_eq!(out.max_keys(), Some(1000));
        assert_eq!(out.is_truncated(), Some(true));
        let rest = list(
            c,
            "many",
            p,
            out.next_continuation_token().map(str::to_owned),
        )
        .await;
        assert_eq!(entries(&rest), ["k1000"]);
        assert_eq!(rest.is_truncated(), Some(false));
    }
}

#[tokio::test]
async fn encoding_type_url_encodes_every_name_in_the_response() {
    // Clients request url encoding for keys XML cannot carry and decode Key, Prefix,
    // Delimiter and StartAfter themselves, some as form values (botocore) and some as plain
    // percent-encoding (Go, most hand-written clients). A name left raw, a `+` left unescaped,
    // or a space written `+` comes back changed to one kind or the other; ceph s3-tests
    // expect `%20`.
    let server = TestServer::start().await;
    with_keys(
        &server,
        "enc",
        &["dir ø/a+b", "dir ø/x%20y", "dir ø/sub dir/z", "dir ø/_"],
    )
    .await;
    let out = server
        .client
        .list_objects_v2()
        .bucket("enc")
        .prefix("dir ø/")
        .delimiter("/")
        .start_after("dir ø/a")
        .encoding_type(EncodingType::Url)
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = out.contents().iter().map(|o| o.key().unwrap()).collect();
    assert_eq!(keys, ["dir%20%C3%B8/a%2Bb", "dir%20%C3%B8/x%2520y"]);
    let prefixes: Vec<&str> = out
        .common_prefixes()
        .iter()
        .map(|p| p.prefix().unwrap())
        .collect();
    assert_eq!(prefixes, ["dir%20%C3%B8/sub%20dir/"]);
    assert_eq!(out.prefix(), Some("dir%20%C3%B8/"));
    assert_eq!(out.delimiter(), Some("/"));
    assert_eq!(out.start_after(), Some("dir%20%C3%B8/a"));
    assert_eq!(out.encoding_type(), Some(&EncodingType::Url));
}

#[tokio::test]
async fn a_listing_parameter_xml_cannot_carry_needs_url_encoding() {
    // Listings echo their parameters, and s3s answers 500 rather than put a control character
    // into XML. A 400 tells the client what to change; URL-encoded, the listing works.
    let server = TestServer::start().await;
    with_keys(&server, "ctl", &["a"]).await;
    let err = server
        .client
        .list_objects_v2()
        .bucket("ctl")
        .prefix("x\u{1}")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidArgument"), "{err:?}");
    // Behind a later continuation token, start-after decides nothing but is still echoed.
    with_keys(&server, "ctl", &["b"]).await;
    let first = server
        .client
        .list_objects_v2()
        .bucket("ctl")
        .max_keys(1)
        .send()
        .await
        .unwrap();
    let err = server
        .client
        .list_objects_v2()
        .bucket("ctl")
        .continuation_token(first.next_continuation_token().unwrap())
        .start_after("\u{1}")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidArgument"), "{err:?}");
    let err = server
        .client
        .list_multipart_uploads()
        .bucket("ctl")
        .key_marker("x\u{1}")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidArgument"), "{err:?}");
    let out = server
        .client
        .list_objects_v2()
        .bucket("ctl")
        .prefix("x\u{1}")
        .encoding_type(EncodingType::Url)
        .send()
        .await
        .unwrap();
    assert_eq!(out.prefix(), Some("x%01"));
    assert_eq!(out.key_count(), Some(0));
}

#[tokio::test]
async fn empty_directories_are_neither_keys_nor_common_prefixes() {
    // fs keeps directories S3 has no notion of: folder markers, or parents left behind by
    // other tools. Listing them would show folders with nothing in them, or keys nobody can
    // GET.
    let server = TestServer::start().await;
    with_keys(&server, "hollow", &["full/x"]).await;
    for dir in ["empty", "nested/empty/deeper", "full/hollow"] {
        server
            .op
            .create_dir(&format!("hollow/{dir}/"))
            .await
            .unwrap();
    }
    let c = &server.client;
    let slash = |prefix| Params {
        prefix,
        delimiter: Some("/"),
        ..Params::default()
    };

    assert_eq!(list_all(c, "hollow", slash(None)).await.names(), ["full/"]);
    assert_eq!(
        list_all(c, "hollow", slash(Some("full/"))).await.names(),
        ["full/x"]
    );
    assert_eq!(
        list_all(c, "hollow", Params::default()).await.names(),
        ["full/x"]
    );
}

#[tokio::test]
async fn listing_reports_the_etag_size_and_mtime_head_reports() {
    // Sync tools (rclone, aws s3 sync) compare listing ETags and sizes with what they saw
    // before; a listing that disagrees with HEAD reads as a changed or corrupted object.
    let server = TestServer::start().await;
    server.create_bucket_dir("etags").await;
    server
        .client
        .put_object()
        .bucket("etags")
        .key("stored")
        .body(ByteStream::from_static(b"hello"))
        .send()
        .await
        .unwrap();
    server.plant("etags", "planted", b"from another tool").await;

    let out = list(&server.client, "etags", Params::default(), None).await;
    assert_eq!(out.contents().len(), 2);
    for object in out.contents() {
        let key = object.key().unwrap();
        let head = server
            .client
            .head_object()
            .bucket("etags")
            .key(key)
            .send()
            .await
            .unwrap();
        assert_eq!(object.e_tag(), head.e_tag(), "{key}");
        assert_eq!(object.size(), head.content_length(), "{key}");
        // HEAD carries an HTTP date, so only whole seconds can agree.
        assert_eq!(
            object.last_modified().map(|t| t.secs()),
            head.last_modified().map(|t| t.secs()),
            "{key}"
        );
    }
    // The stored MD5 lives in user metadata, which fs listings do not carry: it shows up only
    // because each listed entry is stat'ed.
    let stored = out.contents().iter().find(|o| o.key() == Some("stored"));
    let md5 = meta::md5_hex(b"hello");
    assert_eq!(stored.unwrap().e_tag(), Some(format!("\"{md5}\"").as_str()));
}

/// Pages through a v1 listing the way v1 clients do: from NextMarker when the response has
/// one, otherwise from the last key, since AWS sends NextMarker only with a delimiter.
async fn list_all_v1(
    c: &Client,
    bucket: &str,
    delimiter: Option<&str>,
    max_keys: i32,
) -> Vec<String> {
    let mut names = Vec::new();
    let mut marker: Option<String> = None;
    loop {
        let out = c
            .list_objects()
            .bucket(bucket)
            .set_delimiter(delimiter.map(str::to_owned))
            .max_keys(max_keys)
            .set_marker(marker.clone())
            .send()
            .await
            .unwrap();
        let mut page: Vec<String> = out
            .contents()
            .iter()
            .map(|o| o.key().unwrap().to_owned())
            .chain(
                out.common_prefixes()
                    .iter()
                    .map(|p| p.prefix().unwrap().to_owned()),
            )
            .collect();
        page.sort();
        names.extend(page.iter().cloned());
        if out.is_truncated() != Some(true) {
            assert_eq!(out.next_marker(), None);
            return names;
        }
        marker = Some(match delimiter {
            Some(_) => out
                .next_marker()
                .expect("NextMarker with a delimiter")
                .to_owned(),
            None => {
                assert_eq!(
                    out.next_marker(),
                    None,
                    "AWS omits NextMarker without a delimiter"
                );
                page.last().unwrap().clone()
            }
        });
    }
}

#[tokio::test]
async fn v1_listing_pages_through_the_same_entries_as_v2() {
    // rclone with `provider = Other` lists with v1 by default; it must see exactly what v2
    // shows, or a sync would copy or delete the wrong files.
    let server = TestServer::start().await;
    with_keys(
        &server,
        "v1-pages",
        &["a.txt", "a/b", "a/c/d", "b", "c/e", "c/f", "d"],
    )
    .await;
    for delimiter in [None, Some("/")] {
        let v2 = list_all(
            &server.client,
            "v1-pages",
            Params {
                delimiter,
                max_keys: Some(2),
                ..Default::default()
            },
        )
        .await
        .names();
        let v1 = list_all_v1(&server.client, "v1-pages", delimiter, 2).await;
        assert_eq!(v1, v2, "delimiter {delimiter:?}");
    }
}

#[tokio::test]
async fn v1_next_marker_is_the_last_name_not_a_token() {
    // v1 clients send NextMarker back as `marker`, a plain name. A v2 token there would be
    // taken as a key and resume the listing at the wrong place.
    let server = TestServer::start().await;
    with_keys(&server, "v1-marker", &["a/x", "b", "c"]).await;
    let out = server
        .client
        .list_objects()
        .bucket("v1-marker")
        .delimiter("/")
        .max_keys(2)
        .send()
        .await
        .unwrap();
    assert_eq!(out.is_truncated(), Some(true));
    assert_eq!(out.next_marker(), Some("b"));
}

#[tokio::test]
async fn a_prefix_cannot_reach_outside_its_bucket() {
    // The walk turns the prefix's directory part into a backend path; `..` there must not
    // let one bucket's listing show another bucket's keys.
    let server = TestServer::start().await;
    with_keys(&server, "inner", &["x"]).await;
    with_keys(&server, "other", &["secret"]).await;
    for prefix in ["../other/", "../other/s", "x/../../other/"] {
        for delimiter in [None, Some("/")] {
            let p = Params {
                prefix: Some(prefix),
                delimiter,
                ..Params::default()
            };
            assert!(
                list_all(&server.client, "inner", p)
                    .await
                    .names()
                    .is_empty(),
                "{p:?}"
            );
        }
    }
}

#[tokio::test]
async fn listing_a_missing_bucket_is_no_such_bucket() {
    let server = TestServer::start().await;
    let err = server
        .client
        .list_objects_v2()
        .bucket("missing")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchBucket"), "{err:?}");
}

/// Keys and CommonPrefixes of one listing, which v1 and v2 must agree on; both echo the
/// delimiter.
async fn grouped(
    c: &Client,
    bucket: &str,
    prefix: &str,
    delimiter: &str,
) -> (Vec<String>, Vec<String>) {
    let v2 = c
        .list_objects_v2()
        .bucket(bucket)
        .prefix(prefix)
        .delimiter(delimiter)
        .send()
        .await
        .unwrap();
    let v1 = c
        .list_objects()
        .bucket(bucket)
        .prefix(prefix)
        .delimiter(delimiter)
        .send()
        .await
        .unwrap();
    assert_eq!(v2.delimiter(), Some(delimiter));
    assert_eq!(v1.delimiter(), Some(delimiter));
    let names = |objects: &[Object], prefixes: &[CommonPrefix]| {
        (
            objects
                .iter()
                .map(|o| o.key().unwrap().to_owned())
                .collect::<Vec<_>>(),
            prefixes
                .iter()
                .map(|p| p.prefix().unwrap().to_owned())
                .collect::<Vec<_>>(),
        )
    };
    let from_v2 = names(v2.contents(), v2.common_prefixes());
    assert_eq!(names(v1.contents(), v1.common_prefixes()), from_v2, "v1");
    from_v2
}

#[tokio::test]
async fn any_delimiter_groups_keys_up_to_its_first_occurrence_after_the_prefix() {
    // S3 groups by any string, in v1 and v2 alike: flat namespaces group by `-` or `.`,
    // date-partitioned ones by several characters, and ceph s3-tests list with `a`, `%`, ` `,
    // `.` and a newline. Refusing those breaks such clients; grouping by `/` regardless would
    // hand them groups they did not ask for.
    let server = TestServer::start().await;
    type Case<'a> = (
        &'a [&'a str],
        &'a str,
        &'a str,
        &'a [&'a str],
        &'a [&'a str],
    );
    let cases: &[Case] = &[
        (
            &["bar", "baz", "cab", "foo"],
            "",
            "a",
            &["foo"],
            &["ba", "ca"],
        ),
        (
            &["b%ar", "b%az", "c%ab", "foo"],
            "",
            "%",
            &["foo"],
            &["b%", "c%"],
        ),
        (
            &["b ar", "b az", "c ab", "foo"],
            "",
            " ",
            &["foo"],
            &["b ", "c "],
        ),
        (
            &["b.ar", "b.az", "c.ab", "foo"],
            "",
            ".",
            &["foo"],
            &["b.", "c."],
        ),
        // A delimiter that occurs nowhere groups nothing.
        (&["bar", "cab"], "", "\n", &["bar", "cab"], &[]),
        // Only what follows the prefix counts.
        (&["bar", "bazar", "cab"], "ba", "a", &["bar"], &["baza"]),
        (
            &[
                "2021-01-01.log",
                "2021-01-02.log",
                "2021-02-01.log",
                "readme",
            ],
            "",
            "-01",
            &["readme"],
            &["2021-01", "2021-02-01"],
        ),
        // Holding `/`, a delimiter need not end at a directory boundary.
        (
            &["x/a", "x/b/1", "x/bz", "x/c/b"],
            "",
            "/b",
            &["x/a"],
            &["x/b", "x/c/b"],
        ),
        // Other than `/`, below a directory and across a directory boundary.
        (&["p/q-1/x", "p/q-2", "p/r"], "p/", "-", &["p/r"], &["p/q-"]),
        // After a prefix that ends with the delimiter, the key equal to it is a key.
        (
            &["a-", "a-b", "a-b-c", "a-c-"],
            "a-",
            "-",
            &["a-", "a-b"],
            &["a-b-", "a-c-"],
        ),
    ];
    for (i, &(keys, prefix, delimiter, want_keys, want_prefixes)) in cases.iter().enumerate() {
        let bucket = format!("delim-{i}");
        with_keys(&server, &bucket, keys).await;
        let (keys, prefixes) = grouped(&server.client, &bucket, prefix, delimiter).await;
        assert_eq!(keys, want_keys, "{delimiter:?} after {prefix:?}");
        assert_eq!(prefixes, want_prefixes, "{delimiter:?} after {prefix:?}");
    }
}

#[tokio::test]
async fn pages_resume_past_a_common_prefix_of_any_delimiter() {
    // A token or marker issued after a CommonPrefix must skip its whole group, wherever its
    // keys lie, or the next page lists the folder again; a marker inside a group skips it, as
    // inside a `/` group. v1 clients resume from NextMarker, which then names the group.
    let server = TestServer::start().await;
    let keys = ["a-1", "a-2/x", "a-3", "b", "c-1", "c-2/y", "d"];
    with_keys(&server, "dash", &keys).await;
    let c = &server.client;
    let dash = |max_keys, start_after| Params {
        delimiter: Some("-"),
        max_keys,
        start_after,
        ..Params::default()
    };
    let all = ["a-", "b", "c-", "d"];
    for max_keys in [1, 2, 3] {
        let v2 = list_all(c, "dash", dash(Some(max_keys), None)).await;
        assert_eq!(v2.tagged(), ["CP:a-", "b", "CP:c-", "d"], "{max_keys}");
        assert_eq!(list_all_v1(c, "dash", Some("-"), max_keys).await, all);
    }
    let first = c
        .list_objects()
        .bucket("dash")
        .delimiter("-")
        .max_keys(1)
        .send()
        .await
        .unwrap();
    assert_eq!(first.next_marker(), Some("a-"));

    for marker in ["a-", "a-2/x", "a-9"] {
        let v2 = list_all(c, "dash", dash(None, Some(marker))).await;
        assert_eq!(v2.names(), all[1..], "{marker}");
        let v1 = c
            .list_objects()
            .bucket("dash")
            .delimiter("-")
            .marker(marker)
            .send()
            .await
            .unwrap();
        let mut names: Vec<&str> = v1.contents().iter().map(|o| o.key().unwrap()).collect();
        names.extend(v1.common_prefixes().iter().map(|p| p.prefix().unwrap()));
        names.sort();
        assert_eq!(names, all[1..], "{marker}");
    }
}

#[tokio::test]
async fn a_continuation_token_s3dal_did_not_issue_is_invalid_argument() {
    // Guessing a resume point from a garbled token would skip or repeat keys silently.
    let server = TestServer::start().await;
    with_keys(&server, "token", &["a"]).await;
    for token in ["not a token!", "_w"] {
        let err = server
            .client
            .list_objects_v2()
            .bucket("token")
            .continuation_token(token)
            .send()
            .await
            .unwrap_err();
        assert_eq!(err.code(), Some("InvalidArgument"), "{token}: {err:?}");
    }
}

#[tokio::test]
async fn an_empty_continuation_token_lists_from_the_start() {
    // AWS reads an empty token as none, and echoes it.
    let server = TestServer::start().await;
    with_keys(&server, "token-empty", &["a", "b"]).await;
    let out = server
        .client
        .list_objects_v2()
        .bucket("token-empty")
        .continuation_token("")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = out.contents().iter().map(|o| o.key().unwrap()).collect();
    assert_eq!(keys, ["a", "b"]);
    assert_eq!(out.continuation_token(), Some(""));
}

#[tokio::test]
async fn an_empty_delimiter_groups_nothing_and_is_not_echoed() {
    // AWS answers an empty delimiter as if none was sent, in the response too.
    let server = TestServer::start().await;
    with_keys(&server, "delim-empty", &["a/b", "c"]).await;
    let c = &server.client;
    let v2 = c
        .list_objects_v2()
        .bucket("delim-empty")
        .delimiter("")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = v2.contents().iter().map(|o| o.key().unwrap()).collect();
    assert_eq!(keys, ["a/b", "c"]);
    assert!(v2.common_prefixes().is_empty());
    assert_eq!(v2.delimiter(), None);
    let v1 = c
        .list_objects()
        .bucket("delim-empty")
        .delimiter("")
        .send()
        .await
        .unwrap();
    let keys: Vec<&str> = v1.contents().iter().map(|o| o.key().unwrap()).collect();
    assert_eq!(keys, ["a/b", "c"]);
    assert_eq!(v1.delimiter(), None);
}

#[tokio::test]
async fn v1_reports_its_prefix_raw_even_with_url_encoding() {
    // As AWS does. botocore asks for url encoding on every v1 listing and decodes every name
    // but Prefix, so an encoded one would reach Python applications as `a%20b%2B/`.
    let server = TestServer::start().await;
    with_keys(&server, "v1-prefix", &["a b+/k"]).await;
    let out = server
        .client
        .list_objects()
        .bucket("v1-prefix")
        .prefix("a b+/")
        .encoding_type(EncodingType::Url)
        .send()
        .await
        .unwrap();
    assert_eq!(out.prefix(), Some("a b+/"));
    let keys: Vec<&str> = out.contents().iter().map(|o| o.key().unwrap()).collect();
    assert_eq!(keys, ["a%20b%2B/k"]);
}

#[tokio::test]
async fn negative_max_keys_and_unknown_encodings_are_invalid_arguments() {
    let server = TestServer::start().await;
    with_keys(&server, "args", &["a"]).await;
    let c = &server.client;
    let err = c
        .list_objects_v2()
        .bucket("args")
        .max_keys(-1)
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidArgument"), "{err:?}");
    let err = c
        .list_objects_v2()
        .bucket("args")
        .encoding_type(EncodingType::from("base64"))
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("InvalidArgument"), "{err:?}");
}

/// One ListObjectVersions page in listing order, versions as their key and CommonPrefixes as
/// `CP:<prefix>`, and the markers to resume from when it is truncated. Every version must be
/// its object's one version, `null`, and there are no delete markers.
fn version_page(out: &ListObjectVersionsOutput) -> (Vec<String>, Option<(String, String)>) {
    assert!(out.delete_markers().is_empty(), "{out:?}");
    let mut names: Vec<String> = out
        .versions()
        .iter()
        .map(|v| {
            assert_eq!(v.version_id(), Some("null"), "{v:?}");
            assert_eq!(v.is_latest(), Some(true), "{v:?}");
            v.key().unwrap().to_owned()
        })
        .chain(
            out.common_prefixes()
                .iter()
                .map(|p| format!("CP:{}", p.prefix().unwrap())),
        )
        .collect();
    names.sort_by(|a, b| {
        let name = |s: &String| s.strip_prefix("CP:").unwrap_or(s).to_owned();
        name(a).cmp(&name(b))
    });
    let next = if out.is_truncated() == Some(true) {
        Some((
            out.next_key_marker().unwrap().to_owned(),
            out.next_version_id_marker().unwrap().to_owned(),
        ))
    } else {
        assert_eq!(out.next_key_marker(), None);
        assert_eq!(out.next_version_id_marker(), None);
        None
    };
    (names, next)
}

/// Every ListObjectVersions page, each resuming from both next markers of the one before, as
/// clients page.
async fn version_pages(
    c: &Client,
    bucket: &str,
    delimiter: Option<&str>,
    max_keys: i32,
) -> Vec<Vec<String>> {
    let mut pages = Vec::new();
    let mut markers = None;
    loop {
        let (key_marker, version_id_marker) = markers.take().unzip();
        let out = c
            .list_object_versions()
            .bucket(bucket)
            .set_delimiter(delimiter.map(str::to_owned))
            .max_keys(max_keys)
            .set_key_marker(key_marker)
            .set_version_id_marker(version_id_marker)
            .send()
            .await
            .unwrap();
        let (page, next) = version_page(&out);
        if next.is_some() {
            assert_eq!(
                page.len(),
                max_keys as usize,
                "a truncated page must be full"
            );
        }
        pages.push(page);
        let Some(next) = next else {
            return pages;
        };
        assert!(pages.len() < 1000, "pagination never ends");
        markers = Some(next);
    }
}

#[tokio::test]
async fn listings_name_the_owner_the_acls_name() {
    // Tools that show or check ownership read it from listings, so it must be the owner the
    // ACLs name, and appear where S3 puts it: always in v1 and ListObjectVersions, in v2 only
    // when asked with fetch-owner.
    let server = TestServer::start().await;
    server.create_bucket_dir("owned").await;
    let c = &server.client;
    c.put_object()
        .bucket("owned")
        .key("k")
        .body(ByteStream::from_static(b"x"))
        .send()
        .await
        .unwrap();
    let acl = c
        .get_object_acl()
        .bucket("owned")
        .key("k")
        .send()
        .await
        .unwrap();
    let owner = acl.owner().unwrap();
    assert!(owner.id().is_some(), "{owner:?}");

    let v1 = c.list_objects().bucket("owned").send().await.unwrap();
    assert_eq!(v1.contents()[0].owner(), Some(owner));
    let v2 = list(c, "owned", Params::default(), None).await;
    assert_eq!(v2.contents()[0].owner(), None);
    let v2 = c
        .list_objects_v2()
        .bucket("owned")
        .fetch_owner(true)
        .send()
        .await
        .unwrap();
    assert_eq!(v2.contents()[0].owner(), Some(owner));
    let versions = c
        .list_object_versions()
        .bucket("owned")
        .send()
        .await
        .unwrap();
    assert_eq!(versions.versions()[0].owner(), Some(owner));
}

#[tokio::test]
async fn every_object_is_listed_as_its_null_version() {
    // s3dal keeps no versions, so each object is its one version, `null`, the latest, which is
    // how S3 reports objects in an unversioned bucket. Version-aware tools (rclone
    // --s3-versions, backup tools) compare these fields with ListObjectsV2's, and pass the
    // version id back to GET and DELETE, where only `null` names the object.
    let server = TestServer::start().await;
    server.create_bucket_dir("versions").await;
    let c = &server.client;
    c.put_object()
        .bucket("versions")
        .key("stored")
        .body(ByteStream::from_static(b"hello"))
        .send()
        .await
        .unwrap();
    server
        .plant("versions", "dir/planted", b"from another tool")
        .await;

    let objects = list(c, "versions", Params::default(), None).await;
    let out = c
        .list_object_versions()
        .bucket("versions")
        .send()
        .await
        .unwrap();
    assert_eq!(version_page(&out).0, ["dir/planted", "stored"]);
    assert_eq!(out.name(), Some("versions"));
    assert_eq!(out.max_keys(), Some(1000));
    assert_eq!(out.versions().len(), objects.contents().len());
    for (version, object) in out.versions().iter().zip(objects.contents()) {
        let key = version.key().unwrap();
        assert_eq!(version.key(), object.key());
        assert_eq!(version.e_tag(), object.e_tag(), "{key}");
        assert_eq!(version.size(), object.size(), "{key}");
        assert_eq!(version.last_modified(), object.last_modified(), "{key}");
        assert_eq!(
            version.storage_class().map(|s| s.as_str()),
            Some("STANDARD")
        );
        let got = c
            .get_object()
            .bucket("versions")
            .key(key)
            .version_id(version.version_id().unwrap())
            .send()
            .await
            .unwrap();
        assert_eq!(got.e_tag(), version.e_tag(), "{key}");
    }
}

#[tokio::test]
async fn version_pages_resume_after_the_key_marker() {
    // Clients page with NextKeyMarker and NextVersionIdMarker together; a page that resumed
    // anywhere but after the last listed name would repeat or skip objects. Each page must
    // hold what ListObjectsV2 lists, CommonPrefixes of any delimiter included. A key marker
    // alone, or with the `null` version, resumes after the key.
    let server = TestServer::start().await;
    let keys = ["a/1", "a/2", "b", "c d", "e+f", "x-1", "x-2"];
    with_keys(&server, "version-pages", &keys).await;
    let c = &server.client;

    assert_eq!(
        version_pages(c, "version-pages", None, 2).await,
        [
            vec!["a/1", "a/2"],
            vec!["b", "c d"],
            vec!["e+f", "x-1"],
            vec!["x-2"]
        ]
    );
    for delimiter in [None, Some("/"), Some("-")] {
        let p = Params {
            delimiter,
            ..Params::default()
        };
        let objects = list_all(c, "version-pages", p).await.tagged();
        for max_keys in [1, 2, 3, 1000] {
            let versions = version_pages(c, "version-pages", delimiter, max_keys).await;
            assert_eq!(versions.concat(), objects, "{delimiter:?} by {max_keys}");
        }
    }

    let after = |key_marker: &str, version_id_marker: Option<&str>, delimiter: Option<&str>| {
        c.list_object_versions()
            .bucket("version-pages")
            .key_marker(key_marker)
            .set_version_id_marker(version_id_marker.map(str::to_owned))
            .set_delimiter(delimiter.map(str::to_owned))
            .send()
    };
    let rest = ["b", "c d", "e+f", "x-1", "x-2"];
    for version_id_marker in [None, Some("null")] {
        let out = after("a/2", version_id_marker, None).await.unwrap();
        assert_eq!(version_page(&out).0, rest, "{version_id_marker:?}");
        assert_eq!(out.key_marker(), Some("a/2"));
        // AWS echoes a marker that was not sent as an empty element.
        assert_eq!(
            out.version_id_marker(),
            Some(version_id_marker.unwrap_or_default())
        );
        // A marker at or inside a group skips the group, as in ListObjects.
        for key_marker in ["a/", "a/1"] {
            let out = after(key_marker, version_id_marker, Some("/"))
                .await
                .unwrap();
            assert_eq!(
                version_page(&out).0,
                rest,
                "{key_marker} {version_id_marker:?}"
            );
        }
    }

    let none = c
        .list_object_versions()
        .bucket("version-pages")
        .max_keys(0)
        .send()
        .await
        .unwrap();
    assert_eq!(version_page(&none), (vec![], None));
    assert_eq!(none.max_keys(), Some(0));
}

#[tokio::test]
async fn deleting_each_page_of_versions_empties_the_bucket() {
    // ceph s3-tests empty every bucket after each test this way: list a page of versions,
    // delete exactly those keys and version ids, and resume from both next markers. A page
    // that skipped a key would leave the bucket undeletable; a version id DeleteObjects did
    // not take would fail the teardown of every test.
    let server = TestServer::start().await;
    let keys = ["a", "b/c", "b/d", "e", "f g", "h+i", "j"];
    with_keys(&server, "nuke", &keys).await;
    let c = &server.client;
    let mut markers = None;
    loop {
        let (key_marker, version_id_marker) = markers.take().unzip();
        let out = c
            .list_object_versions()
            .bucket("nuke")
            .max_keys(3)
            .set_key_marker(key_marker)
            .set_version_id_marker(version_id_marker)
            .send()
            .await
            .unwrap();
        let objects: Vec<ObjectIdentifier> = out
            .versions()
            .iter()
            .map(|v| {
                ObjectIdentifier::builder()
                    .key(v.key().unwrap())
                    .version_id(v.version_id().unwrap())
                    .build()
                    .unwrap()
            })
            .collect();
        if !objects.is_empty() {
            let delete = Delete::builder()
                .set_objects(Some(objects))
                .quiet(true)
                .build()
                .unwrap();
            let deleted = c
                .delete_objects()
                .bucket("nuke")
                .delete(delete)
                .send()
                .await
                .unwrap();
            assert!(deleted.errors().is_empty(), "{:?}", deleted.errors());
        }
        let Some(next) = version_page(&out).1 else {
            break;
        };
        markers = Some(next);
    }
    assert!(
        list_all(c, "nuke", Params::default())
            .await
            .names()
            .is_empty()
    );
    c.delete_bucket().bucket("nuke").send().await.unwrap();
}

#[tokio::test]
async fn list_object_versions_url_encodes_every_name() {
    // botocore asks for url encoding on every ListObjectVersions request and decodes Key,
    // Prefix, KeyMarker, NextKeyMarker and Delimiter; s3-tests' cleanup sends the decoded
    // NextKeyMarker back as key-marker. A name left raw comes back changed. Unlike v1, Prefix
    // is encoded too, as AWS documents.
    let server = TestServer::start().await;
    with_keys(
        &server,
        "version-enc",
        &["dir ø/a+b", "dir ø/sub dir/z", "dir ø/x y"],
    )
    .await;
    let out = server
        .client
        .list_object_versions()
        .bucket("version-enc")
        .prefix("dir ø/")
        .delimiter("/")
        .key_marker("dir ø/a")
        .version_id_marker("null")
        .max_keys(2)
        .encoding_type(EncodingType::Url)
        .send()
        .await
        .unwrap();
    let (names, next) = version_page(&out);
    assert_eq!(names, ["dir%20%C3%B8/a%2Bb", "CP:dir%20%C3%B8/sub%20dir/"]);
    assert_eq!(
        next,
        Some(("dir%20%C3%B8/sub%20dir/".to_owned(), "null".to_owned()))
    );
    assert_eq!(out.prefix(), Some("dir%20%C3%B8/"));
    assert_eq!(out.key_marker(), Some("dir%20%C3%B8/a"));
    assert_eq!(out.version_id_marker(), Some("null"));
    assert_eq!(out.delimiter(), Some("/"));
    assert_eq!(out.encoding_type(), Some(&EncodingType::Url));
}

#[tokio::test]
async fn version_markers_s3dal_never_issues_are_invalid_arguments() {
    // A version id marker names a version of the key marker's key, and the only version here
    // is `null`. Resuming from a marker s3dal never issued would guess where the client meant
    // to be; AWS refuses a version id marker without a key marker, and one it cannot parse.
    let server = TestServer::start().await;
    with_keys(&server, "version-args", &["a"]).await;
    let c = &server.client;
    let requests = [
        c.list_object_versions()
            .bucket("version-args")
            .version_id_marker("null"),
        c.list_object_versions()
            .bucket("version-args")
            .key_marker("a")
            .version_id_marker("3HL4kqtJlcpXroDTDmJ+rmSpXd3dIbrHY"),
        c.list_object_versions().bucket("version-args").max_keys(-1),
        c.list_object_versions()
            .bucket("version-args")
            .encoding_type(EncodingType::from("base64")),
    ];
    for request in requests {
        let err = request.send().await.unwrap_err();
        assert_eq!(err.code(), Some("InvalidArgument"), "{err:?}");
    }
    let err = c
        .list_object_versions()
        .bucket("missing")
        .send()
        .await
        .unwrap_err();
    assert_eq!(err.code(), Some("NoSuchBucket"), "{err:?}");
}

/// xorshift64: deterministic without a dependency; each seed below is a test case.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        let mut x = self.0;
        x ^= x << 13;
        x ^= x >> 7;
        x ^= x << 17;
        self.0 = x;
        x
    }

    fn below(&mut self, n: usize) -> usize {
        (self.next() % n as u64) as usize
    }

    fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
        &items[self.below(items.len())]
    }

    /// A random leading part of `s`, possibly all or none of it.
    fn cut<'s>(&mut self, s: &'s str) -> &'s str {
        let ends: Vec<usize> = s.char_indices().map(|(i, _)| i).chain([s.len()]).collect();
        &s[..*self.pick(&ends)]
    }

    fn path(&mut self, max_depth: usize) -> String {
        let depth = 1 + self.below(max_depth);
        let segments: Vec<&str> = (0..depth).map(|_| *self.pick(NAMES)).collect();
        segments.join("/")
    }

    /// `len` characters of `s` in a row, or all of `s` if it is shorter. With `around`, a byte
    /// offset in `s`, the run covers the character there.
    fn run(&mut self, s: &str, len: usize, around: Option<usize>) -> String {
        let chars: Vec<(usize, char)> = s.char_indices().collect();
        let len = len.min(chars.len());
        let first = match around {
            Some(at) => {
                let at = chars.iter().position(|&(i, _)| i == at).unwrap();
                at.saturating_sub(self.below(len)).min(chars.len() - len)
            }
            None => self.below(chars.len() - len + 1),
        };
        chars[first..first + len].iter().map(|&(_, c)| c).collect()
    }
}

/// Names around the bytes that decide `/` ordering (`-` 0x2D, `.` 0x2E, `/` 0x2F, `0` 0x30),
/// multi-byte UTF-8, and characters that need escaping in URLs. Lowercase and free of
/// decomposable characters, because macOS file systems fold case and Unicode normalization.
const NAMES: &[&str] = &[
    "a", "a-", "a.b", "a0", "ab", "b", "-", ".x", "0", "ø", "中", "😀", "p q", "x+y", "%",
];

/// Keys on disk plus a few empty directories, which must never show up.
fn random_tree(rng: &mut Rng) -> (Vec<String>, Vec<String>) {
    let mut keys: BTreeSet<String> = (0..10 + rng.below(40)).map(|_| rng.path(4)).collect();
    // fs cannot hold `a` and `a/b` at once: drop every key that is a directory of another.
    let all = keys.clone();
    keys.retain(|k| !all.iter().any(|other| other.starts_with(&format!("{k}/"))));
    let keys: Vec<String> = keys.into_iter().collect();
    let dirs = (0..rng.below(5))
        .map(|_| rng.path(3))
        .filter(|dir| {
            !keys
                .iter()
                .any(|k| k == dir || dir.starts_with(&format!("{k}/")))
        })
        .collect();
    (keys, dirs)
}

/// The listing by definition: keys under the prefix, each folded into its CommonPrefix (the
/// prefix and the rest up to and including the delimiter's first occurrence after it), sorted
/// bytewise and deduplicated, everything up to and including the marker dropped.
/// CommonPrefixes as `CP:<prefix>`.
fn model(
    keys: &[String],
    prefix: &str,
    delimiter: Option<&str>,
    start_after: Option<&str>,
) -> Vec<String> {
    let names: BTreeSet<(String, bool)> = keys
        .iter()
        .filter_map(|k| {
            let rest = k.strip_prefix(prefix)?;
            let end = delimiter.and_then(|d| Some(rest.find(d)? + d.len()));
            Some(match end {
                Some(end) => (format!("{prefix}{}", &rest[..end]), true),
                None => (k.clone(), false),
            })
        })
        .collect();
    names
        .into_iter()
        .filter(|(name, _)| start_after.is_none_or(|marker| name.as_str() > marker))
        .map(|(name, folded)| if folded { format!("CP:{name}") } else { name })
        .collect()
}

#[tokio::test]
async fn paginated_listings_match_a_brute_force_model() {
    // The walk prunes, folds and stops early, and the ordered path seeks past groups; any
    // mistake there drops or repeats keys only for particular trees, delimiters and markers,
    // which hand-written cases miss. Delimiters are cut from the keys, so they occur: `/`, one
    // other character, and two or three holding `/`.
    let server = TestServer::start().await;
    let c = &server.client;
    for seed in [1_u64, 7, 42, 0x9E37_79B9_7F4A_7C15] {
        let mut rng = Rng(seed);
        let bucket = format!("rand-{seed:x}");
        server.create_bucket_dir(&bucket).await;
        let (keys, dirs) = random_tree(&mut rng);
        // Distinct sizes show that each listed entry carries its own object's metadata.
        let mut sizes = BTreeMap::new();
        for (i, key) in keys.iter().enumerate() {
            server.plant(&bucket, key, &vec![b'x'; i]).await;
            sizes.insert(key.clone(), i as i64);
        }
        for dir in &dirs {
            let dir = format!("{bucket}/{}/", dir.trim_end_matches('/'));
            server.op.create_dir(&dir).await.unwrap();
        }

        let some_key = rng.pick(&keys).clone();
        let deep_key = keys.iter().rfind(|k| k.contains('/')).unwrap_or(&some_key);
        let some_prefixes = [
            String::new(),
            rng.cut(&some_key).to_owned(),
            deep_key
                .rfind('/')
                .map_or_else(String::new, |i| deep_key[..=i].to_owned()),
        ];
        let another_key = rng.pick(&keys).clone();
        let markers = [
            None,
            Some(rng.cut(&another_key).to_owned()),
            Some(rng.pick(&keys).clone()),
        ];
        let slashed_len = 2 + rng.below(2);
        let delimiters = [
            None,
            Some("/".to_owned()),
            Some(rng.run(&some_key, 1, None)),
            Some(rng.run(deep_key, slashed_len, deep_key.find('/'))),
        ];
        for delimiter in &delimiters {
            // A prefix that ends with the delimiter, where the groups of a level start.
            let mut prefixes = some_prefixes.to_vec();
            if let Some(d) = delimiter
                && let Some(key) = keys.iter().find(|k| k.contains(d.as_str()))
            {
                prefixes.push(key[..key.find(d.as_str()).unwrap() + d.len()].to_owned());
            }
            for prefix in &prefixes {
                for marker in &markers {
                    let expected = model(&keys, prefix, delimiter.as_deref(), marker.as_deref());
                    for max_keys in [1, 3, 1000] {
                        let p = Params {
                            prefix: (!prefix.is_empty()).then_some(prefix.as_str()),
                            delimiter: delimiter.as_deref(),
                            start_after: marker.as_deref(),
                            max_keys: Some(max_keys),
                        };
                        let listing = list_all(c, &bucket, p).await;
                        assert_eq!(
                            listing.tagged(),
                            expected,
                            "seed {seed}, {p:?}, keys {keys:?}"
                        );
                        for (key, size) in &listing.sizes {
                            assert_eq!(Some(size), sizes.get(key), "seed {seed}, {key}");
                        }
                    }
                }
            }
        }
    }
}
