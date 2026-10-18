//! Object metadata as S3 sees it, resolved with one source of truth per field.
//!
//! The [`Profile`] decides per backend whether a field is native or stored by s3dal in
//! reserved user metadata. OpenDAL accepts user metadata only when a writer is opened, so a
//! computed value (MD5, checksum, part layout) can be stored only if it is known before the
//! write starts. An ETag that could not be stored is synthesized (decision "C1").

use std::borrow::Cow;
use std::collections::HashMap;
use std::fmt::Write;
use std::ops::Range;
use std::time::{SystemTime, UNIX_EPOCH};

use hyper::header::{HeaderName, HeaderValue};
use md5::{Digest, Md5};
use opendal::options::WriteOptions;
use s3s::dto::Checksum;
use s3s::{S3Error, S3ErrorCode, S3Result};

use crate::backend::Profile;
use crate::checksum::ALGORITHMS;

/// User-metadata keys with this prefix belong to s3dal. Clients may not set them, and
/// responses never show them.
pub const RESERVED_PREFIX: &str = "s3dal-";
pub const META_ETAG: &str = "s3dal-etag";
pub const META_CONTENT_TYPE: &str = "s3dal-content-type";
pub const META_CACHE_CONTROL: &str = "s3dal-cache-control";
pub const META_CONTENT_DISPOSITION: &str = "s3dal-content-disposition";
pub const META_CONTENT_ENCODING: &str = "s3dal-content-encoding";
pub const META_CONTENT_LANGUAGE: &str = "s3dal-content-language";
pub const META_EXPIRES: &str = "s3dal-expires";
pub const META_PARTS: &str = "s3dal-parts";
/// Followed by the lowercase algorithm name, for example `s3dal-checksum-crc32`.
pub const META_CHECKSUM_PREFIX: &str = "s3dal-checksum-";

/// What AWS S3 reports when an object was stored without a Content-Type.
pub const DEFAULT_CONTENT_TYPE: &str = "binary/octet-stream";

/// The headers S3 keeps with an object and returns on GET and HEAD, as the client set them.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Attrs {
    pub content_type: Option<String>,
    pub cache_control: Option<String>,
    pub content_disposition: Option<String>,
    pub content_encoding: Option<String>,
    pub content_language: Option<String>,
    /// Kept verbatim, as S3 does.
    pub expires: Option<String>,
    /// Client metadata only; never holds reserved keys.
    pub user_metadata: HashMap<String, String>,
}

impl Attrs {
    /// A form upload (PostObject) takes these from its fields, which may hold what no header can
    /// carry. Stored, such a value would fail every GET and HEAD of the object with a 500.
    pub fn check_header_values(&self) -> S3Result<()> {
        let values = [
            &self.content_type,
            &self.cache_control,
            &self.content_disposition,
            &self.content_encoding,
            &self.content_language,
            &self.expires,
        ];
        match values
            .into_iter()
            .flatten()
            .find(|v| HeaderValue::from_str(v).is_err())
        {
            Some(value) => Err(S3Error::with_message(
                S3ErrorCode::InvalidArgument,
                format!("{value:?} is not a valid header value"),
            )),
            None => Ok(()),
        }
    }
}

/// What s3dal computed about an object's content. Every value must be known before the write
/// starts: one learned later cannot be stored.
#[derive(Default)]
pub struct Computed {
    pub md5_hex: Option<String>,
    pub checksum: Checksum,
    pub parts: Option<PartLayout>,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ObjectMeta {
    /// Unquoted; quote with [`s3s::dto::ETag::Strong`] when building responses.
    pub etag: String,
    pub content_length: u64,
    pub last_modified: Option<SystemTime>,
    pub attrs: Attrs,
    /// Only the checksums that were stored with the object.
    pub checksum: Checksum,
    pub parts: Option<PartLayout>,
}

impl ObjectMeta {
    pub fn content_type(&self) -> &str {
        self.attrs
            .content_type
            .as_deref()
            .unwrap_or(DEFAULT_CONTENT_TYPE)
    }
}

/// How a multipart object was assembled, so `partNumber` can address one part.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct PartLayout {
    pub count: u32,
    /// The size of every part except the last, when they all have the same size. SDK transfer
    /// managers always upload that way; the boundaries of other layouts are not stored.
    pub part_size: Option<u64>,
}

impl PartLayout {
    /// `sizes` in part order; must not be empty.
    pub fn new(sizes: &[u64]) -> Self {
        let (first, rest) = sizes.split_first().expect("a multipart object has parts");
        let leading = &rest[..rest.len().saturating_sub(1)];
        Self {
            count: u32::try_from(sizes.len()).expect("at most 10000 parts"),
            part_size: leading.iter().all(|s| s == first).then_some(*first),
        }
    }

    /// The byte range of part `n` (1-based, at most `count`) in an object of `total` bytes.
    /// `None` when the boundaries are not stored, or when the stored layout no longer fits the
    /// object, for example after another tool rewrote the file.
    pub fn range(&self, n: u32, total: u64) -> Option<Range<u64>> {
        debug_assert!((1..=self.count).contains(&n));
        let size = self.part_size?;
        let last_start = u64::from(self.count - 1).checked_mul(size)?;
        if last_start > total {
            return None;
        }
        let start = u64::from(n - 1) * size;
        let end = if n == self.count { total } else { start + size };
        Some(start..end)
    }

    fn encode(&self) -> String {
        match self.part_size {
            Some(size) => format!("{}:{size}", self.count),
            None => self.count.to_string(),
        }
    }

    fn decode(s: &str) -> Option<Self> {
        let (count, part_size) = match s.split_once(':') {
            Some((count, size)) => (count, Some(size.parse().ok()?)),
            None => (s, None),
        };
        let count = count.parse().ok().filter(|&c| c > 0)?;
        Some(Self { count, part_size })
    }
}

pub fn resolve(profile: &Profile, m: &opendal::Metadata) -> ObjectMeta {
    let user = m.user_metadata();
    let encoded = profile.encoded_metadata_names;
    let reserved = |key: &str| {
        let user = user?;
        if encoded {
            user.get(&encode_name(key))
        } else {
            user.get(key)
        }
    };
    let name = |stored: &str| {
        if encoded {
            decode_name(stored).unwrap_or_else(|| stored.to_owned())
        } else {
            stored.to_owned()
        }
    };
    let pick = |native: bool, native_value: Option<&str>, key: &str| {
        if native { native_value } else { reserved(key) }.map(str::to_owned)
    };
    let last_modified = m.last_modified().map(SystemTime::from);

    let etag = if profile.native_etag {
        m.etag().map(unquote)
    } else {
        reserved(META_ETAG).map(str::to_owned)
    }
    .unwrap_or_else(|| synthesize_etag(m.content_length(), last_modified));

    let attrs = Attrs {
        content_type: pick(
            profile.native_content_type,
            m.content_type(),
            META_CONTENT_TYPE,
        ),
        cache_control: pick(
            profile.native_cache_control,
            m.cache_control(),
            META_CACHE_CONTROL,
        ),
        content_disposition: pick(
            profile.native_content_disposition,
            m.content_disposition(),
            META_CONTENT_DISPOSITION,
        ),
        content_encoding: pick(
            profile.native_content_encoding,
            m.content_encoding(),
            META_CONTENT_ENCODING,
        ),
        content_language: reserved(META_CONTENT_LANGUAGE).map(str::to_owned),
        expires: reserved(META_EXPIRES).map(str::to_owned),
        user_metadata: user
            .into_iter()
            .flatten()
            .map(|(k, v)| (name(k), v))
            .filter(|(k, _)| !is_reserved(k))
            .map(|(k, v)| (k, decode_user_value(v)))
            .collect(),
    };

    let mut checksum = Checksum::default();
    for (name, field, _) in ALGORITHMS {
        *field(&mut checksum) = reserved(&checksum_key(name)).map(str::to_owned);
    }

    ObjectMeta {
        etag,
        content_length: m.content_length(),
        last_modified,
        attrs,
        checksum,
        parts: reserved(META_PARTS).and_then(PartLayout::decode),
    }
}

/// Options for writing an object with `attrs`. See [`Computed`] for what may be passed.
pub fn write_options(profile: &Profile, attrs: Attrs, computed: Computed) -> WriteOptions {
    let mut user: HashMap<String, String> = attrs
        .user_metadata
        .into_iter()
        .map(|(k, v)| (k, encode_user_value(v)))
        .collect();
    let mut opts = WriteOptions::default();
    // A backend that stores Content-Type fills in its own default for a write without one
    // (RustFS and MinIO use application/octet-stream), which then reads back as if a client had
    // sent it. Writing AWS's default keeps what S3 reports for such an object the same on every
    // backend; on the others `resolve` supplies it.
    let content_type = match attrs.content_type {
        None if profile.native_content_type => Some(DEFAULT_CONTENT_TYPE.to_owned()),
        content_type => content_type,
    };
    for (native, value, slot, key) in [
        (
            profile.native_content_type,
            content_type,
            &mut opts.content_type,
            META_CONTENT_TYPE,
        ),
        (
            profile.native_cache_control,
            attrs.cache_control,
            &mut opts.cache_control,
            META_CACHE_CONTROL,
        ),
        (
            profile.native_content_disposition,
            attrs.content_disposition,
            &mut opts.content_disposition,
            META_CONTENT_DISPOSITION,
        ),
        (
            profile.native_content_encoding,
            attrs.content_encoding,
            &mut opts.content_encoding,
            META_CONTENT_ENCODING,
        ),
    ] {
        match value {
            Some(v) if native => *slot = Some(v),
            Some(v) => {
                user.insert(key.to_owned(), v);
            }
            None => {}
        }
    }
    if let Some(language) = attrs.content_language {
        user.insert(META_CONTENT_LANGUAGE.to_owned(), language);
    }
    if let Some(expires) = attrs.expires {
        user.insert(META_EXPIRES.to_owned(), expires);
    }

    if !profile.native_etag
        && let Some(md5) = computed.md5_hex
    {
        user.insert(META_ETAG.to_owned(), md5);
    }
    let mut checksum = computed.checksum;
    for (name, field, _) in ALGORITHMS {
        if let Some(value) = field(&mut checksum).take() {
            user.insert(checksum_key(name), value);
        }
    }
    if let Some(parts) = computed.parts {
        user.insert(META_PARTS.to_owned(), parts.encode());
    }

    if profile.encoded_metadata_names {
        user = user
            .into_iter()
            .map(|(k, v)| (encode_name(&k), v))
            .collect();
    }
    opts.user_metadata = Some(user);
    opts
}

/// A metadata name in a form any backend keeps, for one that refuses or alters names S3
/// allows (`Profile::encoded_metadata_names`). Azure takes only C# identifiers, and returns
/// names lowercased, as HTTP headers. ASCII lowercase letters and digits stand for themselves,
/// except a leading digit; every other byte becomes `_` and two lowercase hex digits, so
/// `s3dal-etag` is stored as `s3dal_2detag`.
pub fn encode_name(name: &str) -> String {
    let mut encoded = String::with_capacity(name.len());
    for (i, b) in name.bytes().enumerate() {
        if b.is_ascii_lowercase() || (b.is_ascii_digit() && i > 0) {
            encoded.push(char::from(b));
        } else {
            let _ = write!(encoded, "_{b:02x}");
        }
    }
    encoded
}

/// The name [`encode_name`] stored as `name`. `None` for a name it cannot have produced, such
/// as one another tool stored, which is then shown as it is.
fn decode_name(name: &str) -> Option<String> {
    let mut bytes = Vec::with_capacity(name.len());
    let mut rest = name;
    while let Some(c) = rest.chars().next() {
        if c == '_' {
            bytes.push(u8::from_str_radix(rest.get(1..3)?, 16).ok()?);
            rest = &rest[3..];
        } else {
            bytes.push(u8::try_from(c).ok()?);
            rest = &rest[c.len_utf8()..];
        }
    }
    let decoded = String::from_utf8(bytes).ok()?;
    // Only the canonical form, so no two stored names decode to the same one.
    (encode_name(&decoded) == name).then_some(decoded)
}

/// A client's metadata value in the form S3 carries it in a header: RFC 2047 when it is not
/// printable ASCII. S3 upstreams take user metadata as request headers, which cannot hold
/// anything else, so the raw value would fail every write there; storing one form everywhere
/// keeps backends alike. Only an input over 1 MiB fails to encode, and no header carries that.
///
/// A value kept as it is loses leading and trailing spaces, which a header cannot carry
/// either: S3 servers trim them, and Azure, which also trims, then refuses the write as
/// wrongly signed because OpenDAL signed the untrimmed value.
fn encode_user_value(value: String) -> String {
    match s3s_rfc2047::encode(&value) {
        Ok(Cow::Owned(encoded)) => encoded,
        Ok(Cow::Borrowed(raw)) => raw.trim_matches(' ').to_owned(),
        Err(_) => value,
    }
}

/// The client's value back from [`encode_user_value`]. A value another tool stored, raw or
/// malformed, is returned as it is.
fn decode_user_value(value: &str) -> String {
    s3s_rfc2047::decode(value).map_or_else(|_| value.to_owned(), Cow::into_owned)
}

fn checksum_key(algorithm: &str) -> String {
    format!("{META_CHECKSUM_PREFIX}{}", algorithm.to_ascii_lowercase())
}

/// Rejects client metadata in the reserved namespace, where a client could forge the fields
/// that s3dal reports for an object, and metadata larger than S3 takes: 2 KB, counted as the
/// UTF-8 bytes of every name and value. An S3 upstream would refuse it too, after the body.
///
/// A name from a header is a header name already, but a form upload (PostObject) may call any
/// field `x-amz-meta-*`. One no header can carry would be stored, and then fail every GET and
/// HEAD of the object with a 500.
pub fn check_user_metadata(user_metadata: &HashMap<String, String>) -> S3Result<()> {
    if let Some(key) = user_metadata.keys().find(|k| is_reserved(k)) {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidArgument,
            format!("metadata key x-amz-meta-{key} is reserved by s3dal"),
        ));
    }
    if let Some(key) = user_metadata
        .keys()
        .find(|k| HeaderName::from_bytes(format!("x-amz-meta-{k}").as_bytes()).is_err())
    {
        return Err(S3Error::with_message(
            S3ErrorCode::InvalidArgument,
            format!("metadata key x-amz-meta-{key:?} is not a valid header name"),
        ));
    }
    let size: usize = user_metadata.iter().map(|(k, v)| k.len() + v.len()).sum();
    if size > USER_METADATA_LIMIT {
        return Err(S3Error::with_message(
            S3ErrorCode::MetadataTooLarge,
            "Your metadata headers exceed the maximum allowed metadata size.",
        ));
    }
    Ok(())
}

const USER_METADATA_LIMIT: usize = 2048;

fn is_reserved(key: &str) -> bool {
    key.len() >= RESERVED_PREFIX.len()
        && key[..RESERVED_PREFIX.len()].eq_ignore_ascii_case(RESERVED_PREFIX)
}

pub fn md5_hex(data: &[u8]) -> String {
    hex(&Md5::digest(data))
}

pub fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Stands in for a content hash when none could be stored. It always contains `-`, because
/// clients such as rclone read a 32-hex ETag as the content MD5 and would report corruption.
/// It changes whenever size or mtime changes, which is what a validator needs.
pub fn synthesize_etag(content_length: u64, last_modified: Option<SystemTime>) -> String {
    let mtime_ns = last_modified
        .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
        .map_or(0, |d| d.as_nanos());
    format!(
        "{}-0",
        md5_hex(format!("{content_length}:{mtime_ns}").as_bytes())
    )
}

/// OpenDAL returns ETags as the service sent them, quotes and weak prefix included.
pub(crate) fn unquote(etag: &str) -> String {
    let etag = etag.strip_prefix("W/").unwrap_or(etag);
    etag.trim_matches('"').to_owned()
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use opendal::raw::Timestamp;
    use opendal::{Metadata, MetadataBuilder};

    use super::*;

    const GATEWAY: Profile = Profile {
        native_etag: false,
        native_content_type: false,
        native_cache_control: false,
        native_content_disposition: false,
        native_content_encoding: false,
        encoded_metadata_names: false,
        ordered_list: false,
        real_dirs: true,
        conditional_writes: false,
    };
    const NATIVE: Profile = Profile {
        native_etag: true,
        native_content_type: true,
        native_cache_control: true,
        native_content_disposition: true,
        native_content_encoding: true,
        encoded_metadata_names: false,
        ordered_list: true,
        real_dirs: false,
        conditional_writes: true,
    };
    /// Every field in reserved metadata, under encoded names.
    const ENCODED: Profile = Profile {
        encoded_metadata_names: true,
        ..GATEWAY
    };

    /// Names S3 allows in x-amz-meta-*, with what some backend refuses or alters in names.
    const AWKWARD_NAMES: [&str; 7] = [
        "with-dash",
        "with_underscore",
        "0digit-first",
        "UPPER-and-lower",
        "token!#$%&'*+.^`|~chars",
        "_",
        "s3dal",
    ];

    /// Whether Azure takes `name`: a C# identifier, ASCII here, that survives the lowercasing
    /// of HTTP header names.
    fn azure_keeps(name: &str) -> bool {
        let mut bytes = name.bytes();
        bytes
            .next()
            .is_some_and(|b| b.is_ascii_lowercase() || b == b'_')
            && bytes.all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'_')
    }

    fn file(
        len: u64,
        etag: Option<&str>,
        content_type: Option<&str>,
        user: &[(&str, &str)],
    ) -> Metadata {
        let mut b = MetadataBuilder::file(len);
        b.last_modified(Timestamp::from_second(1_700_000_000).unwrap());
        if let Some(e) = etag {
            b.etag(e);
        }
        if let Some(ct) = content_type {
            b.content_type(ct);
        }
        b.user_metadata(user.iter().map(|(k, v)| ((*k).to_owned(), (*v).to_owned())));
        b.build()
    }

    /// What a backend that keeps only user metadata (fs) returns for a write with `opts`.
    fn stored(len: u64, opts: WriteOptions) -> Metadata {
        let mut b = MetadataBuilder::file(len);
        b.user_metadata(opts.user_metadata.unwrap());
        b.build()
    }

    fn all_attrs() -> Attrs {
        Attrs {
            content_type: Some("text/plain".into()),
            cache_control: Some("max-age=60".into()),
            content_disposition: Some("attachment; filename=\"a.txt\"".into()),
            content_encoding: Some("gzip".into()),
            content_language: Some("zh-TW".into()),
            expires: Some("Wed, 21 Oct 2015 07:28:00 GMT".into()),
            user_metadata: HashMap::from([("color".to_owned(), "red".to_owned())]),
        }
    }

    #[test]
    fn gateway_profile_reads_reserved_fields_and_hides_them() {
        let m = file(
            5,
            Some("\"native-ignored\""),
            Some("text/ignored"),
            &[
                (META_ETAG, "abc"),
                (META_CONTENT_TYPE, "text/plain"),
                ("color", "red"),
            ],
        );
        let meta = resolve(&GATEWAY, &m);
        assert_eq!(meta.etag, "abc");
        assert_eq!(meta.content_type(), "text/plain");
        // Clients must never see s3dal's bookkeeping as if it were their own metadata.
        assert_eq!(
            meta.attrs.user_metadata,
            HashMap::from([("color".to_owned(), "red".to_owned())])
        );
    }

    #[test]
    fn native_profile_ignores_reserved_fields() {
        // One source of truth: a stale reserved key, for example left by a gateway-mode copy,
        // must not override what the backend reports.
        let m = file(
            5,
            Some("W/\"xyz\""),
            Some("image/png"),
            &[(META_ETAG, "stale"), (META_CACHE_CONTROL, "stale")],
        );
        let meta = resolve(&NATIVE, &m);
        assert_eq!(meta.etag, "xyz");
        assert_eq!(meta.content_type(), "image/png");
        assert_eq!(meta.attrs.cache_control, None);
    }

    #[test]
    fn user_metadata_is_stored_header_safe_and_read_back_as_sent() {
        // S3 upstreams take user metadata as request headers, and a raw non-ASCII value failed
        // every write there.
        let attrs = Attrs {
            user_metadata: HashMap::from([("greeting".to_owned(), "你好，世界".to_owned())]),
            ..Attrs::default()
        };
        let opts = write_options(&GATEWAY, attrs.clone(), Computed::default());
        let user = opts.user_metadata.clone().unwrap();
        assert!(
            user["greeting"].bytes().all(|b| (0x20..0x7f).contains(&b)),
            "{user:?}"
        );
        let meta = resolve(&GATEWAY, &stored(1, opts));
        assert_eq!(meta.attrs.user_metadata, attrs.user_metadata);
    }

    #[test]
    fn edge_spaces_of_a_plain_value_are_not_stored() {
        // A header cannot carry them, and Azure refused the write as wrongly signed. A value
        // stored encoded keeps them, since a client gets that one back encoded.
        let attrs = Attrs {
            user_metadata: HashMap::from([
                ("plain".to_owned(), "  a  b ".to_owned()),
                ("encoded".to_owned(), " ä ".to_owned()),
            ]),
            ..Attrs::default()
        };
        let opts = write_options(&GATEWAY, attrs, Computed::default());
        assert_eq!(opts.user_metadata.as_ref().unwrap()["plain"], "a  b");
        let meta = resolve(&GATEWAY, &stored(1, opts));
        assert_eq!(meta.attrs.user_metadata["encoded"], " ä ");
    }

    #[test]
    fn every_metadata_name_is_stored_as_one_azure_keeps_and_read_back_as_sent() {
        // Azure refuses `-` in names, and so every reserved key; a name that comes back
        // different would lose the client's metadata or s3dal's own fields.
        let attrs = Attrs {
            user_metadata: AWKWARD_NAMES
                .iter()
                .map(|n| ((*n).to_owned(), "v".to_owned()))
                .collect(),
            ..all_attrs()
        };
        let checksum = Checksum {
            checksum_crc32: Some("DUoRhQ==".into()),
            ..Default::default()
        };
        let computed = Computed {
            md5_hex: Some("abc".into()),
            checksum: checksum.clone(),
            parts: Some(PartLayout::new(&[5, 5, 2])),
        };
        let opts = write_options(&ENCODED, attrs.clone(), computed);
        let user = opts.user_metadata.clone().unwrap();
        for name in user.keys() {
            assert!(azure_keeps(name), "{name:?} would be refused or altered");
        }
        assert!(user.contains_key(&encode_name(META_ETAG)), "{user:?}");

        let meta = resolve(&ENCODED, &stored(12, opts));
        assert_eq!(meta.attrs, attrs);
        assert_eq!(meta.etag, "abc");
        assert!(meta.checksum == checksum);
        assert_eq!(meta.parts, Some(PartLayout::new(&[5, 5, 2])));
    }

    #[test]
    fn encoded_names_are_one_to_one() {
        // Two names stored under one would overwrite each other.
        let names = AWKWARD_NAMES.map(encode_name);
        let distinct: std::collections::HashSet<_> = names.iter().collect();
        assert_eq!(distinct.len(), names.len(), "{names:?}");
        for name in AWKWARD_NAMES.into_iter().chain(["", "日本", "a_2d"]) {
            assert_eq!(decode_name(&encode_name(name)).as_deref(), Some(name));
        }
    }

    #[test]
    fn names_another_tool_stored_are_shown_as_they_are() {
        // Only what `encode_name` produces is decoded; anything else would be misread or lost.
        for foreign in ["Color", "my_name", "a_2D", "a_2", "_zz", "0abc", "a_c3"] {
            assert_eq!(decode_name(foreign), None, "{foreign}");
        }
        let m = file(1, None, None, &[("my_name", "x")]);
        let meta = resolve(&ENCODED, &m);
        assert_eq!(meta.attrs.user_metadata["my_name"], "x");
    }

    #[test]
    fn objects_without_stored_fields_get_synthesized_etag_and_default_type() {
        // Files placed into an fs root by other tools have no reserved fields.
        let m = file(5, None, None, &[]);
        let meta = resolve(&GATEWAY, &m);
        assert!(meta.etag.ends_with("-0"), "{}", meta.etag);
        assert_eq!(meta.content_type(), DEFAULT_CONTENT_TYPE);
        assert_eq!(meta.attrs, Attrs::default());
        assert_eq!(meta.parts, None);
    }

    #[test]
    fn synthesized_etag_never_looks_like_an_md5() {
        let etag = synthesize_etag(5, Some(UNIX_EPOCH + Duration::from_secs(1)));
        assert!(etag.contains('-'));
        assert_ne!(etag.len(), 32);
    }

    #[test]
    fn synthesized_etag_tracks_size_and_mtime() {
        let t = UNIX_EPOCH + Duration::from_secs(1);
        assert_eq!(synthesize_etag(5, Some(t)), synthesize_etag(5, Some(t)));
        assert_ne!(synthesize_etag(5, Some(t)), synthesize_etag(6, Some(t)));
        assert_ne!(
            synthesize_etag(5, Some(t)),
            synthesize_etag(5, Some(t + Duration::from_nanos(1)))
        );
    }

    #[test]
    fn everything_written_on_the_gateway_profile_reads_back() {
        // Each field takes the reserved path on fs; a field that is written but not resolved
        // (or the reverse) would silently vanish from GET and HEAD.
        let checksum = Checksum {
            checksum_crc32: Some("DUoRhQ==".into()),
            checksum_sha256: Some("sha".into()),
            ..Default::default()
        };
        let parts = PartLayout {
            count: 3,
            part_size: Some(5),
        };
        let opts = write_options(
            &GATEWAY,
            all_attrs(),
            Computed {
                md5_hex: Some("abc".into()),
                checksum: checksum.clone(),
                parts: Some(parts),
            },
        );
        assert_eq!(opts.content_type, None);
        assert_eq!(opts.cache_control, None);

        let meta = resolve(&GATEWAY, &stored(12, opts));
        assert_eq!(meta.etag, "abc");
        assert_eq!(meta.attrs, all_attrs());
        assert!(meta.checksum == checksum);
        assert_eq!(meta.parts, Some(parts));
    }

    #[test]
    fn native_profile_writes_native_fields_and_keeps_the_rest_reserved() {
        let opts = write_options(
            &NATIVE,
            all_attrs(),
            Computed {
                md5_hex: Some("abc".into()),
                ..Default::default()
            },
        );
        assert_eq!(opts.content_type.as_deref(), Some("text/plain"));
        assert_eq!(opts.cache_control.as_deref(), Some("max-age=60"));
        assert_eq!(opts.content_encoding.as_deref(), Some("gzip"));
        let user = opts.user_metadata.unwrap();
        // OpenDAL has no field for these, so even a native backend keeps them reserved.
        assert!(user.contains_key(META_CONTENT_LANGUAGE));
        assert!(user.contains_key(META_EXPIRES));
        // The backend's own ETag is authoritative; storing another would be a second source.
        assert!(!user.contains_key(META_ETAG));
        assert!(!user.contains_key(META_CONTENT_TYPE));
    }

    #[test]
    fn a_missing_content_type_is_written_as_aws_default_only_where_native() {
        // A native backend would store its own default, which reads back as the client's.
        let native = write_options(&NATIVE, Attrs::default(), Computed::default());
        assert_eq!(native.content_type.as_deref(), Some(DEFAULT_CONTENT_TYPE));
        // On the gateway path `resolve` supplies the default, so nothing is stored for it.
        let gateway = write_options(&GATEWAY, Attrs::default(), Computed::default());
        assert_eq!(gateway.content_type, None);
        assert!(
            !gateway
                .user_metadata
                .unwrap()
                .contains_key(META_CONTENT_TYPE)
        );
    }

    #[test]
    fn reserved_metadata_from_clients_is_rejected() {
        let forged = HashMap::from([("S3DAL-etag".to_owned(), "x".to_owned())]);
        let err = check_user_metadata(&forged).unwrap_err();
        assert_eq!(*err.code(), S3ErrorCode::InvalidArgument);
        assert!(
            check_user_metadata(&HashMap::from([("s3dal".to_owned(), "x".to_owned())])).is_ok()
        );
    }

    #[test]
    fn md5_matches_known_vector() {
        assert_eq!(md5_hex(b""), "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn uniform_parts_map_to_byte_ranges() {
        // An SDK downloading by partNumber must get exactly the bytes it uploaded as that part.
        let layout = PartLayout::new(&[5, 5, 2]);
        assert_eq!(layout.part_size, Some(5));
        assert_eq!(layout.range(1, 12), Some(0..5));
        assert_eq!(layout.range(2, 12), Some(5..10));
        assert_eq!(layout.range(3, 12), Some(10..12));
        assert_eq!(PartLayout::new(&[7]).range(1, 7), Some(0..7));
    }

    #[test]
    fn unknown_or_stale_boundaries_give_no_range() {
        // Serving a guessed range would hand the client the wrong bytes for a part.
        let irregular = PartLayout::new(&[5, 6, 2]);
        assert_eq!(irregular.part_size, None);
        assert_eq!(irregular.range(2, 13), None);
        let stale = PartLayout {
            count: 3,
            part_size: Some(5),
        };
        assert_eq!(stale.range(1, 9), None);
    }

    #[test]
    fn part_layout_survives_encoding() {
        for layout in [PartLayout::new(&[5, 5, 2]), PartLayout::new(&[5, 6, 2])] {
            assert_eq!(PartLayout::decode(&layout.encode()), Some(layout));
        }
        for garbage in ["", "0", "x", "3:", "3:x"] {
            assert_eq!(PartLayout::decode(garbage), None, "{garbage}");
        }
    }
}
