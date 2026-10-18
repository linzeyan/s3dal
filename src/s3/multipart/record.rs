//! What an upload keeps on the backend: `upload.json` from Create, and one manifest per
//! committed part. Both are JSON, because this is s3dal's own state and must outlive the process
//! and be shared by every instance on the backend.

use std::collections::{BTreeMap, HashMap};
use std::time::SystemTime;

use s3s::dto::Checksum;
use s3s::{S3Error, S3Result, s3_error};
use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use super::upload_dir;
use crate::S3Dal;
use crate::backend::without_stalling;
use crate::checksum::ALGORITHMS;
use crate::error::{Ctx, map};
use crate::meta::Attrs;

pub(super) const UPLOAD_FILE: &str = "upload.json";

#[derive(Serialize, Deserialize)]
pub(super) struct Upload {
    pub key: String,
    /// Applied to the object at Complete, validated as for PutObject at Create.
    #[serde(with = "AttrsDef")]
    pub attrs: Attrs,
    /// As named at Create; parts that name none are hashed with it.
    pub checksum_algorithm: Option<String>,
    /// `COMPOSITE` or `FULL_OBJECT`, set exactly when the algorithm is.
    pub checksum_type: Option<String>,
    pub initiated: SystemTime,
    /// Set in the same file once the object is committed, so the upload turns from active to
    /// completed in one atomic write and no request can see it as both.
    pub completed: Option<Completed>,
}

/// What a successful Complete answered. SDKs retry Complete when its response is lost, and AWS
/// answers the retry with the same result rather than NoSuchUpload, so it is kept after the part
/// data is gone.
#[derive(Serialize, Deserialize)]
pub(super) struct Completed {
    /// The multipart ETag computed from the part ETags. A retry matches when its part ETags give
    /// the same value: AWS compares the parts' ETags and nothing else.
    pub parts: String,
    /// The ETag the response named; on a backend with native ETags, the backend's.
    pub etag: String,
    /// The object's checksum, of the upload's algorithm and type, when it has one.
    pub checksum: Option<String>,
    pub size: u64,
    pub at: SystemTime,
}

/// Serde's mirror of [`Attrs`]. The derive builds `Attrs` with a struct literal, so a field
/// added there and missing here fails to compile instead of being dropped from uploads.
#[derive(Serialize, Deserialize)]
#[serde(remote = "Attrs")]
struct AttrsDef {
    content_type: Option<String>,
    cache_control: Option<String>,
    content_disposition: Option<String>,
    content_encoding: Option<String>,
    content_language: Option<String>,
    expires: Option<String>,
    user_metadata: HashMap<String, String>,
}

/// A committed part. Writing it is the commit point: ListParts and Complete read only manifests,
/// so a part whose data failed a check is never seen.
#[derive(Serialize, Deserialize)]
pub(super) struct PartManifest {
    /// The data file in the upload directory. Every upload of a part number writes a new one, so
    /// a Complete that read an older manifest still finds its data.
    pub data: String,
    pub size: u64,
    /// Hex; the part's ETag, and an input to the object's ETag.
    pub md5: String,
    /// Algorithm name to value, for every checksum computed over the part.
    pub checksums: BTreeMap<String, String>,
    pub last_modified: SystemTime,
}

pub(super) fn manifest_name(part: u32) -> String {
    format!("part-{part:05}.json")
}

/// The part number a manifest file name stands for. Only the exact form [`manifest_name`]
/// writes counts, so no other name can alias a part.
pub(super) fn manifest_part(name: &str) -> Option<u32> {
    let digits = name.strip_prefix("part-")?.strip_suffix(".json")?;
    if digits.len() != 5 || !digits.bytes().all(|b| b.is_ascii_digit()) {
        return None;
    }
    digits.parse().ok()
}

/// The upload, if it exists, was created for `key`, and is not completed. S3 answers an id used
/// with another key as an unknown upload, and a completed upload is gone for every operation
/// but a retried Complete.
pub(super) async fn load_upload(
    dal: &S3Dal,
    bucket: &str,
    id: Uuid,
    key: &str,
) -> S3Result<Upload> {
    match load_record(dal, bucket, id, key).await? {
        upload if upload.completed.is_none() => Ok(upload),
        _ => Err(no_such_upload()),
    }
}

/// The upload, active or completed, if it exists and was created for `key`; for Complete.
pub(super) async fn load_record(
    dal: &S3Dal,
    bucket: &str,
    id: Uuid,
    key: &str,
) -> S3Result<Upload> {
    match load_upload_at(dal, &upload_dir(bucket, id)).await? {
        Some(upload) if upload.key == key => Ok(upload),
        _ => Err(no_such_upload()),
    }
}

/// The upload in directory `dir`, `None` without an `upload.json`: such a directory belongs to
/// an upload being created or torn down.
pub(super) async fn load_upload_at(dal: &S3Dal, dir: &str) -> S3Result<Option<Upload>> {
    load(dal, &format!("{dir}{UPLOAD_FILE}")).await
}

pub(super) async fn store_upload(
    dal: &S3Dal,
    bucket: &str,
    id: Uuid,
    upload: &Upload,
) -> S3Result<()> {
    store(
        dal,
        &format!("{}{UPLOAD_FILE}", upload_dir(bucket, id)),
        upload,
    )
    .await
}

/// `None` when the part was never committed.
pub(super) async fn load_manifest(
    dal: &S3Dal,
    dir: &str,
    part: u32,
) -> S3Result<Option<PartManifest>> {
    load(dal, &format!("{dir}{}", manifest_name(part))).await
}

pub(super) async fn store_manifest(
    dal: &S3Dal,
    dir: &str,
    part: u32,
    manifest: &PartManifest,
) -> S3Result<()> {
    store(dal, &format!("{dir}{}", manifest_name(part)), manifest).await
}

async fn load<T: DeserializeOwned>(dal: &S3Dal, path: &str) -> S3Result<Option<T>> {
    let buf = match dal.op.read(path).await {
        Ok(buf) => buf,
        Err(err) if err.kind() == opendal::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(map(err, Ctx::Other)),
    };
    serde_json::from_slice(&buf.to_bytes())
        .map(Some)
        .map_err(|err| {
            tracing::error!(path, error = %err, "corrupt multipart upload state");
            S3Error::internal_error(err)
        })
}

/// A whole-file write; on fs it lands through `atomic_write_dir`, so a reader sees the old file
/// or the new one, never a partial one.
async fn store<T: Serialize>(dal: &S3Dal, path: &str, value: &T) -> S3Result<()> {
    let json = serde_json::to_vec(value).map_err(S3Error::internal_error)?;
    without_stalling(&dal.op, dal.op.write(path, json))
        .await
        .map(drop)
        .map_err(|err| map(err, Ctx::Other))
}

pub(super) fn no_such_upload() -> S3Error {
    s3_error!(
        NoSuchUpload,
        "The specified upload does not exist. The upload ID may be invalid, or the upload may have been aborted or completed."
    )
}

pub(super) fn checksum_map(mut checksum: Checksum) -> BTreeMap<String, String> {
    ALGORITHMS
        .iter()
        .filter_map(|(name, field, _)| Some(((*name).to_owned(), field(&mut checksum).take()?)))
        .collect()
}

pub(super) fn checksum_of(map: &BTreeMap<String, String>) -> Checksum {
    let mut checksum = Checksum::default();
    for (name, field, _) in ALGORITHMS {
        *field(&mut checksum) = map.get(name).cloned();
    }
    checksum
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_exact_manifest_names_are_parts() {
        // `+1` or `1` would parse as part 1 but name a different file than part 1's manifest.
        assert_eq!(manifest_part(&manifest_name(1)), Some(1));
        assert_eq!(manifest_part(&manifest_name(10_000)), Some(10_000));
        for other in [
            "part-1.json",
            "part-+0001.json",
            "part-00001-0b8a1f0e.json",
            "part-00001-0b8a1f0e",
            "upload.json",
        ] {
            assert_eq!(manifest_part(other), None, "{other}");
        }
    }

    #[test]
    fn checksums_survive_the_manifest() {
        // A checksum lost here would make Complete refuse a part the client hashed correctly.
        let checksum = Checksum {
            checksum_crc32: Some("DUoRhQ==".into()),
            checksum_sha256: Some("sha".into()),
            ..Default::default()
        };
        let map = checksum_map(checksum.clone());
        assert_eq!(map.len(), 2);
        assert!(checksum_of(&map) == checksum);
    }
}
