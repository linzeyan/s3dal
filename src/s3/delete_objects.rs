//! DeleteObjects: each key is deleted exactly as DeleteObject deletes it, and failures are
//! reported per key instead of failing the request.

use std::collections::BTreeMap;

use futures::StreamExt;
use s3s::dto::*;
use s3s::{S3Error, S3ErrorCode, S3Request, S3Response, S3Result, s3_error};

use super::store::{self, DeleteCondition};
use super::tier::Removals;
use super::version::{self, Versioning};
use super::{S3Dal, bucket_state, naming};
use crate::events::{EventKind, ObjectEvent, Origin, Usage};

/// S3's limit per request.
const MAX_KEYS: usize = 1000;
/// Keys deleted at once. Each delete is a few backend calls, so a full request finishes in
/// a fraction of the time a sequential one would, without flooding the backend.
const CONCURRENCY: usize = 16;

pub(super) async fn delete_objects(
    dal: &S3Dal,
    req: S3Request<DeleteObjectsInput>,
) -> S3Result<S3Response<DeleteObjectsOutput>> {
    let origin = Origin::of(&req);
    let input = req.input;
    naming::require_bucket(dal, &input.bucket).await?;
    let objects = input.delete.objects;
    // AWS rejects an empty or oversized list as not matching its schema.
    if !(1..=MAX_KEYS).contains(&objects.len()) {
        return Err(s3_error!(
            MalformedXML,
            "The XML you provided was not well-formed or did not validate against our published schema"
        ));
    }
    // The response names every key, and s3s answers 500 to one XML cannot carry. Found after the
    // deletes, that would report the whole request as failed with the other keys already gone,
    // and a retry would do the same again. No such key can exist, so nothing is lost.
    let xml_safe = |o: &ObjectIdentifier| {
        naming::xml_safe(&o.key) && o.version_id.as_deref().is_none_or(naming::xml_safe)
    };
    if !objects.iter().all(xml_safe) {
        return Err(s3_error!(
            InvalidArgument,
            "object keys and version ids with control characters other than tab, line feed and carriage return are not supported"
        ));
    }
    let quiet = input.delete.quiet.unwrap_or(false);

    let bucket = input.bucket.as_str();
    let state = version::state(dal, bucket).await?;
    let tracked = bucket_state::tracked(dal, bucket).await?;
    let origin = &origin;
    // The tier objects of the cold objects removed, deleted in one batch per tier at the end.
    let removals = &Removals::default();
    // `buffered` keeps request order, so the response lists keys in the order they were sent.
    let results: Vec<_> = futures::stream::iter(objects)
        .map(|object| async move {
            let result = delete_one(dal, bucket, &object, state, tracked, removals).await;
            if let Ok(Outcome {
                change: Some((kind, removed)),
                version,
                ..
            }) = &result
            {
                dal.emit(ObjectEvent {
                    version: version.clone(),
                    usage: tracked.then(|| Usage::default().minus(*removed)),
                    ..ObjectEvent::new(*kind, bucket, &object.key, origin.clone())
                });
            }
            (object, result)
        })
        .buffered(CONCURRENCY)
        .collect()
        .await;
    removals.run(dal).await;

    // A batch usually empties a few directories of many keys. Pruning after each key cost two
    // backend calls per key, nearly all of them finding the directory still full, and the
    // concurrent deletes raced to remove the same directory; once per directory does the same.
    let mut emptied: BTreeMap<&str, &str> = BTreeMap::new();
    for (object, result) in &results {
        if let Ok(Outcome { pruned: true, .. }) = result
            && let Some((dir, _)) = object.key.trim_end_matches('/').rsplit_once('/')
        {
            emptied.entry(dir).or_insert(&object.key);
        }
    }
    futures::stream::iter(emptied.into_values())
        .for_each_concurrent(CONCURRENCY, |key| {
            store::prune_empty_parents(dal, bucket, key)
        })
        .await;

    let mut deleted = Vec::new();
    let mut errors = Vec::new();
    for (object, result) in results {
        match result {
            Ok(outcome) => deleted.push(DeletedObject {
                key: Some(object.key),
                version_id: object.version_id,
                delete_marker: outcome.delete_marker.then_some(true),
                delete_marker_version_id: outcome.marker_version_id,
            }),
            Err(err) => errors.push(Error {
                code: Some(err.code().as_str().to_owned()),
                message: Some(message(&err)),
                key: Some(object.key),
                version_id: object.version_id,
            }),
        }
    }

    Ok(S3Response::new(DeleteObjectsOutput {
        // Quiet mode reports only the keys that failed.
        deleted: (!quiet).then_some(deleted),
        errors: Some(errors),
        ..Default::default()
    }))
}

/// What deleting one entry did.
struct Outcome {
    /// Something was deleted that may have emptied directories, which the caller prunes once
    /// per directory. A versioned delete prunes its own.
    pruned: bool,
    /// A delete marker was written, or the version removed was one.
    delete_marker: bool,
    /// That marker's version id.
    marker_version_id: Option<String>,
    /// What changed, as an event names it, and what left the bucket's usage; `None` when
    /// nothing did.
    change: Option<(EventKind, Usage)>,
    /// The version removed or the marker written, for the event.
    version: Option<String>,
}

async fn delete_one(
    dal: &S3Dal,
    bucket: &str,
    object: &ObjectIdentifier,
    state: Versioning,
    tracked: bool,
    removals: &Removals,
) -> S3Result<Outcome> {
    // Only a delete that succeeded hands its tier objects to the batch: one that failed part
    // way may have left the stub that names them.
    let mine = Removals::default();
    let outcome = delete_noting(dal, bucket, object, state, tracked, &mine).await?;
    removals.absorb(mine);
    Ok(outcome)
}

async fn delete_noting(
    dal: &S3Dal,
    bucket: &str,
    object: &ObjectIdentifier,
    state: Versioning,
    tracked: bool,
    removals: &Removals,
) -> S3Result<Outcome> {
    // AWS documents `*` here as matching any object, as in an If-Match header. s3s reads the
    // element as an ETag, quoted or not, so it arrives as the strong ETag `*`.
    let if_match = object.e_tag.clone().map(|tag| match tag {
        ETag::Strong(value) if value == "*" => ETagCondition::Any,
        tag => ETagCondition::ETag(tag),
    });
    let cond = DeleteCondition {
        if_match: if_match.as_ref(),
        last_modified: object.last_modified_time.as_ref(),
        size: object.size,
    };
    let version_id = object.version_id.as_deref();
    if state == Versioning::Unversioned {
        // Deleting a version other than the one that exists must not delete the object.
        naming::require_null_version(version_id)?;
        let removed = store::delete_unpruned(dal, bucket, &object.key, cond, removals).await?;
        return Ok(Outcome {
            pruned: removed.is_some(),
            delete_marker: false,
            marker_version_id: None,
            change: removed.map(|r| (EventKind::Delete, r)),
            version: None,
        });
    }
    let deleted = version::delete(
        dal,
        bucket,
        &object.key,
        version_id,
        cond,
        state,
        tracked,
        removals,
    )
    .await?;
    Ok(Outcome {
        pruned: false,
        delete_marker: deleted.delete_marker,
        marker_version_id: deleted.version_id.clone().filter(|_| deleted.delete_marker),
        change: deleted.change,
        version: deleted.version_id,
    })
}

/// S3 always sends a message with each per-key error; s3dal's errors carry one only where the
/// code alone would not explain the failure, so the common codes get S3's wording.
fn message(err: &S3Error) -> String {
    if let Some(message) = err.message() {
        return message.to_owned();
    }
    let code = err.code();
    match code {
        S3ErrorCode::NoSuchKey => "The specified key does not exist.",
        S3ErrorCode::KeyTooLongError => "Your key is too long.",
        S3ErrorCode::AccessDenied => "Access Denied",
        S3ErrorCode::SlowDown => "Please reduce your request rate.",
        S3ErrorCode::NotImplemented => {
            "A header you provided implies functionality that is not implemented."
        }
        S3ErrorCode::InternalError => "We encountered an internal error. Please try again.",
        _ => code.as_str(),
    }
    .to_owned()
}
