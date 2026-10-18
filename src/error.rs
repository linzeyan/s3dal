use std::io;

use opendal::ErrorKind;
use s3s::{S3Error, S3ErrorCode};

/// What an OpenDAL error refers to. The same `NotFound` means `NoSuchBucket` or `NoSuchKey`
/// depending on what the operation was looking up.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Ctx {
    Bucket,
    ObjectRead,
    ObjectWrite,
    Other,
}

pub(crate) fn map(err: opendal::Error, ctx: Ctx) -> S3Error {
    // A key with a segment longer than the filesystem takes in a name, or whose path is longer
    // than the system takes in a path (1024 bytes on macOS, the root's included), names no
    // object and cannot be stored. Both are ENAMETOOLONG, which fs reports as `Unexpected` with
    // the io error as its source. S3 limits only the whole key, to 1024 bytes.
    if io_kind(&err) == Some(io::ErrorKind::InvalidFilename) {
        match ctx {
            Ctx::ObjectRead => return S3Error::new(S3ErrorCode::NoSuchKey),
            Ctx::ObjectWrite => {
                return S3Error::with_message(
                    S3ErrorCode::KeyTooLongError,
                    "a segment of the key, or the key with the backend's root, is longer than \
                     this backend takes in a file name or path",
                );
            }
            Ctx::Bucket | Ctx::Other => {}
        }
    }
    let code = match (kind(&err), ctx) {
        (ErrorKind::NotFound, Ctx::Bucket) => S3ErrorCode::NoSuchBucket,
        // Reading `a/b` when `a` is an object fails with NotADirectory on fs; the key is absent.
        (
            ErrorKind::NotFound | ErrorKind::IsADirectory | ErrorKind::NotADirectory,
            Ctx::ObjectRead,
        ) => S3ErrorCode::NoSuchKey,
        // On fs, writing `a/b` over an object `a` fails while creating the directory `a`, which
        // reports EEXIST; no other object write can see AlreadyExists without a condition.
        (
            ErrorKind::IsADirectory | ErrorKind::NotADirectory | ErrorKind::AlreadyExists,
            Ctx::ObjectWrite,
        ) => {
            // S3 has no such error; on hierarchical backends `a` and `a/b` cannot coexist.
            return S3Error::with_message(
                S3ErrorCode::InvalidRequest,
                "key conflicts with an existing object or prefix on this backend",
            );
        }
        (ErrorKind::AlreadyExists, Ctx::Bucket) => S3ErrorCode::BucketAlreadyOwnedByYou,
        (ErrorKind::ConditionNotMatch, _) => S3ErrorCode::PreconditionFailed,
        (ErrorKind::PermissionDenied, _) => S3ErrorCode::AccessDenied,
        (ErrorKind::Unsupported, _) => S3ErrorCode::NotImplemented,
        (ErrorKind::RateLimited, _) => S3ErrorCode::SlowDown,
        _ => {
            // Backend details (paths, hostnames, credentials in URLs) stay in the log.
            tracing::error!(error = %err, ?ctx, "backend error");
            S3ErrorCode::InternalError
        }
    };
    S3Error::new(code)
}

/// OpenDAL's fs service reports EISDIR and ENOTDIR as `Unexpected` and keeps the io error only
/// as the source, so the kind alone cannot tell a key/prefix conflict from a real failure.
fn kind(err: &opendal::Error) -> ErrorKind {
    match (err.kind(), io_kind(err)) {
        (ErrorKind::Unexpected, Some(io::ErrorKind::IsADirectory)) => ErrorKind::IsADirectory,
        (ErrorKind::Unexpected, Some(io::ErrorKind::NotADirectory)) => ErrorKind::NotADirectory,
        (kind, _) => kind,
    }
}

/// The io error behind an OpenDAL error, for services that wrap one.
pub(crate) fn io_kind(err: &opendal::Error) -> Option<io::ErrorKind> {
    std::error::Error::source(err)?
        .downcast_ref::<io::Error>()
        .map(io::Error::kind)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn code(kind: ErrorKind, ctx: Ctx) -> S3ErrorCode {
        map(opendal::Error::new(kind, "boom"), ctx).code().clone()
    }

    #[test]
    fn not_found_depends_on_what_was_looked_up() {
        assert_eq!(
            code(ErrorKind::NotFound, Ctx::Bucket),
            S3ErrorCode::NoSuchBucket
        );
        assert_eq!(
            code(ErrorKind::NotFound, Ctx::ObjectRead),
            S3ErrorCode::NoSuchKey
        );
    }

    #[test]
    fn reading_a_directory_is_a_missing_key() {
        // S3 reports NoSuchKey for `a` when only `a/b` exists.
        assert_eq!(
            code(ErrorKind::IsADirectory, Ctx::ObjectRead),
            S3ErrorCode::NoSuchKey
        );
    }

    /// What OpenDAL's fs service actually returns for a failed syscall: `Unexpected`, with the
    /// io error as the source.
    fn fs_error(io_kind: io::ErrorKind) -> opendal::Error {
        opendal::Error::new(ErrorKind::Unexpected, "fs").set_source(io::Error::from(io_kind))
    }

    #[test]
    fn fs_key_prefix_conflicts_are_invalid_request() {
        // fs never reports IsADirectory/NotADirectory as kinds; without looking at the io source
        // every conflict would surface as a retryable InternalError instead of a client error.
        for err in [
            fs_error(io::ErrorKind::IsADirectory),
            fs_error(io::ErrorKind::NotADirectory),
            opendal::Error::new(ErrorKind::AlreadyExists, "mkdir over a file"),
        ] {
            assert_eq!(
                *map(err, Ctx::ObjectWrite).code(),
                S3ErrorCode::InvalidRequest
            );
        }
    }

    #[test]
    fn a_name_too_long_for_the_filesystem_is_no_object_and_cannot_be_stored() {
        // Not a 500: the client sent a key the backend cannot hold, which is its to change.
        let too_long = || fs_error(io::ErrorKind::InvalidFilename);
        assert_eq!(
            *map(too_long(), Ctx::ObjectRead).code(),
            S3ErrorCode::NoSuchKey
        );
        assert_eq!(
            *map(too_long(), Ctx::ObjectWrite).code(),
            S3ErrorCode::KeyTooLongError
        );
    }

    #[test]
    fn reading_below_an_object_is_a_missing_key() {
        // GET `a/b` when `a` is an object: S3 says NoSuchKey, fs says ENOTDIR.
        assert_eq!(
            *map(fs_error(io::ErrorKind::NotADirectory), Ctx::ObjectRead).code(),
            S3ErrorCode::NoSuchKey
        );
    }

    #[test]
    fn other_fs_failures_stay_internal() {
        assert_eq!(
            *map(fs_error(io::ErrorKind::StorageFull), Ctx::ObjectWrite).code(),
            S3ErrorCode::InternalError
        );
    }

    #[test]
    fn unknown_errors_do_not_leak_details() {
        let err = map(
            opendal::Error::new(ErrorKind::Unexpected, "/secret/path"),
            Ctx::Other,
        );
        assert_eq!(*err.code(), S3ErrorCode::InternalError);
        assert!(err.message().is_none_or(|m| !m.contains("/secret/path")));
    }
}
