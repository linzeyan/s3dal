//! Reads straight from the filesystem behind OpenDAL's fs service, made the way that service
//! makes them, so that several take one trip through tokio's blocking pool where OpenDAL takes
//! one each. On fs such a trip, a thread handoff and its futex wake-ups, costs more than the
//! syscalls it carries.

#[cfg(unix)]
use std::collections::HashMap;
#[cfg(unix)]
use std::io::{self, Read, Seek, SeekFrom};
#[cfg(unix)]
use std::ops::Range;
use std::path::Path;
#[cfg(unix)]
use std::path::PathBuf;

#[cfg(unix)]
use bytes::Bytes;
use opendal::raw::{Timestamp, new_std_io_error};
use opendal::{Metadata, MetadataBuilder, Operator};

pub(super) fn is_fs(op: &Operator) -> bool {
    op.info().scheme() == "fs"
}

/// Objects up to this size come with GET's stat (see [`read_small`]). OpenDAL's fs reader reads
/// up to 2 MiB at a time, so it read such an object in one piece too, into as much memory.
#[cfg(unix)]
pub(super) const READ_WHOLE: u64 = 2 << 20;

/// Runs `read` on the blocking pool.
pub(super) async fn blocking<T: Send + 'static>(
    read: impl FnOnce() -> opendal::Result<T> + Send + 'static,
) -> opendal::Result<T> {
    tokio::task::spawn_blocking(read).await.map_err(|join| {
        opendal::Error::new(opendal::ErrorKind::Unexpected, "blocking task failed").set_source(join)
    })?
}

/// What OpenDAL's fs `stat` returns for `path`, read the same way (metadata following links,
/// then the `user.` xattrs whose values are UTF-8), so a listing or a GET reports what HEAD
/// does.
pub(super) fn stat(path: &Path) -> opendal::Result<Metadata> {
    let found = std::fs::metadata(path).map_err(new_std_io_error)?;
    described(path, &found)
}

fn described(path: &Path, found: &std::fs::Metadata) -> opendal::Result<Metadata> {
    let mut m = if found.is_dir() {
        MetadataBuilder::dir()
    } else if found.is_file() {
        MetadataBuilder::file(found.len())
    } else {
        MetadataBuilder::unknown()
    };
    m.last_modified(Timestamp::try_from(
        found.modified().map_err(new_std_io_error)?,
    )?);
    #[cfg(unix)]
    {
        let user_metadata = user_metadata(path)?;
        if !user_metadata.is_empty() {
            m.user_metadata(user_metadata);
        }
    }
    #[cfg(not(unix))]
    let _ = path;
    Ok(m.build())
}

#[cfg(unix)]
fn user_metadata(path: &Path) -> opendal::Result<HashMap<String, String>> {
    let mut found = HashMap::new();
    let names = match xattr::list(path) {
        Ok(names) => names,
        // As OpenDAL reads them: a filesystem without xattrs holds no user metadata.
        Err(err) if err.kind() == io::ErrorKind::Unsupported || err.raw_os_error() == Some(95) => {
            return Ok(found);
        }
        Err(err) => return Err(new_std_io_error(err)),
    };
    for name in names {
        if let Some(key) = name.to_string_lossy().strip_prefix("user.")
            && let Ok(Some(value)) = xattr::get(path, &name)
            && let Ok(value) = String::from_utf8(value)
        {
            found.insert(key.to_owned(), value);
        }
    }
    Ok(found)
}

/// GET's [`stat`] of `path`, with the content when it is a file of at most [`READ_WHOLE`]
/// bytes. Through OpenDAL the stat, the open and the read are a trip through the blocking pool
/// each, most of what a small GET costs.
#[cfg(unix)]
pub(super) fn read_small(path: &Path) -> opendal::Result<(Metadata, Option<Bytes>)> {
    let found = std::fs::metadata(path).map_err(new_std_io_error)?;
    let m = described(path, &found)?;
    if !found.is_file() || found.len() > READ_WHOLE {
        return Ok((m, None));
    }
    let body = read_whole(path, &found).map_err(new_std_io_error)?;
    Ok((m, body.map(Bytes::from)))
}

/// The content of the file at `path` that `found` describes, or `None` if another file has
/// taken its place since (writes land by rename): that content is not the one the metadata
/// describes, and GET then reads it as it reads a large object.
#[cfg(unix)]
fn read_whole(path: &Path, found: &std::fs::Metadata) -> io::Result<Option<Vec<u8>>> {
    use std::os::unix::fs::MetadataExt;

    // Opened only once the stat showed a regular file: opening a FIFO would block.
    let mut file = std::fs::File::open(path)?;
    let opened = file.metadata()?;
    if (opened.dev(), opened.ino()) != (found.dev(), found.ino()) {
        return Ok(None);
    }
    let mut body = vec![0; usize::try_from(found.len()).map_err(io::Error::other)?];
    file.read_exact(&mut body)?;
    Ok(Some(body))
}

/// Why [`assemble`] failed: reading the source at this index, or writing the file.
#[cfg(unix)]
pub(super) enum AssembleError {
    Source(usize, opendal::Error),
    Write(opendal::Error),
}

/// Writes `sources`, each a file's path and the range of it to take, one after another into a
/// new file at `tmp`, with `user_metadata` as its `user.` xattrs, synced, as
/// OpenDAL's fs writer leaves a write it closes; returns what that writer's close reports. The
/// file is removed on failure.
///
/// `io::copy` from file to file is `copy_file_range` on Linux: the data never passes through
/// s3dal, and a filesystem that shares extents (btrfs, XFS) writes none of it again. OpenDAL's
/// writer copies through a buffer instead, and sets the xattrs only after the rename, so an
/// object could be seen without its ETag.
#[cfg(unix)]
pub(super) fn assemble(
    tmp: &Path,
    sources: &[(PathBuf, Range<u64>)],
    user_metadata: &HashMap<String, String>,
) -> Result<Metadata, AssembleError> {
    let assembled = write_assembly(tmp, sources, user_metadata);
    if assembled.is_err() {
        let _ = std::fs::remove_file(tmp);
    }
    assembled
}

#[cfg(unix)]
fn write_assembly(
    tmp: &Path,
    sources: &[(PathBuf, Range<u64>)],
    user_metadata: &HashMap<String, String>,
) -> Result<Metadata, AssembleError> {
    use xattr::FileExt;

    let write = |err: io::Error| AssembleError::Write(new_std_io_error(err));
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(tmp)
        .map_err(write)?;
    for (n, (path, range)) in sources.iter().enumerate() {
        let source = |err: io::Error| AssembleError::Source(n, new_std_io_error(err));
        let mut from = std::fs::File::open(path).map_err(source)?;
        from.seek(SeekFrom::Start(range.start)).map_err(source)?;
        let len = range.end - range.start;
        let copied = io::copy(&mut from.take(len), &mut file).map_err(source)?;
        if copied != len {
            return Err(source(io::ErrorKind::UnexpectedEof.into()));
        }
    }
    for (key, value) in user_metadata {
        file.set_xattr(format!("user.{key}"), value.as_bytes())
            .map_err(write)?;
    }
    file.sync_all().map_err(write)?;
    let written = file.metadata().map_err(write)?;
    let mut m = MetadataBuilder::file(written.len());
    m.last_modified(
        Timestamp::try_from(written.modified().map_err(write)?).map_err(AssembleError::Write)?,
    );
    Ok(m.build())
}

/// Makes the directories `target` lies in, as OpenDAL's fs writer makes them when it opens.
#[cfg(unix)]
pub(super) fn make_parents(target: &Path) -> opendal::Result<()> {
    match target.parent() {
        Some(parent) => std::fs::create_dir_all(parent).map_err(new_std_io_error),
        None => Ok(()),
    }
}

/// Moves the file at `tmp` to `target`. The directories on the way are made again: a delete
/// that emptied one may have pruned it since [`make_parents`].
#[cfg(unix)]
pub(super) fn rename_into_place(tmp: &Path, target: &Path) -> opendal::Result<()> {
    make_parents(target)?;
    std::fs::rename(tmp, target).map_err(new_std_io_error)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fs_operator(root: &Path) -> Operator {
        let root = root.to_string_lossy().into_owned();
        crate::backend::build_operator("fs", &[("root".to_owned(), root)]).unwrap()
    }

    #[tokio::test]
    async fn fs_stats_report_what_opendal_stats_do() {
        // Listings and GETs stat fs keys themselves; a field read differently from OpenDAL's
        // stat would make them and HEAD disagree about the object (its ETag lives in xattrs,
        // which only OpenDAL's way of reading them maps to user metadata).
        let root = tempfile::tempdir().unwrap();
        let op = fs_operator(root.path());
        op.write_with("b/k", b"data".to_vec())
            .user_metadata([
                ("s3dal-etag".to_owned(), "\"abc\"".to_owned()),
                ("other".to_owned(), "v".to_owned()),
            ])
            .await
            .unwrap();
        op.write("b/plain", b"x".to_vec()).await.unwrap();
        op.create_dir("b/d/").await.unwrap();
        #[cfg(unix)]
        xattr::set(root.path().join("b/plain"), "user.bin", &[0xff, 0xfe]).unwrap();

        for path in ["b/k", "b/plain", "b/d/"] {
            let ours = stat(&root.path().join(path)).unwrap();
            let theirs = op.stat(path).await.unwrap();
            assert_eq!(ours.mode(), theirs.mode(), "{path}");
            assert_eq!(ours.content_length(), theirs.content_length(), "{path}");
            assert_eq!(ours.last_modified(), theirs.last_modified(), "{path}");
            // Stored sorted, so equal maps print alike.
            assert_eq!(
                format!("{:?}", ours.user_metadata()),
                format!("{:?}", theirs.user_metadata()),
                "{path}"
            );
            assert_eq!(ours.etag(), theirs.etag(), "{path}");
        }
        #[cfg(unix)]
        assert_eq!(
            stat(&root.path().join("b/k"))
                .unwrap()
                .user_metadata()
                .map(|m| m.len()),
            Some(2)
        );
        let ours = stat(&root.path().join("b/missing")).unwrap_err();
        let theirs = op.stat("b/missing").await.unwrap_err();
        assert_eq!(ours.kind(), theirs.kind());
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn a_small_object_comes_with_its_stat_and_a_large_one_does_not() {
        // A small GET's whole cost was three blocking-pool trips (stat, open, read); its body
        // must come with the stat, and the stat must be HEAD's. A large object keeps
        // OpenDAL's reader, which streams it instead of holding it in memory.
        let root = tempfile::tempdir().unwrap();
        let op = fs_operator(root.path());
        op.write_with("b/small", b"data".to_vec())
            .user_metadata([("s3dal-etag".to_owned(), "\"abc\"".to_owned())])
            .await
            .unwrap();
        let large = usize::try_from(READ_WHOLE).unwrap() + 1;
        op.write("b/large", vec![7; large]).await.unwrap();
        op.create_dir("b/d/").await.unwrap();

        let (m, body) = read_small(&root.path().join("b/small")).unwrap();
        assert_eq!(body.as_deref(), Some(&b"data"[..]));
        let head = op.stat("b/small").await.unwrap();
        assert_eq!(m.content_length(), head.content_length());
        assert_eq!(m.last_modified(), head.last_modified());
        assert_eq!(
            format!("{:?}", m.user_metadata()),
            format!("{:?}", head.user_metadata())
        );

        let (m, body) = read_small(&root.path().join("b/large")).unwrap();
        assert_eq!(m.content_length(), large as u64);
        assert!(body.is_none());
        let (m, body) = read_small(&root.path().join("b/d")).unwrap();
        assert!(m.is_dir());
        assert!(body.is_none());
        let ours = read_small(&root.path().join("b/missing")).unwrap_err();
        assert_eq!(ours.kind(), op.stat("b/missing").await.unwrap_err().kind());
    }

    #[cfg(unix)]
    #[test]
    fn assembling_joins_the_sources_and_leaves_nothing_when_one_is_gone() {
        // Complete on fs builds the object from its parts' files: each the range its manifest
        // records, which in the upload's object file need not start at 0, in order, with the
        // user metadata where OpenDAL keeps it. A part an Abort removed meanwhile fails it,
        // naming that part, with no file left behind.
        let root = tempfile::tempdir().unwrap();
        let (a, b) = (root.path().join("a"), root.path().join("b"));
        std::fs::write(&a, b"..hello-").unwrap();
        std::fs::write(&b, b"world, and more").unwrap();
        let metadata = HashMap::from([("s3dal-etag".to_owned(), "\"x\"".to_owned())]);

        let tmp = root.path().join("t1");
        let Ok(m) = assemble(&tmp, &[(a.clone(), 2..8), (b, 0..5)], &metadata) else {
            panic!("assembly failed");
        };
        assert_eq!(std::fs::read(&tmp).unwrap(), b"hello-world");
        assert_eq!(m.content_length(), 11);
        let stored = stat(&tmp).unwrap();
        assert_eq!(
            stored.user_metadata().and_then(|m| m.get("s3dal-etag")),
            Some("\"x\"")
        );

        let tmp = root.path().join("t2");
        let gone = root.path().join("gone");
        match assemble(&tmp, &[(a, 2..8), (gone, 0..1)], &metadata) {
            Err(AssembleError::Source(1, err)) => {
                assert_eq!(err.kind(), opendal::ErrorKind::NotFound);
            }
            _ => panic!("a missing source must fail as that source"),
        }
        assert!(!tmp.exists());
    }

    #[cfg(unix)]
    #[test]
    fn a_file_replaced_after_its_stat_is_not_read_under_that_stat() {
        // A PUT lands by rename. Content read from the file that took the old one's place
        // would go out under the old ETag and length.
        let root = tempfile::tempdir().unwrap();
        let path = root.path().join("k");
        std::fs::write(&path, b"old").unwrap();
        let found = std::fs::metadata(&path).unwrap();
        assert_eq!(
            read_whole(&path, &found).unwrap().as_deref(),
            Some(&b"old"[..])
        );

        let next = root.path().join("k.tmp");
        std::fs::write(&next, b"new").unwrap();
        std::fs::rename(&next, &path).unwrap();
        assert_eq!(read_whole(&path, &found).unwrap(), None);
    }
}
