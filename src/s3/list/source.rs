// SPDX-FileCopyrightText: 2023-2026 The s3s Authors
// SPDX-License-Identifier: Apache-2.0
//
// The ordered walk follows s3s-fs (`list_objects_ordered`, `walk_listing_dir` and
// `subtree_contains_file` in crates/s3s-fs/src/s3.rs), rewritten over OpenDAL.

//! Where a page's keys come from. A backend that lists in key order is read through, seeking
//! past CommonPrefix groups; any other is walked one directory at a time in key order. Neither
//! reads more than the page needs, and neither decides what the page holds: that is
//! [`PageBuilder`]'s job.

use std::io;
use std::path::PathBuf;

use futures::future::Either;
use futures::{TryStreamExt, stream};
use opendal::options::ListOptions;
use opendal::{Entry, Lister, Metadata, MetadataBuilder, Operator};
use s3s::S3Result;

use super::page::{Page, PageBuilder, Query};
use crate::error::{Ctx, map};
use crate::s3::{S3Dal, naming};

/// Entries of a CommonPrefix group read past its first key before seeking past the rest. A seek
/// is one more list request, as is reading one more page of entries (1000 on S3): a small group
/// is cheaper to read through than to seek past, and a large one then costs at most about twice
/// what the better choice would have.
const SEEK_AFTER: usize = 1000;

pub(super) async fn collect(dal: &S3Dal, bucket: &str, query: &Query) -> S3Result<Page<Metadata>> {
    let mut page = PageBuilder::new(query);
    let bucket_dir = naming::bucket_dir(bucket);
    if !page.is_full()
        && let Some(start) = prefix_dir(&query.prefix)
    {
        if dal.profile.ordered_list {
            read_ordered(&dal.op, &bucket_dir, &mut page, SEEK_AFTER).await?;
        } else {
            walk(&dal.op, &bucket_dir, start, &mut page).await?;
        }
    }
    Ok(page.finish())
}

/// The directory part of `prefix`, below which every matching key lies, so the walk starts
/// there instead of at the bucket root. Keys never contain empty, `.` or `..` segments, or a
/// control character XML cannot carry, so a prefix with one matches nothing. Refusing it here
/// also keeps `..` out of backend paths, where it would lead out of the bucket, and the control
/// character away from an S3 upstream, which echoes the prefix into XML that cannot be parsed
/// (RustFS 1.0.2 does, even when asked for `encoding-type=url`).
fn prefix_dir(prefix: &str) -> Option<&str> {
    let dir = &prefix[..prefix.rfind('/').map_or(0, |i| i + 1)];
    let possible =
        naming::xml_safe(prefix) && (dir.is_empty() || naming::validate_key(dir).is_ok());
    possible.then_some(dir)
}

/// The backend's own order is the key order (see `Profile::ordered_list`), so this maps paths
/// to keys and saves reading what the page would drop. A CommonPrefix stands for its whole
/// group, so once `seek_after` entries of a group have been read past its first key, the
/// listing restarts past the group: reading it through would make a delimiter listing cost as
/// much as listing every key. `seek_after` is [`SEEK_AFTER`] outside tests.
async fn read_ordered(
    op: &Operator,
    bucket_dir: &str,
    page: &mut PageBuilder<'_, Metadata>,
    seek_after: usize,
) -> S3Result<()> {
    let query = page.query();
    let path = format!("{bucket_dir}{}", query.prefix);
    // A marker that is a CommonPrefix, or lies inside one, skips that whole group (see
    // `Item::name`), so the listing can start past the group.
    let mut after = query
        .start_after
        .as_deref()
        .map(|marker| match query.common_prefix(marker) {
            Some(group) => past_group(group, marker),
            None => marker.to_owned(),
        });
    let mut lister = list_after(op, bucket_dir, &path, after.as_deref()).await?;
    // The CommonPrefix of the last key offered, and the entries read inside it since.
    let mut group: Option<String> = None;
    let mut read_in_group = 0;
    while !page.is_full()
        && let Some(entry) = lister.try_next().await.map_err(backend)?
    {
        let (entry_path, meta) = entry.into_parts();
        let Some(name) = entry_path.strip_prefix(bucket_dir) else {
            continue;
        };
        // The builder knows the marker but not where the listing sought to, so what lies at or
        // before that is skipped here. A backend that ignored the seek would otherwise bring
        // the group back, and the listing would seek past it again forever.
        if after.as_deref().is_some_and(|after| name <= after) {
            continue;
        }
        let name_group = query.common_prefix(name);
        if name_group.is_some() && name_group == group.as_deref() {
            read_in_group += 1;
        } else {
            // Only a key makes a CommonPrefix: a group of folder markers is no group to skip.
            group = name_group.filter(|_| meta.is_file()).map(str::to_owned);
            read_in_group = 0;
        }
        // Directories, folder markers included, are never listed as keys.
        if meta.is_file() {
            page.offer(name, meta);
        }
        if let Some(group) = group.as_deref()
            && read_in_group >= seek_after
            && !page.is_full()
        {
            let past = past_group(group, name);
            lister = list_after(op, bucket_dir, &path, Some(&past)).await?;
            after = Some(past);
        }
    }
    Ok(())
}

/// A recursive lister of `path`, resuming after the key `start_after`, or before it.
///
/// An S3 upstream echoes the start-after into its XML too (see [`prefix_dir`]), and a marker
/// may hold a control character where a URL-encoded listing asks for one. No key holds one, so
/// the lister resumes where that character starts instead: only keys going on with a tab, line
/// feed or carriage return there lie between, and the callers drop them as lying before the
/// marker.
async fn list_after(
    op: &Operator,
    bucket_dir: &str,
    path: &str,
    start_after: Option<&str>,
) -> S3Result<Lister> {
    let start_after =
        start_after.map(|key| format!("{bucket_dir}{}", naming::xml_safe_prefix(key)));
    #[cfg(test)]
    tests::record_lister(path, start_after.as_deref());
    let opts = ListOptions {
        recursive: true,
        start_after,
        ..Default::default()
    };
    op.lister_options(path, opts).await.map_err(backend)
}

/// A start-after past every key in `group`, a CommonPrefix, and past `last`, the name the
/// listing has reached in it.
///
/// The exact bound, the greatest string that starts with `group`, does not exist: it would be
/// `group` followed by U+10FFFF, the greatest character, repeated forever. So the run is made
/// just long enough to sort after `last`, and each seek lands strictly past what was already
/// read. A key it misses continues the same run of U+10FFFF, which no real name does; such a
/// key is read and sought past again, so the listing still moves forward. A run long enough to
/// pass any 1024-byte key would send a start-after longer than any key on every seek.
fn past_group(group: &str, last: &str) -> String {
    // Termination: `last` sorts before `group` plus a run one character longer than its own.
    debug_assert!(last.starts_with(group), "{last:?} is not in {group:?}");
    let mut past = group.to_owned();
    loop {
        past.push(char::MAX);
        if past.as_str() > last {
            return past;
        }
    }
}

/// Feeds the keys below `start` (a directory, `""` for the bucket root) in key order.
///
/// Sorting each directory by entry name, with a directory named `name/` as OpenDAL names it,
/// gives the global key order: `.` (0x2E) sorts before `/` (0x2F), so `a.txt` precedes
/// `a/b`, and `a/` precedes `a0`. The walk descends depth first and lazily, skips directories
/// that cannot reach the page, and stands for a directory that folds into one CommonPrefix
/// with a single file from it.
///
/// With `/` as the delimiter every directory below the prefix folds, so a page costs one
/// listing per directory on the way down plus one per CommonPrefix. With any other delimiter
/// only a directory whose path holds the delimiter after the prefix folds. Every other one has
/// to be read, because the delimiter may first occur anywhere in the names below it, so the
/// walk may read every key below the prefix.
async fn walk(
    op: &Operator,
    bucket_dir: &str,
    start: &str,
    page: &mut PageBuilder<'_, Metadata>,
) -> S3Result<()> {
    let query = page.query();
    let mut stack = vec![sorted_children(op, bucket_dir, start).await?.into_iter()];
    while let Some(children) = stack.last_mut() {
        let Some((path, meta)) = children.next() else {
            stack.pop();
            continue;
        };
        if page.is_full() {
            break;
        }
        if meta.is_file() {
            page.offer(&path, meta);
        } else if !query.may_list_below(&path) {
            continue;
        } else if let Some(prefix) = query.common_prefix(&path) {
            // A group of a delimiter other than `/` can span several directories (`a-1/` and
            // `a-2/` for `-`); once one of them proved it, the others add nothing.
            if query.is_after_start(prefix)
                && !page.took_last(prefix)
                && let Some((key, meta)) = any_file_below(op, bucket_dir, &path).await?
            {
                page.offer(&key, meta);
            }
        } else {
            stack.push(sorted_children(op, bucket_dir, &path).await?.into_iter());
        }
    }
    Ok(())
}

/// One directory's files and subdirectories, as paths relative to the bucket, in key order.
async fn sorted_children(
    op: &Operator,
    bucket_dir: &str,
    dir: &str,
) -> S3Result<Vec<(String, Metadata)>> {
    #[cfg(test)]
    tests::record_listing(dir);
    let path = format!("{bucket_dir}{dir}");
    let entries = if is_fs(op) {
        fs_entries(op, path, |_| false).await
    } else {
        op.list(&path)
            .await
            .map(|entries| entries.into_iter().map(Entry::into_parts).collect())
    };
    let mut children: Vec<(String, Metadata)> = entries
        .map_err(backend)?
        .into_iter()
        .filter_map(|(path, meta)| {
            let rel = path.strip_prefix(bucket_dir)?;
            // The listing includes `dir` itself; other entry kinds (symlinks, sockets) cannot
            // be objects.
            (rel != dir && (meta.is_file() || meta.is_dir())).then(|| (rel.to_owned(), meta))
        })
        .collect();
    children.sort_unstable_by(|a, b| a.0.cmp(&b.0));
    Ok(children)
}

/// A CommonPrefix is listed only if an object exists below it, and any one file proves that,
/// so the search stops at the first file in whatever order the backend lists. Only a file
/// whose name is a key counts: the page drops any other (one another tool wrote), and the
/// CommonPrefix with it, though valid keys lie below.
async fn any_file_below(
    op: &Operator,
    bucket_dir: &str,
    dir: &str,
) -> S3Result<Option<(String, Metadata)>> {
    let is_key = |bucket_dir: &str, path: &str| {
        path.strip_prefix(bucket_dir)
            .is_some_and(|rel| naming::validate_key(rel).is_ok())
    };
    let mut pending = vec![dir.to_owned()];
    while let Some(dir) = pending.pop() {
        #[cfg(test)]
        tests::record_listing(&dir);
        let path = format!("{bucket_dir}{dir}");
        let mut entries = if is_fs(op) {
            let bucket_dir = bucket_dir.to_owned();
            let entries = fs_entries(op, path, move |path| is_key(&bucket_dir, path))
                .await
                .map_err(backend)?;
            Either::Left(stream::iter(entries.into_iter().map(Ok)))
        } else {
            let lister = op.lister(&path).await.map_err(backend)?;
            Either::Right(lister.map_ok(Entry::into_parts))
        };
        while let Some((path, meta)) = entries.try_next().await.map_err(backend)? {
            let Some(rel) = path.strip_prefix(bucket_dir) else {
                continue;
            };
            if meta.is_file() && is_key(bucket_dir, &path) {
                return Ok(Some((rel.to_owned(), meta)));
            }
            if meta.is_dir() && rel != dir {
                pending.push(rel.to_owned());
            }
        }
    }
    Ok(None)
}

fn is_fs(op: &Operator) -> bool {
    op.info().scheme() == "fs"
}

/// The files and directories in the backend directory `path`, named as OpenDAL lists them,
/// read straight from the filesystem, up to the first file `stop_at` accepts.
///
/// OpenDAL's fs lister stats every entry, each in a blocking task of its own, for a size and
/// mtime the listing never uses: fs keeps the ETag in xattrs, so every key a page returns is
/// stat'ed again anyway. The walk reads whole directories, so each page of a 10,000-key
/// directory paid 10,000 such stats, where readdir alone gives names and types. The lister also
/// trims trailing whitespace off a directory's name, so the keys below `a /` were never listed.
async fn fs_entries(
    op: &Operator,
    path: String,
    stop_at: impl Fn(&str) -> bool + Send + 'static,
) -> opendal::Result<Vec<(String, Metadata)>> {
    let dir = PathBuf::from(op.info().root()).join(&path);
    let read = move || -> io::Result<Vec<(String, Metadata)>> {
        let entries = match std::fs::read_dir(&dir) {
            Ok(entries) => entries,
            // As OpenDAL lists them: a directory that is gone, or a file, holds nothing.
            Err(err)
                if matches!(
                    err.kind(),
                    io::ErrorKind::NotFound | io::ErrorKind::NotADirectory
                ) =>
            {
                return Ok(Vec::new());
            }
            Err(err) => return Err(err),
        };
        let mut found = Vec::new();
        for entry in entries {
            let entry = entry?;
            // Keys are UTF-8, so no other name can be an object or lead to one.
            let Ok(name) = entry.file_name().into_string() else {
                continue;
            };
            // Not followed, as OpenDAL does not: a symlink is neither, and is no object.
            let file_type = match entry.file_type() {
                Ok(file_type) => file_type,
                Err(err) if err.kind() == io::ErrorKind::NotFound => continue,
                Err(err) => return Err(err),
            };
            if file_type.is_dir() {
                found.push((format!("{path}{name}/"), MetadataBuilder::dir().build()));
            } else if file_type.is_file() {
                let file = format!("{path}{name}");
                let stop = stop_at(&file);
                // The size is left unread: every fs key the page returns is stat'ed (see above).
                found.push((file, MetadataBuilder::file(0).build()));
                if stop {
                    break;
                }
            }
        }
        Ok(found)
    };
    let io_error = |err: io::Error| {
        let kind = match err.kind() {
            io::ErrorKind::PermissionDenied => opendal::ErrorKind::PermissionDenied,
            _ => opendal::ErrorKind::Unexpected,
        };
        opendal::Error::new(kind, "list directory").set_source(err)
    };
    tokio::task::spawn_blocking(read)
        .await
        .map_err(|err| io_error(err.into()))?
        .map_err(io_error)
}

fn backend(err: opendal::Error) -> s3s::S3Error {
    map(err, Ctx::Other)
}

#[cfg(test)]
mod tests {
    use std::cell::RefCell;
    use std::collections::BTreeSet;
    use std::path::Path;

    use super::super::page::Item;
    use super::*;
    use crate::backend::build_operator;

    /// A lister as the ordered path asked for it: backend path and start-after.
    type ListerStart = (String, Option<String>);

    thread_local! {
        static LISTED: RefCell<Vec<String>> = const { RefCell::new(Vec::new()) };
        static LISTERS: RefCell<Vec<ListerStart>> = const { RefCell::new(Vec::new()) };
    }

    /// Directories listed on this thread, so tests can show what the walk did not read.
    pub(super) fn record_listing(dir: &str) {
        LISTED.with(|listed| listed.borrow_mut().push(dir.to_owned()));
    }

    fn take_listed() -> Vec<String> {
        LISTED.with(|listed| listed.take())
    }

    /// Ordered listers started on this thread, so tests can show where the ordered path sought
    /// to: the memory backend ignores start-after, so its results cannot show that.
    pub(super) fn record_lister(path: &str, start_after: Option<&str>) {
        LISTERS.with(|listers| {
            listers
                .borrow_mut()
                .push((path.to_owned(), start_after.map(str::to_owned)));
        });
    }

    fn take_listers() -> Vec<ListerStart> {
        LISTERS.with(|listers| listers.take())
    }

    fn query(prefix: &str, delimiter: bool, start_after: Option<&str>, max_keys: usize) -> Query {
        Query {
            prefix: prefix.to_owned(),
            delimiter: delimiter.then(|| "/".to_owned()),
            start_after: start_after.map(str::to_owned),
            max_keys,
        }
    }

    fn names(page: &Page<Metadata>) -> Vec<String> {
        page.items
            .iter()
            .map(|item| match item {
                Item::Object { key, .. } => key.clone(),
                Item::CommonPrefix(prefix) => format!("CP:{prefix}"),
            })
            .collect()
    }

    /// A bucket `b` on an fs operator, holding `files` and the empty directories `dirs`.
    fn fs_bucket(root: &Path, files: &[&str], dirs: &[&str]) -> Operator {
        for file in files {
            let path = root.join("b").join(file);
            std::fs::create_dir_all(path.parent().unwrap()).unwrap();
            std::fs::write(path, b"x").unwrap();
        }
        for dir in dirs {
            std::fs::create_dir_all(root.join("b").join(dir)).unwrap();
        }
        build_operator("fs", &[("root".into(), root.to_string_lossy().into())]).unwrap()
    }

    async fn walk_page(op: &Operator, q: &Query) -> (Vec<String>, Vec<String>) {
        take_listed();
        let mut page = PageBuilder::new(q);
        let start = prefix_dir(&q.prefix).unwrap();
        walk(op, "b/", start, &mut page).await.unwrap();
        (names(&page.finish()), take_listed())
    }

    #[tokio::test]
    async fn the_walk_starts_in_the_directory_the_prefix_names() {
        // Listing `photos/2021/` must not read the bucket root, which can be huge.
        let root = tempfile::tempdir().unwrap();
        let op = fs_bucket(root.path(), &["a/x", "p/q/r1", "p/q/s/t", "z"], &[]);
        let (names, listed) = walk_page(&op, &query("p/q/r", false, None, 1000)).await;
        assert_eq!(names, ["p/q/r1"]);
        assert_eq!(listed, ["p/q/"]);
    }

    #[tokio::test]
    async fn the_walk_skips_directories_before_the_marker() {
        // Resuming deep into a large bucket must not re-read everything before the marker.
        let root = tempfile::tempdir().unwrap();
        let op = fs_bucket(root.path(), &["a/x", "b/y", "c/z"], &[]);
        let (names, listed) = walk_page(&op, &query("", false, Some("b0"), 1000)).await;
        assert_eq!(names, ["c/z"]);
        assert_eq!(listed, ["", "c/"]);
        // A marker inside a directory still needs the rest of that directory.
        let (names, listed) = walk_page(&op, &query("", false, Some("b/x"), 1000)).await;
        assert_eq!(names, ["b/y", "c/z"]);
        assert_eq!(listed, ["", "b/", "c/"]);
    }

    #[tokio::test]
    async fn the_walk_stops_one_item_past_the_page() {
        let root = tempfile::tempdir().unwrap();
        let op = fs_bucket(root.path(), &["a/x", "b/y", "c/z", "d/w"], &[]);
        let (names, listed) = walk_page(&op, &query("", false, None, 1)).await;
        assert_eq!(names, ["a/x"]);
        assert_eq!(listed, ["", "a/", "b/"]);
    }

    #[tokio::test]
    async fn one_file_proves_a_common_prefix() {
        // A CommonPrefix stands for its whole subtree; reading the subtree to emit it would
        // make a delimiter listing of a bucket cost as much as listing every key.
        let root = tempfile::tempdir().unwrap();
        let op = fs_bucket(root.path(), &["a/1", "a/2/x", "a/3/y", "b"], &[]);
        let (names, listed) = walk_page(&op, &query("", true, None, 1000)).await;
        assert_eq!(names, ["CP:a/", "b"]);
        assert_eq!(listed, ["", "a/"]);
    }

    #[tokio::test]
    async fn the_walk_proves_a_group_spanning_directories_once() {
        // With a delimiter other than `/`, one CommonPrefix can cover many directories
        // (`2021-01/`, `2021-02/`, ... for `-`). Proving each one with a listing of its own
        // would cost a request per directory for an item already on the page. A directory
        // whose path does not hold the delimiter has to be read: the delimiter may come below.
        let root = tempfile::tempdir().unwrap();
        let op = fs_bucket(
            root.path(),
            &["a-1/x", "a-2/y", "a-3/z", "b/c-d", "b/e"],
            &[],
        );
        let q = Query {
            delimiter: Some("-".to_owned()),
            ..query("", false, None, 1000)
        };
        let (names, listed) = walk_page(&op, &q).await;
        assert_eq!(names, ["CP:a-", "CP:b/c-", "b/e"]);
        assert_eq!(listed, ["", "a-1/", "b/"]);
    }

    #[tokio::test]
    async fn directories_without_files_are_no_common_prefix() {
        // S3 has no empty directories; showing one would list a folder that holds nothing.
        let root = tempfile::tempdir().unwrap();
        let op = fs_bucket(root.path(), &["k"], &["e/", "f/g/h/"]);
        let (names, _) = walk_page(&op, &query("", true, None, 1000)).await;
        assert_eq!(names, ["k"]);
    }

    #[tokio::test]
    async fn a_name_no_key_can_have_proves_no_common_prefix() {
        // Another tool may write such a file. Taken as the proof, it would be dropped from the
        // page, and the CommonPrefix with it, though `a/sub/ok` lies below.
        let root = tempfile::tempdir().unwrap();
        let op = fs_bucket(root.path(), &["a/bad\u{1}", "a/sub/ok"], &[]);
        let (names, _) = walk_page(&op, &query("", true, None, 1000)).await;
        assert_eq!(names, ["CP:a/"]);
    }

    #[tokio::test]
    async fn keys_below_a_directory_ending_in_whitespace_are_listed() {
        // `a /x` is a valid key, and OpenDAL's lister names its directory `a/`.
        let root = tempfile::tempdir().unwrap();
        let op = fs_bucket(root.path(), &["a /x", "b"], &[]);
        let (names, _) = walk_page(&op, &query("", false, None, 1000)).await;
        assert_eq!(names, ["a /x", "b"]);
        let (names, _) = walk_page(&op, &query("", true, None, 1000)).await;
        assert_eq!(names, ["CP:a /", "b"]);
    }

    #[cfg(unix)]
    #[tokio::test]
    async fn symlinks_are_no_objects() {
        // A link in the root could lead anywhere on the host; listing it, or what lies below
        // it, would serve files no client put there.
        let root = tempfile::tempdir().unwrap();
        let outside = tempfile::tempdir().unwrap();
        std::fs::write(outside.path().join("secret"), b"x").unwrap();
        let op = fs_bucket(root.path(), &["k"], &[]);
        let bucket = root.path().join("b");
        std::os::unix::fs::symlink(outside.path(), bucket.join("dir")).unwrap();
        std::os::unix::fs::symlink(outside.path().join("secret"), bucket.join("file")).unwrap();
        for delimiter in [false, true] {
            let (names, _) = walk_page(&op, &query("", delimiter, None, 1000)).await;
            assert_eq!(names, ["k"], "delimiter {delimiter}");
        }
    }

    #[test]
    fn prefixes_with_segments_no_key_can_have_match_nothing() {
        assert_eq!(prefix_dir("photos/20"), Some("photos/"));
        assert_eq!(prefix_dir("photo"), Some(""));
        assert_eq!(prefix_dir("a/.."), Some("a/"), "`..b` is a valid name");
        for bad in ["../x", "a/../", "a//b", "/a", "./", "a/x\u{1}"] {
            assert_eq!(prefix_dir(bad), None, "{bad}");
        }
    }

    #[tokio::test]
    async fn a_marker_with_a_control_character_never_reaches_the_backend() {
        // An S3 upstream echoes the start-after into XML, and RustFS answers a control
        // character there with a document nobody can parse: the listing failed with 500. No key
        // holds one, so seeking to where it starts lists the same page.
        let op = memory_bucket(["a", "b", "b\tx", "b\u{1}", "bz", "c"]).await;
        let (page, listers) = ordered_page(&op, &query("", false, Some("b\u{b}"), 1000), 0).await;
        assert_eq!(names(&page), ["bz", "c"]);
        assert_eq!(listers, [start(Some("b"))]);
    }

    #[tokio::test]
    async fn an_ordered_backend_is_read_in_its_own_order() {
        // fs cannot exercise this path, but the memory backend lists recursively in key order,
        // and ignores start-after, which the builder must cover.
        let op = build_operator("memory", &[]).unwrap();
        for key in ["a.txt", "a/b", "a/c/d", "a0", "m/k"] {
            op.write(&format!("b/{key}"), b"x".to_vec()).await.unwrap();
        }
        op.create_dir("b/m/").await.unwrap();
        op.write("other/z", b"x".to_vec()).await.unwrap();

        let read = |q: Query| {
            let op = op.clone();
            async move {
                let mut page = PageBuilder::new(&q);
                read_ordered(&op, "b/", &mut page, SEEK_AFTER)
                    .await
                    .unwrap();
                names(&page.finish())
            }
        };
        assert_eq!(
            read(query("", false, None, 1000)).await,
            ["a.txt", "a/b", "a/c/d", "a0", "m/k"],
            "folder markers are not keys"
        );
        assert_eq!(read(query("", true, Some("a/"), 2)).await, ["a0", "CP:m/"]);
        assert_eq!(
            read(query("a/", true, None, 1000)).await,
            ["a/b", "CP:a/c/"]
        );
    }

    /// A bucket `b` on a memory operator, which lists recursively in key order.
    async fn memory_bucket(keys: impl IntoIterator<Item = impl AsRef<str>>) -> Operator {
        let op = build_operator("memory", &[]).unwrap();
        for key in keys {
            let path = format!("b/{}", key.as_ref());
            op.write(&path, b"x".to_vec()).await.unwrap();
        }
        op
    }

    /// One page of bucket `b` from the ordered path, and the listers it started.
    async fn ordered_page(
        op: &Operator,
        q: &Query,
        seek_after: usize,
    ) -> (Page<Metadata>, Vec<ListerStart>) {
        take_listers();
        let mut page = PageBuilder::new(q);
        read_ordered(op, "b/", &mut page, seek_after).await.unwrap();
        (page.finish(), take_listers())
    }

    /// A lister of the whole bucket `b`, resuming after the key `after`.
    fn start(after: Option<&str>) -> ListerStart {
        ("b/".to_owned(), after.map(|key| format!("b/{key}")))
    }

    #[tokio::test]
    async fn the_ordered_path_seeks_past_a_large_common_prefix() {
        // A CommonPrefix stands for its whole group. Reading the group through to emit it makes
        // a delimiter listing cost as much as listing every key: one page of `aws s3 ls` on a
        // bucket whose `logs/` holds a million keys would take a thousand list requests.
        let mut keys: Vec<String> = (0..SEEK_AFTER + 2).map(|i| format!("a/{i:05}")).collect();
        keys.extend(["0", "b", "c/x"].map(str::to_owned));
        let op = memory_bucket(&keys).await;
        let q = query("", true, None, 1000);
        let (page, listers) = ordered_page(&op, &q, SEEK_AFTER).await;
        assert_eq!(names(&page), ["0", "CP:a/", "b", "CP:c/"]);
        assert_eq!(listers, [start(None), start(Some("a/\u{10FFFF}"))]);

        // `a/` is the item after the page's last, which only proves truncation; seeking past
        // it would be a request for nothing.
        let (page, listers) = ordered_page(&op, &query("", true, None, 1), 0).await;
        assert_eq!(
            (names(&page), page.next_marker),
            (vec!["0".into()], Some("0".into()))
        );
        assert_eq!(listers, [start(None)]);
    }

    #[tokio::test]
    async fn the_ordered_path_seeks_past_a_group_of_any_delimiter() {
        // A group is every key that starts with its CommonPrefix, whatever the delimiter, so
        // the seek that keeps a `/` listing cheap works for the others too, including one that
        // holds `/` but ends inside a name.
        let op = memory_bucket(["a-1", "a-2", "a-3", "b", "x/a", "x/b/1", "x/bz", "x/c"]).await;
        let q = |delimiter: &str| Query {
            delimiter: Some(delimiter.to_owned()),
            ..query("", false, None, 1000)
        };
        let (page, listers) = ordered_page(&op, &q("-"), 0).await;
        assert_eq!(names(&page), ["CP:a-", "b", "x/a", "x/b/1", "x/bz", "x/c"]);
        assert_eq!(listers, [start(None), start(Some("a-\u{10FFFF}"))]);
        let (page, listers) = ordered_page(&op, &q("/b"), 0).await;
        assert_eq!(
            names(&page),
            ["a-1", "a-2", "a-3", "b", "x/a", "CP:x/b", "x/c"]
        );
        assert_eq!(listers, [start(None), start(Some("x/b\u{10FFFF}"))]);
    }

    #[tokio::test]
    async fn a_small_common_prefix_is_read_through() {
        // A seek is one more list request, while the rest of a small group usually comes with
        // the page the backend already sent. Seeking past every group would turn one page of a
        // bucket with a thousand small folders into a thousand list requests in a row.
        let op = memory_bucket(["a/1", "a/2", "a/3", "b/1", "c"]).await;
        let (page, listers) = ordered_page(&op, &query("", true, None, 1000), SEEK_AFTER).await;
        assert_eq!(names(&page), ["CP:a/", "CP:b/", "c"]);
        assert_eq!(listers, [start(None)]);
    }

    #[tokio::test]
    async fn resuming_after_a_common_prefix_starts_past_its_group() {
        // The token after a page that ends in `a/` must not make the next page read all of
        // `a/` again only to drop it; neither must a marker inside `a/`, which skips the group.
        let op = memory_bucket(["a/1", "a/2", "b"]).await;
        for marker in ["a/", "a/1"] {
            let q = query("", true, Some(marker), 1000);
            let (page, listers) = ordered_page(&op, &q, SEEK_AFTER).await;
            assert_eq!(names(&page), ["b"], "{marker}");
            assert_eq!(listers, [start(Some("a/\u{10FFFF}"))], "{marker}");
        }
        // Without a delimiter there is no group: the marker resumes inside the directory.
        let q = query("", false, Some("a/1"), 1000);
        let (page, listers) = ordered_page(&op, &q, SEEK_AFTER).await;
        assert_eq!(names(&page), ["a/2", "b"]);
        assert_eq!(listers, [start(Some("a/1"))]);
    }

    #[tokio::test]
    async fn every_seek_lands_past_what_was_read() {
        // Keys may hold U+10FFFF, which sorts at or after where a seek lands. A seek that did
        // not pass the key it was made from would list the same group forever.
        let m = char::MAX;
        let op = memory_bucket([
            format!("a/{m}"),
            format!("a/{m}{m}"),
            format!("a/{m}{m}x"),
            "b".into(),
        ])
        .await;
        let (page, listers) = ordered_page(&op, &query("", true, None, 1000), 0).await;
        assert_eq!(names(&page), ["CP:a/", "b"]);
        assert_eq!(
            listers,
            [
                start(None),
                start(Some(&format!("a/{m}{m}"))),
                start(Some(&format!("a/{m}{m}{m}"))),
            ]
        );
    }

    /// xorshift64: deterministic without a dependency; each seed below is a test case.
    struct Rng(u64);

    impl Rng {
        fn below(&mut self, n: usize) -> usize {
            let mut x = self.0;
            x ^= x << 13;
            x ^= x >> 7;
            x ^= x << 17;
            self.0 = x;
            (x % n as u64) as usize
        }

        fn pick<'a, T>(&mut self, items: &'a [T]) -> &'a T {
            &items[self.below(items.len())]
        }

        /// A random leading part of `s`, possibly all or none of it.
        fn cut<'s>(&mut self, s: &'s str) -> &'s str {
            let ends: Vec<usize> = s.char_indices().map(|(i, _)| i).chain([s.len()]).collect();
            &s[..*self.pick(&ends)]
        }

        fn path(&mut self, names: &[&str]) -> String {
            let depth = 1 + self.below(3);
            let segments: Vec<&str> = (0..depth).map(|_| *self.pick(names)).collect();
            segments.join("/")
        }

        /// `len` characters of `s` in a row, or all of `s` if it is shorter. With `around`, a
        /// byte offset in `s`, the run covers the character there.
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

    /// The listing by definition: keys under the prefix, each folded into its CommonPrefix,
    /// the prefix and the rest up to and including the delimiter's first occurrence after it,
    /// sorted bytewise and deduplicated, all up to the marker dropped.
    fn model(keys: &BTreeSet<String>, q: &Query) -> Vec<String> {
        let names: BTreeSet<(String, bool)> = keys
            .iter()
            .filter_map(|key| {
                let rest = key.strip_prefix(q.prefix.as_str())?;
                let end = q
                    .delimiter
                    .as_deref()
                    .and_then(|d| Some(rest.find(d)? + d.len()));
                Some(match end {
                    Some(end) => (format!("{}{}", q.prefix, &rest[..end]), true),
                    None => (key.clone(), false),
                })
            })
            .collect();
        names
            .into_iter()
            .filter(|(name, _)| q.is_after_start(name))
            .map(|(name, folded)| if folded { format!("CP:{name}") } else { name })
            .collect()
    }

    /// Which source reads bucket `b`: the ordered path, seeking past a group after this many
    /// of its entries, or the walk.
    #[derive(Debug, Clone, Copy)]
    enum Strategy {
        Ordered(usize),
        Walk,
    }

    /// Every page of `q` from `strategy`, followed through its markers.
    async fn all_pages(op: &Operator, mut q: Query, strategy: Strategy) -> Vec<String> {
        let mut all = Vec::new();
        loop {
            let page = match strategy {
                Strategy::Ordered(seek_after) => {
                    let (page, listers) = ordered_page(op, &q, seek_after).await;
                    let starts: Vec<&Option<String>> =
                        listers.iter().map(|(_, after)| after).collect();
                    assert!(
                        starts.is_sorted_by(|a, b| a < b),
                        "seeks went back: {starts:?}"
                    );
                    page
                }
                Strategy::Walk => {
                    let mut page = PageBuilder::new(&q);
                    if let Some(start) = prefix_dir(&q.prefix) {
                        walk(op, "b/", start, &mut page).await.unwrap();
                    }
                    take_listed();
                    page.finish()
                }
            };
            all.extend(names(&page));
            let Some(marker) = page.next_marker else {
                return all;
            };
            assert!(all.len() < 10_000, "pagination never ends: {q:?}");
            q.start_after = Some(marker);
        }
    }

    #[tokio::test]
    async fn pages_of_both_sources_match_a_brute_force_model() {
        // Seeking skips keys unread on the strength of `past_group` and the group bookkeeping,
        // and the walk prunes and folds whole directories; a mistake there drops or repeats
        // keys only for particular keys, delimiters and markers, which hand-written cases
        // miss. Delimiters are cut from the keys, so they occur: one character, and two or
        // three holding `/` or not. Names holding U+10FFFF lie past where seeks land. The memory
        // backend lists in key order and also serves the walk, one directory at a time.
        const NAMES: &[&str] = &[
            "a",
            "a-",
            "a.b",
            "a0",
            "b",
            "-",
            "0",
            "ø",
            "😀",
            "\u{10FFFF}",
            "x\u{10FFFF}",
        ];
        let strategies = [
            Strategy::Ordered(0),
            Strategy::Ordered(1),
            Strategy::Ordered(SEEK_AFTER),
            Strategy::Walk,
        ];
        for seed in [1_u64, 7, 42, 0x9E37_79B9_7F4A_7C15] {
            let mut rng = Rng(seed);
            let keys: BTreeSet<String> = (0..10 + rng.below(30)).map(|_| rng.path(NAMES)).collect();
            let op = memory_bucket(&keys).await;
            // Folder markers are never keys, and alone make no CommonPrefix.
            for _ in 0..rng.below(4) {
                let marker = format!("b/{}/", rng.path(NAMES));
                op.create_dir(&marker).await.unwrap();
            }

            let keys_in_order: Vec<&String> = keys.iter().collect();
            let some_key = *rng.pick(&keys_in_order);
            let deep_key = keys.iter().rfind(|k| k.contains('/')).unwrap_or(some_key);
            let dir_of = |key: &str| {
                key.rfind('/')
                    .map_or_else(String::new, |i| key[..=i].into())
            };
            let (slashed_len, other_len) = (2 + rng.below(2), 2 + rng.below(2));
            let other_key = *rng.pick(&keys_in_order);
            let delimiters = [
                None,
                Some("/".to_owned()),
                Some(rng.run(some_key, 1, None)),
                Some(rng.run(deep_key, slashed_len, deep_key.find('/'))),
                Some(rng.run(other_key, other_len, None)),
            ];
            let some_prefixes = [
                String::new(),
                rng.cut(some_key).to_owned(),
                dir_of(deep_key),
            ];
            let another_key = *rng.pick(&keys_in_order);
            let some_markers = [
                None,
                Some(rng.cut(another_key).to_owned()),
                Some((*rng.pick(&keys_in_order)).clone()),
                Some(dir_of(deep_key)).filter(|dir| !dir.is_empty()),
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
                    let mut q = Query {
                        prefix: prefix.clone(),
                        delimiter: delimiter.clone(),
                        start_after: None,
                        max_keys: 1000,
                    };
                    // Markers at a CommonPrefix and at the last key inside it.
                    let groups: Vec<String> = model(&keys, &q)
                        .into_iter()
                        .filter_map(|name| name.strip_prefix("CP:").map(str::to_owned))
                        .collect();
                    let mut markers = some_markers.to_vec();
                    if !groups.is_empty() {
                        let group = rng.pick(&groups).clone();
                        markers.push(keys.iter().rfind(|k| k.starts_with(&group)).cloned());
                        markers.push(Some(group));
                    }
                    for marker in &markers {
                        for max_keys in [1, 3, 1000] {
                            q.start_after = marker.clone();
                            q.max_keys = max_keys;
                            let expected = model(&keys, &q);
                            for strategy in strategies {
                                assert_eq!(
                                    all_pages(&op, q.clone(), strategy).await,
                                    expected,
                                    "seed {seed}, {strategy:?}, {q:?}, keys {keys:?}"
                                );
                            }
                        }
                    }
                }
            }
        }
    }
}
