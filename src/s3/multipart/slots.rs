//! Where parts go on fs so that Complete can take them without copying. Each upload directory
//! holds one file, [`OBJECT_FILE`], and part `n` is written into it at `(n - 1) * size`, where
//! `size` is the part size the first large enough part fixed. Clients upload parts of one size,
//! the last one shorter, so their parts lie in that file where the object needs them, and
//! Complete renames it into place: on ext4, which shares no extents, the copy wrote every byte
//! a second time. A part that cannot take its slot goes to a file of its own, as every part did
//! before, and an upload whose parts do not fill the file exactly is copied as before.
//!
//! A manifest is the commit point of a part (see `record`), so the data it names must never
//! change. Only the upload of a part number that claims its slot writes it: a later one, or
//! one racing it, would overwrite data a committed manifest names, or interleave with it. The
//! claim is given up only when that upload fails, before any manifest names the slot, so that
//! a retry of the part can take it. Part writers
//! hold a shared lock on the file and Complete an exclusive one; neither waits for the other,
//! as a part that finds the file taken goes to a file of its own, and a Complete that finds a
//! part being written copies.

use std::collections::HashMap;
use std::fs::{File, OpenOptions, TryLockError};
use std::io;
use std::os::unix::fs::MetadataExt;
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use opendal::raw::Timestamp;
use opendal::{Metadata, MetadataBuilder};
use uuid::Uuid;

use super::complete::MIN_PART_SIZE;
use super::record::PartManifest;

/// The file in an upload's directory that the parts are written into.
pub(super) const OBJECT_FILE: &str = "object";

/// The part size the slots assume, in decimal.
const PART_SIZE_FILE: &str = "object.part-size";

/// S3's largest object; no slot reaches past it.
const MAX_OBJECT_SIZE: u64 = 5 * 1024 * 1024 * 1024 * 1024;

/// A part's place in [`OBJECT_FILE`], locked against Complete taking the file.
pub(super) struct Slot {
    pub file: File,
    pub offset: u64,
    /// Keeps every other upload of the part out of the slot while it exists.
    pub claim: PathBuf,
}

/// Opens the slot of part `part`, a body of `len` bytes, in the upload directory `dir`; `None`
/// when the part goes to a file of its own.
pub(super) fn open(dir: &Path, part: u32, len: u64) -> io::Result<Option<Slot>> {
    match try_open(dir, part, len) {
        // A Complete took the file, or the upload is gone: an Abort, or a sweep. The part takes
        // the path it took before slots, which in the latter case fails or leaves what the
        // sweep removes.
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(None),
        opened => opened,
    }
}

fn try_open(dir: &Path, part: u32, len: u64) -> io::Result<Option<Slot>> {
    let Some(size) = part_size(dir, len)? else {
        return Ok(None);
    };
    let Some(offset) = offset(part, len, size) else {
        return Ok(None);
    };
    let claim = dir.join(format!("slot-{part:05}"));
    match OpenOptions::new().write(true).create_new(true).open(&claim) {
        Ok(_) => {}
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => return Ok(None),
        Err(err) => return Err(err),
    }
    // Never created here: once a Complete has taken the file, one made again would hold only
    // the parts written after, and a Complete copying from it would read zeros for the rest.
    let path = dir.join(OBJECT_FILE);
    let file = OpenOptions::new().write(true).open(&path)?;
    match file.try_lock_shared() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => return Ok(None),
        Err(TryLockError::Error(err)) => return Err(err),
    }
    // Opened before a Complete renamed the file into place, and locked once it let go: the
    // file is an object now.
    if !still_named(&file, &path)? {
        return Ok(None);
    }
    Ok(Some(Slot {
        file,
        offset,
        claim,
    }))
}

/// The part size the slots of the upload in `dir` assume: the one recorded, or `len` if this
/// body is the first large enough to fix it. A smaller one can only be the last part, so it
/// fixes nothing. `None` while it is unknown.
///
/// The record is written in a file of its own and linked into place, so no part reads it half
/// written: a prefix of a size is a smaller size, and would place a part in another's slot.
/// [`OBJECT_FILE`] is made before, and only here, so it exists once a size does.
fn part_size(dir: &Path, len: u64) -> io::Result<Option<u64>> {
    let path = dir.join(PART_SIZE_FILE);
    match std::fs::read_to_string(&path) {
        Ok(recorded) => return Ok(recorded.parse().ok()),
        Err(err) if err.kind() == io::ErrorKind::NotFound => {}
        Err(err) => return Err(err),
    }
    if len < MIN_PART_SIZE {
        return Ok(None);
    }
    let draft = dir.join(format!("{PART_SIZE_FILE}.{}", Uuid::new_v4().hyphenated()));
    std::fs::write(&draft, len.to_string())?;
    // A part that won the link may already be writing into it.
    let linked = OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(false)
        .open(dir.join(OBJECT_FILE))
        .and_then(|_| std::fs::hard_link(&draft, &path));
    let _ = std::fs::remove_file(&draft);
    match linked {
        Ok(()) => Ok(Some(len)),
        Err(err) if err.kind() == io::ErrorKind::AlreadyExists => {
            Ok(std::fs::read_to_string(&path)?.parse().ok())
        }
        Err(err) => Err(err),
    }
}

/// Where part `part`, `len` bytes long, starts when parts are `size` bytes; `None` when it does
/// not fit its slot.
fn offset(part: u32, len: u64, size: u64) -> Option<u64> {
    if len > size {
        return None;
    }
    let at = u64::from(part.checked_sub(1)?).checked_mul(size)?;
    (at.checked_add(len)? <= MAX_OBJECT_SIZE).then_some(at)
}

/// Whether `path` still names `file`: not once a Complete renamed it away, or after another
/// file took the name.
fn still_named(file: &File, path: &Path) -> io::Result<bool> {
    let opened = file.metadata()?;
    match std::fs::symlink_metadata(path) {
        Ok(named) => Ok((named.dev(), named.ino()) == (opened.dev(), opened.ino())),
        Err(err) if err.kind() == io::ErrorKind::NotFound => Ok(false),
        Err(err) => Err(err),
    }
}

/// Whether `parts`, in the order Complete lists them, lie in [`OBJECT_FILE`] exactly where the
/// object needs them: each in its slot, one right after the other from the start.
pub(super) fn fill(parts: &[PartManifest]) -> bool {
    let mut at = 0;
    parts.iter().all(|part| {
        let placed = part.data == OBJECT_FILE && part.offset == at;
        at += part.size;
        placed
    })
}

/// Takes [`OBJECT_FILE`] in `dir` for an object of `total` bytes, whose parts [`fill`] it: locked
/// so that no part is written into it any more, with `user_metadata` as its `user.` xattrs, its
/// mtime the time of this Complete, and synced. Returns it with what OpenDAL's fs writer reports
/// on close. `None` when a part is being written into it, or it holds more than the parts:
/// then it is left as it was, and the parts are copied.
///
/// Nothing here changes what a later Complete of the upload needs: a Complete that fails after
/// this (its condition, the rename) leaves the file where it was, and the next one sets the
/// metadata afresh.
pub(super) fn take(
    dir: &Path,
    total: u64,
    user_metadata: &HashMap<String, String>,
) -> io::Result<Option<(File, Metadata)>> {
    use xattr::FileExt;

    let path = dir.join(OBJECT_FILE);
    let file = match OpenOptions::new().write(true).open(&path) {
        Ok(file) => file,
        Err(err) if err.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(err) => return Err(err),
    };
    match file.try_lock() {
        Ok(()) => {}
        Err(TryLockError::WouldBlock) => return Ok(None),
        Err(TryLockError::Error(err)) => return Err(err),
    }
    // A part past the last one listed, written or being written before this lock, makes the
    // file longer than the object.
    if !still_named(&file, &path)? || file.metadata()?.len() != total {
        return Ok(None);
    }
    for name in file.list_xattr()? {
        if name.as_encoded_bytes().starts_with(b"user.") {
            file.remove_xattr(&name)?;
        }
    }
    for (key, value) in user_metadata {
        file.set_xattr(format!("user.{key}"), value.as_bytes())?;
    }
    // The parts were written as they arrived; like a copied object, this one dates from its
    // Complete.
    file.set_modified(SystemTime::now())?;
    file.sync_all()?;
    let written = file.metadata()?;
    let mut m = MetadataBuilder::file(written.len());
    m.last_modified(Timestamp::try_from(written.modified()?).map_err(io::Error::other)?);
    Ok(Some((file, m.build())))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_slot_is_placed_only_where_the_part_fits() {
        const MIB: u64 = 1024 * 1024;
        assert_eq!(offset(1, 8 * MIB, 8 * MIB), Some(0));
        assert_eq!(offset(3, MIB, 8 * MIB), Some(16 * MIB));
        // Longer than the slot, it would run into the next part's.
        assert_eq!(offset(2, 8 * MIB + 1, 8 * MIB), None);
        // Past S3's largest object no Complete could use it, and ext4 refuses files past
        // 16 TiB: 10,000 slots of 5 GiB would reach 48.8 TiB.
        assert_eq!(offset(10_000, 5 << 30, 5 << 30), None);
        assert_eq!(offset(1_024, 5 << 30, 5 << 30), Some(1_023 * (5 << 30)));
        assert_eq!(offset(0, 1, 1), None);
    }

    #[test]
    fn the_first_full_part_fixes_the_slots_and_each_slot_is_written_once() {
        // A short part may be the last one, so it cannot tell where the others go. A second
        // upload of a part number must not write over data a committed manifest names, nor
        // into a part still being written.
        let dir = tempfile::tempdir().unwrap();
        let slot = |part, len| open(dir.path(), part, len).unwrap().map(|s| s.offset);
        assert_eq!(slot(3, 10), None);
        assert!(!dir.path().join(OBJECT_FILE).exists());
        assert_eq!(slot(1, MIN_PART_SIZE), Some(0));
        assert_eq!(slot(3, 10), Some(2 * MIN_PART_SIZE));
        assert_eq!(slot(1, MIN_PART_SIZE), None);
        assert_eq!(slot(2, MIN_PART_SIZE + 1), None);
    }

    #[test]
    fn a_taken_file_takes_no_more_parts() {
        // A part arriving after a Complete renamed the file into place would otherwise make it
        // again, with only that part in it; a retried Complete copying from it would read
        // zeros where the earlier parts were.
        let dir = tempfile::tempdir().unwrap();
        assert!(open(dir.path(), 1, MIN_PART_SIZE).unwrap().is_some());
        std::fs::rename(dir.path().join(OBJECT_FILE), dir.path().join("taken")).unwrap();
        assert!(open(dir.path(), 2, MIN_PART_SIZE).unwrap().is_none());
        assert!(!dir.path().join(OBJECT_FILE).exists());
    }

    #[test]
    fn a_part_and_a_complete_never_share_the_file() {
        // Complete renames the file into place: a part written into it then would change an
        // object, and one written while Complete copies from it would race the copy. Neither
        // waits for the other; whoever finds the file in use goes its own way.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(OBJECT_FILE);
        let first = open(dir.path(), 1, MIN_PART_SIZE).unwrap().unwrap();
        first.file.set_len(MIN_PART_SIZE).unwrap();
        let none = HashMap::new();
        assert!(take(dir.path(), MIN_PART_SIZE, &none).unwrap().is_none());
        drop(first);

        let completing = File::open(&path).unwrap();
        completing.lock().unwrap();
        assert!(open(dir.path(), 2, MIN_PART_SIZE).unwrap().is_none());
        drop(completing);

        let (taken, m) = take(dir.path(), MIN_PART_SIZE, &none).unwrap().unwrap();
        assert_eq!(m.content_length(), MIN_PART_SIZE);
        // Longer than the parts listed, it holds a part the object must not.
        drop(taken);
        assert!(
            take(dir.path(), MIN_PART_SIZE - 1, &none)
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn a_file_renamed_away_is_no_longer_the_upload_s() {
        // A part writer that opened the file before a Complete renamed it into place must not
        // write into what is an object by then.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join(OBJECT_FILE);
        let file = File::create(&path).unwrap();
        assert!(still_named(&file, &path).unwrap());
        std::fs::rename(&path, dir.path().join("taken")).unwrap();
        assert!(!still_named(&file, &path).unwrap());
        File::create(&path).unwrap();
        assert!(!still_named(&file, &path).unwrap());
    }
}
