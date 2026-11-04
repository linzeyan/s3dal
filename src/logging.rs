//! The server's log: levels that a reload can change, and a file that rotates by size.

use std::ffi::OsString;
use std::fs::{File, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::sync::{Mutex, OnceLock, PoisonError};

use tracing_subscriber::filter::LevelFilter;
use tracing_subscriber::fmt::MakeWriter;
use tracing_subscriber::{EnvFilter, Registry, reload};

/// The running filter, set once by [`init`]. Global so a reload can reach it without threading
/// a handle through every constructor; an embedder that never calls `init` gets a no-op.
static FILTER: OnceLock<reload::Handle<EnvFilter, Registry>> = OnceLock::new();

/// The filter for `--log-level`. s3s logs every error answer, a client's unsigned request
/// included, at ERROR, in two or three lines; the access log says the same in one line at the
/// right level. So s3s is off unless a directive names it: `info,s3s=debug` brings its lines
/// back.
///
/// Stricter than `EnvFilter`, which reads a bare word that is not a level as a target at TRACE
/// and `target=` as TRACE too: a typo such as `verbose` would turn the log off, and `s3dal=`
/// turn it all on, without an error. Here every directive names a real level.
pub fn filter(directives: &str) -> Result<EnvFilter, String> {
    let mut names_s3s = false;
    let list: Vec<&str> = directives
        .split(',')
        .map(str::trim)
        .filter(|d| !d.is_empty())
        .collect();
    for directive in &list {
        let (target, level) = directive.split_once('=').unwrap_or(("", directive));
        // LevelFilter reads "" as ERROR, so an empty level is refused here first.
        let level = level.trim();
        let parsed = Some(level)
            .filter(|l| !l.is_empty())
            .and_then(|l| l.parse::<LevelFilter>().ok());
        parsed.ok_or_else(|| {
            format!(
                "{directive:?}: {level:?} is not a level; use trace, debug, info, warn, error \
                 or off, alone or as target=level"
            )
        })?;
        let target = target.trim().split('[').next().unwrap_or_default();
        names_s3s |= target == "s3s" || target.starts_with("s3s::");
    }
    if list.is_empty() {
        return Err("no log level; use trace, debug, info, warn, error or off".into());
    }
    let mut full = list.join(",");
    if !names_s3s {
        full.push_str(",s3s=off");
    }
    EnvFilter::builder()
        .parse(&full)
        .map_err(|e| format!("{directives:?}: {e}"))
}

/// Remembers the reload handle of the filter the process logs through.
pub fn set_reload_handle(handle: reload::Handle<EnvFilter, Registry>) {
    let _ = FILTER.set(handle);
}

/// Applies new levels to the running log; the directives were checked when the settings were
/// read, so a failure here can only be a filter already gone.
pub fn set_level(directives: &str) {
    let (Some(handle), Ok(filter)) = (FILTER.get(), filter(directives)) else {
        return;
    };
    let _ = handle.reload(filter);
}

/// A log file that, once a line would take it past `max_size`, is renamed to `<file>.1`, the
/// older ones shifted to `.2` up to `.<max_files>`, and the oldest beyond that removed. Every
/// line is written whole to one file: tracing hands each event over as one buffer. A
/// `max_size` of 0 never rotates.
pub struct RotatingFile {
    state: Mutex<State>,
}

struct State {
    path: PathBuf,
    /// `None` after a rotation could not open the new file; the next line tries again.
    file: Option<File>,
    size: u64,
    max_size: u64,
    max_files: u32,
}

impl RotatingFile {
    /// Opens the file for appending, so a restart continues it. Fails if it cannot be opened,
    /// so the server does not start without the log it was told to keep.
    pub fn open(path: &Path, max_size: u64, max_files: u32) -> io::Result<Self> {
        let file = open_append(path)?;
        let size = file.metadata()?.len();
        Ok(Self {
            state: Mutex::new(State {
                path: path.to_owned(),
                file: Some(file),
                size,
                max_size,
                max_files,
            }),
        })
    }
}

fn open_append(path: &Path) -> io::Result<File> {
    OpenOptions::new().create(true).append(true).open(path)
}

fn numbered(path: &Path, n: u32) -> PathBuf {
    let mut name = OsString::from(path.as_os_str());
    name.push(format!(".{n}"));
    PathBuf::from(name)
}

impl State {
    fn rotate(&mut self) {
        self.file = None;
        if self.max_files == 0 {
            let _ = std::fs::remove_file(&self.path);
        } else {
            // Removed first: on Windows a rename does not replace an existing file.
            let _ = std::fs::remove_file(numbered(&self.path, self.max_files));
            for n in (1..self.max_files).rev() {
                let _ = std::fs::rename(numbered(&self.path, n), numbered(&self.path, n + 1));
            }
            let _ = std::fs::rename(&self.path, numbered(&self.path, 1));
        }
        self.reopen();
    }

    fn reopen(&mut self) {
        if let Ok(file) = open_append(&self.path) {
            self.size = file.metadata().map_or(0, |m| m.len());
            self.file = Some(file);
        }
    }

    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = u64::try_from(buf.len()).unwrap_or(u64::MAX);
        // A file still empty takes the line however long it is, or one line longer than the
        // limit would rotate on every write.
        if self.max_size > 0 && self.size > 0 && self.size.saturating_add(len) > self.max_size {
            self.rotate();
        }
        if self.file.is_none() {
            self.reopen();
        }
        let file = self
            .file
            .as_mut()
            .ok_or_else(|| io::Error::other("the log file could not be opened"))?;
        file.write_all(buf)?;
        self.size = self.size.saturating_add(len);
        Ok(buf.len())
    }
}

pub struct RotatingWriter<'a>(&'a RotatingFile);

impl Write for RotatingWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        self.0
            .state
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .write(buf)
    }

    fn flush(&mut self) -> io::Result<()> {
        Ok(())
    }
}

impl<'a> MakeWriter<'a> for RotatingFile {
    type Writer = RotatingWriter<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        RotatingWriter(self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn line(n: usize) -> Vec<u8> {
        let mut l = vec![b'x'; n - 1];
        l.push(b'\n');
        l
    }

    fn write(log: &RotatingFile, buf: &[u8]) {
        log.make_writer().write_all(buf).unwrap();
    }

    fn len(path: &Path) -> Option<u64> {
        std::fs::metadata(path).ok().map(|m| m.len())
    }

    #[test]
    fn a_line_that_would_pass_the_limit_starts_a_new_file() {
        // A log that grows without bound fills the disk the objects live on; rotating between
        // lines keeps every line whole in one file, so a collector never reads half of one.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s3dal.log");
        let log = RotatingFile::open(&path, 100, 3).unwrap();
        write(&log, &line(60));
        write(&log, &line(30));
        assert_eq!(len(&path), Some(90));
        write(&log, &line(20));
        assert_eq!(len(&numbered(&path, 1)), Some(90));
        assert_eq!(len(&path), Some(20));
    }

    #[test]
    fn only_max_files_rotated_files_are_kept() {
        // The bound on disk use is (max_files + 1) × max_size; a file kept beyond it would
        // make the bound a lie.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s3dal.log");
        let log = RotatingFile::open(&path, 10, 2).unwrap();
        for n in 1..=5 {
            write(&log, &line(6 + n));
        }
        // Newest first: the current file holds the fifth line, .1 the fourth, .2 the third.
        assert_eq!(len(&path), Some(11));
        assert_eq!(len(&numbered(&path, 1)), Some(10));
        assert_eq!(len(&numbered(&path, 2)), Some(9));
        assert_eq!(len(&numbered(&path, 3)), None);
    }

    #[test]
    fn zero_rotated_files_starts_the_file_over() {
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s3dal.log");
        let log = RotatingFile::open(&path, 10, 0).unwrap();
        write(&log, &line(8));
        write(&log, &line(8));
        assert_eq!(len(&path), Some(8));
        assert_eq!(len(&numbered(&path, 1)), None);
    }

    #[test]
    fn a_zero_max_size_never_rotates() {
        // `0` is how rotation is left to logrotate or the platform, which must find one file.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s3dal.log");
        let log = RotatingFile::open(&path, 0, 3).unwrap();
        for _ in 0..50 {
            write(&log, &line(100));
        }
        assert_eq!(len(&path), Some(5_000));
        assert_eq!(len(&numbered(&path, 1)), None);
    }

    #[test]
    fn a_restart_continues_the_file_and_its_size() {
        // Counted from zero again, a server restarted often would let the file grow past the
        // limit before it ever rotated.
        let dir = tempfile::tempdir().unwrap();
        let path = dir.path().join("s3dal.log");
        write(&RotatingFile::open(&path, 100, 1).unwrap(), &line(80));
        let log = RotatingFile::open(&path, 100, 1).unwrap();
        write(&log, &line(30));
        assert_eq!(len(&numbered(&path, 1)), Some(80));
        assert_eq!(len(&path), Some(30));
    }

    #[test]
    fn s3s_is_off_unless_a_directive_names_it() {
        // s3s logs each refused request at ERROR; on by default, a scanner's unsigned probes
        // would fill the log with errors the access log already reports at INFO.
        let shown = |d: &str| filter(d).unwrap().to_string();
        assert!(shown("debug").contains("s3s=off"));
        assert!(shown("info,s3dal=debug").contains("s3s=off"));
        assert!(!shown("info,s3s=debug").contains("s3s=off"));
        assert!(!shown("info,s3s::ops=debug").contains("s3s=off"));
        // Another crate whose name starts the same is not s3s.
        assert!(shown("info,s3s_test=debug").contains("s3s=off"));
    }

    #[test]
    fn a_bare_word_must_be_a_level() {
        // EnvFilter would read `verbose` as a target at TRACE and turn the rest of the log
        // off, and `s3dal=` as TRACE, so the operator would never learn of the typo.
        for bad in ["verbose", "info,dbug", "", " , ", "info,s3dal="] {
            assert!(filter(bad).is_err(), "{bad:?} accepted");
        }
        for good in [
            "debug",
            "OFF",
            "warn,s3dal::server=info",
            "info, s3dal=trace",
        ] {
            assert!(filter(good).is_ok(), "{good:?} refused");
        }
        assert!(filter("s3dal=loud").is_err());
    }
}
