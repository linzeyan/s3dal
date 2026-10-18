use std::collections::HashMap;
use std::path::Path;

use anyhow::{Context, bail};
use futures::StreamExt;
use opendal::Operator;
use opendal::layers::RetryLayer;
use opendal::options::{DeleteOptions, ListOptions, WriteOptions};
use tokio::runtime::RuntimeFlavor;

use crate::meta;

/// Directory at the namespace root for s3dal's own data. Valid bucket names start with a
/// lowercase letter or digit, so this can never collide with a bucket.
pub const RESERVED_ROOT: &str = ".s3dal/";

const PROBE_META_KEY: &str = "s3dal-probe";
/// A metadata name S3 allows, with the characters some backends refuse in names: `-`, which
/// every reserved key holds, `.`, `_` and a leading digit. Lowercase, because most backends
/// take names as HTTP headers, which lowercase them anyway.
const PROBE_META_NAME: &str = "0s3dal-probe.name_";
const PROBE_CONTENT_TYPE: &str = "application/x-s3dal-probe";
const PROBE_CACHE_CONTROL: &str = "max-age=4711";
const PROBE_CONTENT_DISPOSITION: &str = "attachment; filename=\"s3dal-probe\"";
const PROBE_CONTENT_ENCODING: &str = "gzip";
/// `s3dal probe`, gzipped. A real gzip body, because GCS decompresses gzip objects for clients
/// that do not ask for gzip, which only a gzip body reveals.
const PROBE_BODY: &[u8] = &[
    0x1f, 0x8b, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0xff, 0x2b, 0x36, 0x4e, 0x49, 0xcc, 0x51,
    0x28, 0x28, 0xca, 0x4f, 0x4a, 0x05, 0x00, 0x5e, 0x6b, 0x1f, 0x3d, 0x0b, 0x00, 0x00, 0x00,
];

/// What the backend actually does, measured once at startup.
///
/// Capabilities alone are not enough: a backend can accept user metadata and drop it (fs on a
/// filesystem without xattrs), and no capability says whether `stat` returns an ETag or whether
/// directories outlive their last child. Each field picks one code path for the whole process,
/// so a field never has two sources of truth.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Profile {
    /// `stat` returns the MD5 of what one write stored as its ETag, so the backend's ETag is
    /// what S3 would report and is authoritative. An ETag of another kind (Azure's and GCS's
    /// are opaque version tags) would break clients that check it against the content.
    pub native_etag: bool,
    /// Content-Type set at write time comes back from `stat`.
    pub native_content_type: bool,
    /// The same, for Cache-Control.
    pub native_cache_control: bool,
    /// The same, for Content-Disposition.
    pub native_content_disposition: bool,
    /// The same, for Content-Encoding, and the content reads back as written. GCS stores a
    /// gzip encoding but then serves the content decompressed, which S3 never does.
    pub native_content_encoding: bool,
    /// The backend refuses or alters metadata names that S3 allows, so every name is stored
    /// in a form it keeps (`meta::encode_name`). Azure takes only C# identifiers as names.
    pub encoded_metadata_names: bool,
    /// A recursive listing returns keys in key order and resumes strictly after a given key.
    /// Measured, because the `list_with_start_after` capability only says the backend accepts
    /// the option: OpenDAL's onedrive declares it too, and nothing promises its order.
    pub ordered_list: bool,
    /// Directories persist after their last child is deleted, so s3dal must prune them.
    pub real_dirs: bool,
    /// The backend refuses a write whose condition no longer holds where the write commits: a
    /// create over a key that exists, and a replace of a version another write has replaced.
    /// Conditional writes then carry their condition to the backend, so they hold against other
    /// processes sharing it, not only within this one. Measured, because S3-compatible services
    /// differ, and one that ignores the condition would still accept the write.
    pub conditional_writes: bool,
}

/// Builds an operator from an OpenDAL scheme and its options.
pub fn build_operator(scheme: &str, opts: &[(String, String)]) -> anyhow::Result<Operator> {
    // Registers the services and, for remote ones, the process-wide HTTP transport; without
    // `auto-register-services` neither happens on its own.
    opendal::install_default();
    let mut opts = opts.to_vec();
    if scheme == "fs" {
        ensure_fs_atomic_write_dir(&mut opts)?;
    }
    let op = Operator::via_iter(scheme, opts)
        .with_context(|| format!("cannot build OpenDAL operator for scheme {scheme:?}"))?;
    // Not fs: OpenDAL marks every local io error it does not classify as temporary, a
    // directory not empty and a name too long among them, so retries would only hold back
    // their answer by about 8 seconds (pruning a folder that still holds keys, on every
    // DELETE in it).
    if scheme == "fs" {
        return Ok(op);
    }
    // Remote services fail transiently (throttling, dropped connections) and OpenDAL does not
    // retry on its own. Retrying here repeats only the failed backend call, such as one part
    // upload or a read resumed at its offset; left to the S3 client, the same blip resends a
    // whole streamed PUT or restarts a GET mid-body. Jitter keeps concurrent requests from
    // retrying in lockstep.
    Ok(op.layer(RetryLayer::new().with_jitter()))
}

/// OpenDAL's fs writer writes in place unless `atomic_write_dir` is set, which would expose
/// partially uploaded objects to readers and leave them behind on failed uploads.
fn ensure_fs_atomic_write_dir(opts: &mut Vec<(String, String)>) -> anyhow::Result<()> {
    if opts.iter().any(|(k, _)| k == "atomic_write_dir") {
        return Ok(());
    }
    let root = opts
        .iter()
        .find(|(k, _)| k == "root")
        .map(|(_, v)| v.clone())
        .context("the fs backend requires `--backend-opt root=<dir>`")?;
    // Inside the root so the final rename never crosses filesystems.
    let dir = Path::new(&root).join(".s3dal").join("tmp");
    std::fs::create_dir_all(&dir).with_context(|| format!("cannot create {}", dir.display()))?;
    opts.push((
        "atomic_write_dir".to_owned(),
        dir.to_string_lossy().into_owned(),
    ));
    Ok(())
}

/// Runs `fut`, a write that ends in a writer's close, so that on fs it holds up no other
/// request. OpenDAL's fs writer syncs the file and sets its xattrs in blocking calls on
/// whichever thread polls the close (`PositionWrite::close`, opendal-service-fs 0.59.4). On a
/// runtime worker each fsync held the worker for its whole length: with one worker per core,
/// only that many commits reached the disk at a time, and every other request on those workers
/// waited. 10,000 1 KiB PUTs took 55 s on a disk that commits them in 7. `block_in_place` hands
/// the worker's other tasks to another thread first. A current-thread runtime (a test's, or an
/// embedder's) has no other thread to hand them to, so there the write runs as before.
pub(crate) async fn without_stalling<F: Future>(op: &Operator, fut: F) -> F::Output {
    let handle = tokio::runtime::Handle::current();
    if op.info().scheme() != "fs" || handle.runtime_flavor() != RuntimeFlavor::MultiThread {
        return fut.await;
    }
    tokio::task::block_in_place(|| handle.block_on(fut))
}

/// Measures the backend and refuses to run on one that cannot hold s3dal's metadata.
pub async fn probe(op: &Operator) -> anyhow::Result<Profile> {
    let info = op.info();
    let cap = info.capability();
    if !cap.write_with_user_metadata {
        bail!(
            "backend {:?} cannot store user metadata (capability `write_with_user_metadata`); \
             s3dal keeps x-amz-meta-* and its own object fields there",
            info.scheme()
        );
    }

    let dir = format!("{RESERVED_ROOT}probe/{}/", uuid::Uuid::new_v4());
    let result = probe_in(op, &dir).await;
    let cleanup = DeleteOptions {
        recursive: true,
        ..Default::default()
    };
    if let Err(err) = op.delete_options(&dir, cleanup).await {
        tracing::warn!(%dir, error = %err, "cannot remove probe directory");
    }
    result
}

async fn probe_in(op: &Operator, dir: &str) -> anyhow::Result<Profile> {
    let cap = op.info().capability();

    let encoded_metadata_names = !keeps_metadata_name(op, &format!("{dir}names")).await?;
    // Written in the form s3dal will store names in, so the check below also proves that the
    // encoded form survives.
    let meta_key = if encoded_metadata_names {
        meta::encode_name(PROBE_META_KEY)
    } else {
        PROBE_META_KEY.to_owned()
    };

    let object = format!("{dir}object");
    let mut opts = WriteOptions {
        user_metadata: Some(HashMap::from([(meta_key.clone(), "1".to_owned())])),
        ..Default::default()
    };
    if cap.write_with_content_type {
        opts.content_type = Some(PROBE_CONTENT_TYPE.to_owned());
    }
    if cap.write_with_cache_control {
        opts.cache_control = Some(PROBE_CACHE_CONTROL.to_owned());
    }
    if cap.write_with_content_disposition {
        opts.content_disposition = Some(PROBE_CONTENT_DISPOSITION.to_owned());
    }
    if cap.write_with_content_encoding {
        opts.content_encoding = Some(PROBE_CONTENT_ENCODING.to_owned());
    }
    op.write_options(&object, PROBE_BODY.to_vec(), opts)
        .await
        .context("probe: cannot write")?;
    let meta = op.stat(&object).await.context("probe: cannot stat")?;

    let kept_metadata = meta.user_metadata().and_then(|m| m.get(&meta_key)) == Some("1");
    if !kept_metadata {
        bail!(
            "backend accepted user metadata but did not return it; \
             for fs, use a filesystem with user xattr support"
        );
    }

    let child = format!("{dir}d/f");
    op.write(&child, b"x".to_vec())
        .await
        .context("probe: cannot write nested object")?;
    op.delete(&child)
        .await
        .context("probe: cannot delete nested object")?;
    let parent = format!("{dir}d/");
    let entries = op.list(dir).await.context("probe: cannot list")?;
    let real_dirs = entries.iter().any(|e| e.path() == parent);

    let ordered_list = cap.list_with_start_after
        && cap.list_with_recursive
        && lists_in_key_order(op, &format!("{dir}order/")).await?;

    let native_content_encoding = cap.write_with_content_encoding
        && meta.content_encoding() == Some(PROBE_CONTENT_ENCODING)
        && reads_back_unchanged(op, &object).await?;

    let conditional_writes = honors_write_conditions(op, &format!("{dir}conditions/")).await?;

    Ok(Profile {
        native_etag: meta.etag().map(meta::unquote) == Some(meta::md5_hex(PROBE_BODY)),
        native_content_type: cap.write_with_content_type
            && meta.content_type() == Some(PROBE_CONTENT_TYPE),
        native_cache_control: cap.write_with_cache_control
            && meta.cache_control() == Some(PROBE_CACHE_CONTROL),
        native_content_disposition: cap.write_with_content_disposition
            && meta.content_disposition() == Some(PROBE_CONTENT_DISPOSITION),
        native_content_encoding,
        encoded_metadata_names,
        ordered_list,
        real_dirs,
        conditional_writes,
    })
}

/// Whether a metadata name S3 allows comes back as written. A backend may refuse it (Azure
/// answers 400 InvalidMetadata) or keep it under another name; either way names must be
/// encoded. An error alone cannot tell a refusal from an outage, since the retry layer marks
/// every error it returns persistent, retried or not. So a refusal counts only when a name in
/// the encoded form then goes through and the name is refused again. Taking an outage for a
/// refusal would encode names for good on a backend that keeps them, and the objects stored
/// before would lose their reserved fields.
async fn keeps_metadata_name(op: &Operator, path: &str) -> anyhow::Result<bool> {
    let write = |name: String| {
        let opts = WriteOptions {
            user_metadata: Some(HashMap::from([(name, "1".to_owned())])),
            ..Default::default()
        };
        op.write_options(path, b"x".to_vec(), opts)
    };
    if write(PROBE_META_NAME.to_owned()).await.is_err() {
        if let Err(err) = write(meta::encode_name(PROBE_META_KEY)).await {
            // Either an outage, or a backend that stores no user metadata at all, which its
            // capability does not tell: fs claims it, and a filesystem without user xattrs
            // answers ENOTSUP. A write without metadata tells the two apart, so the refusal says
            // what is missing instead of only that a write failed.
            op.write(path, b"x".to_vec())
                .await
                .context("probe: cannot write")?;
            return Err(anyhow::Error::new(err).context(
                "backend refuses to store user metadata, where s3dal keeps x-amz-meta-* and \
                 its own object fields; for fs, use a filesystem with user xattr support",
            ));
        }
        if let Err(err) = write(PROBE_META_NAME.to_owned()).await {
            tracing::info!(
                name = PROBE_META_NAME,
                error = %err,
                "backend refuses metadata names that S3 allows; storing names encoded"
            );
            return Ok(false);
        }
    }
    let meta = op.stat(path).await.context("probe: cannot stat")?;
    let kept = meta.user_metadata().and_then(|m| m.get(PROBE_META_NAME)) == Some("1");
    if !kept {
        tracing::info!(
            name = PROBE_META_NAME,
            returned = ?meta.user_metadata(),
            "backend alters metadata names that S3 allows; storing names encoded"
        );
    }
    Ok(kept)
}

/// Whether `path`, the probe object, reads back as [`PROBE_BODY`] when read as GetObject reads,
/// with a range. GCS ignores the range of an object it decompresses and serves the whole
/// decompressed content, of another length. Decided by the first bytes that differ, without
/// reading on, since the length that follows need not fit the range.
async fn reads_back_unchanged(op: &Operator, path: &str) -> anyhow::Result<bool> {
    let reader = op.reader(path).await.context("probe: cannot read")?;
    let range = 0..PROBE_BODY.len() as u64;
    let mut chunks = reader
        .into_stream(range)
        .await
        .context("probe: cannot read")?;
    let mut body = Vec::new();
    while let Some(chunk) = chunks.next().await {
        body.extend_from_slice(&chunk.context("probe: cannot read")?.to_bytes());
        if !PROBE_BODY.starts_with(&body) {
            break;
        }
    }
    let unchanged = body == PROBE_BODY;
    if !unchanged {
        tracing::info!(
            "backend changes gzip content on the way out; storing Content-Encoding itself"
        );
    }
    Ok(unchanged)
}

/// Whether the backend checks write conditions itself (see [`Profile::conditional_writes`]),
/// measured in `dir`. Plain writes come first and must succeed, so a failing backend stops the
/// probe rather than passing for one without conditions. A conditional write that fails other
/// than by its condition, or succeeds where it should not, means the backend cannot be trusted
/// with them; s3dal then checks conditions within the process only, as it does on fs.
async fn honors_write_conditions(op: &Operator, dir: &str) -> anyhow::Result<bool> {
    let cap = op.info().capability();
    if !cap.write_with_if_not_exists
        || !(cap.write_with_if_match || cap.write_with_if_version_match)
    {
        return Ok(false);
    }
    let (path, created) = (format!("{dir}replaced"), format!("{dir}created"));
    op.write(&path, b"1".to_vec())
        .await
        .context("probe: cannot write")?;
    let replaced = op.stat(&path).await.context("probe: cannot stat")?;
    op.write(&path, b"2".to_vec())
        .await
        .context("probe: cannot write")?;
    let current = op.stat(&path).await.context("probe: cannot stat")?;

    let refused = |result: opendal::Result<opendal::Metadata>| matches!(result, Err(err) if err.kind() == opendal::ErrorKind::ConditionNotMatch);
    let create = || WriteOptions {
        if_not_exists: true,
        ..Default::default()
    };
    let unchanged_since = |meta: &opendal::Metadata| WriteOptions {
        if_not_changed: Some(meta.clone()),
        ..Default::default()
    };
    let steps = [
        (
            "create over an existing key",
            refused(op.write_options(&path, b"3".to_vec(), create()).await),
        ),
        (
            "replace of a replaced version",
            refused(
                op.write_options(&path, b"4".to_vec(), unchanged_since(&replaced))
                    .await,
            ),
        ),
        (
            "replace of the current version",
            op.write_options(&path, b"5".to_vec(), unchanged_since(&current))
                .await
                .is_ok(),
        ),
        (
            "create of a new key",
            op.write_options(&created, b"6".to_vec(), create())
                .await
                .is_ok(),
        ),
    ];
    let failed: Vec<&str> = steps
        .iter()
        .filter(|(_, ok)| !ok)
        .map(|(step, _)| *step)
        .collect();
    if !failed.is_empty() {
        tracing::warn!(
            ?failed,
            "backend does not check write conditions as it claims; conditional writes hold \
             within this process only"
        );
    }
    Ok(failed.is_empty())
}

/// Whether a recursive listing of `dir` after its first key returns exactly the keys after it,
/// in key order. The keys are written in neither key order nor directories-first order, and
/// `a-` and `a.txt` sort before `a/c` (`-` 0x2D, `.` 0x2E, `/` 0x2F), so a backend that lists
/// in creation order, puts directories first, or compares path segments fails, as does one
/// that ignores or includes the start-after key.
async fn lists_in_key_order(op: &Operator, dir: &str) -> anyhow::Result<bool> {
    const KEYS: [&str; 4] = ["b", "a/c", "a.txt", "a-"];
    for key in KEYS {
        op.write(&format!("{dir}{key}"), b"x".to_vec())
            .await
            .context("probe: cannot write ordering objects")?;
    }
    let mut keys = KEYS.map(|key| format!("{dir}{key}"));
    keys.sort_unstable();
    let opts = ListOptions {
        recursive: true,
        start_after: Some(keys[0].clone()),
        ..Default::default()
    };
    let listed: Vec<String> = op
        .list_options(dir, opts)
        .await
        .context("probe: cannot list after a key")?
        .into_iter()
        .filter(|entry| entry.metadata().is_file())
        .map(|entry| entry.path().to_owned())
        .collect();
    let in_order = listed == keys[1..];
    if !in_order {
        tracing::warn!(
            ?listed,
            expected = ?&keys[1..],
            "backend does not list in key order after a key, although it claims to resume \
             after one; listings walk directories instead"
        );
    }
    Ok(in_order)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn fs_operator(root: &Path) -> Operator {
        build_operator(
            "fs",
            &[("root".to_owned(), root.to_string_lossy().into_owned())],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn fs_takes_the_gateway_paths_for_every_field() {
        // fs has no ETag and no content headers, lists in readdir order and keeps empty dirs, so
        // each field must take s3dal's own path; xattrs take any metadata name. If OpenDAL ever
        // adds one of these natively, this test should fail and force a deliberate switch.
        let root = tempfile::tempdir().unwrap();
        let profile = probe(&fs_operator(root.path())).await.unwrap();
        assert_eq!(
            profile,
            Profile {
                native_etag: false,
                native_content_type: false,
                native_cache_control: false,
                native_content_disposition: false,
                native_content_encoding: false,
                encoded_metadata_names: false,
                ordered_list: false,
                real_dirs: true,
                conditional_writes: false,
            }
        );
    }

    #[tokio::test]
    async fn probe_leaves_nothing_behind() {
        // Probe data must never leak into a user's root or accumulate across restarts.
        let root = tempfile::tempdir().unwrap();
        let op = fs_operator(root.path());
        probe(&op).await.unwrap();
        let probes = op.list(&format!("{RESERVED_ROOT}probe/")).await.unwrap();
        assert!(
            probes
                .iter()
                .all(|e| e.path() == format!("{RESERVED_ROOT}probe/")),
            "leftover: {probes:?}"
        );
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 1)]
    async fn an_fs_commit_holds_up_no_other_request() {
        // The fs close fsyncs on whichever thread polls it. Held on a runtime worker, that stalls
        // every request queued on the worker for the length of the fsync; with one worker, all
        // of them. The sleep stands in for a slow fsync.
        let root = tempfile::tempdir().unwrap();
        let op = fs_operator(root.path());
        let (started, has_started) = tokio::sync::oneshot::channel();
        let commit = tokio::spawn(async move {
            without_stalling(&op, async {
                op.write("k", "v").await.unwrap();
                started.send(()).unwrap();
                std::thread::sleep(std::time::Duration::from_millis(500));
                std::time::Instant::now()
            })
            .await
        });
        has_started.await.unwrap();
        let other = tokio::spawn(async { std::time::Instant::now() })
            .await
            .unwrap();
        let committed = commit.await.unwrap();
        assert!(other < committed, "the other request waited for the commit");
    }

    #[tokio::test]
    async fn the_order_probe_accepts_only_key_order_after_a_key() {
        // The listing path trusts the backend to list in key order and to resume strictly after
        // a key; a backend that does not would drop or repeat keys. Rejecting one that does
        // would instead walk every S3 bucket directory by directory.
        let memory = || build_operator("memory", &[]).unwrap();
        let in_order = |op: Operator| async move { lists_in_key_order(&op, "p/").await.unwrap() };
        assert!(in_order(memory().layer(listing::Resuming(str::cmp))).await);
        let by_segment = |a: &str, b: &str| a.split('/').cmp(b.split('/'));
        assert!(
            !in_order(memory().layer(listing::Resuming(by_segment))).await,
            "`a/c` listed before `a-`, as a directory-first walk does"
        );
        assert!(
            !in_order(memory()).await,
            "memory lists in key order but ignores start-after"
        );
    }

    /// Test doubles for backends the order probe must tell apart.
    mod listing {
        use std::cmp::Ordering;
        use std::sync::Arc;

        use opendal::raw::*;
        use opendal::{Capability, OperationContext, Result};

        /// Resumes strictly after the start-after key, as S3 does, and lists in the given
        /// order. Over the memory backend, which ignores start-after.
        #[derive(Debug)]
        pub struct Resuming(pub fn(&str, &str) -> Ordering);

        impl Layer for Resuming {
            fn apply_service(&self, inner: Servicer) -> Servicer {
                Arc::new(ResumingService {
                    inner,
                    order: self.0,
                })
            }
        }

        #[derive(Debug)]
        struct ResumingService {
            inner: Servicer,
            order: fn(&str, &str) -> Ordering,
        }

        impl Service for ResumingService {
            type Reader = oio::Reader;
            type Writer = oio::Writer;
            type Lister = ResumingLister;
            type Deleter = oio::Deleter;
            type Copier = oio::Copier;
            type Composer = oio::Composer;

            fn info(&self) -> ServiceInfo {
                self.inner.info()
            }

            fn capability(&self) -> Capability {
                self.inner.capability()
            }

            async fn create_dir(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpCreateDir,
            ) -> Result<RpCreateDir> {
                self.inner.create_dir(ctx, path, args).await
            }

            async fn stat(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpStat,
            ) -> Result<RpStat> {
                self.inner.stat(ctx, path, args).await
            }

            fn read(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpRead,
            ) -> Result<oio::Reader> {
                self.inner.read(ctx, path, args)
            }

            fn write(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpWrite,
            ) -> Result<oio::Writer> {
                self.inner.write(ctx, path, args)
            }

            fn delete(&self, ctx: &OperationContext) -> Result<oio::Deleter> {
                self.inner.delete(ctx)
            }

            fn list(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpList,
            ) -> Result<ResumingLister> {
                Ok(ResumingLister {
                    after: args.start_after().map(str::to_owned),
                    order: self.order,
                    inner: self.inner.list(ctx, path, args)?,
                    entries: None,
                })
            }

            fn copy(
                &self,
                ctx: &OperationContext,
                from: &str,
                to: &str,
                args: OpCopy,
            ) -> Result<oio::Copier> {
                self.inner.copy(ctx, from, to, args)
            }

            async fn rename(
                &self,
                ctx: &OperationContext,
                from: &str,
                to: &str,
                args: OpRename,
            ) -> Result<RpRename> {
                self.inner.rename(ctx, from, to, args).await
            }

            async fn presign(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpPresign,
            ) -> Result<RpPresign> {
                self.inner.presign(ctx, path, args).await
            }
        }

        struct ResumingLister {
            inner: oio::Lister,
            after: Option<String>,
            order: fn(&str, &str) -> Ordering,
            entries: Option<std::vec::IntoIter<oio::Entry>>,
        }

        impl oio::List for ResumingLister {
            async fn next(&mut self) -> Result<Option<oio::Entry>> {
                if self.entries.is_none() {
                    let mut entries = Vec::new();
                    while let Some(entry) = self.inner.next().await? {
                        if self
                            .after
                            .as_deref()
                            .is_none_or(|after| entry.path() > after)
                        {
                            entries.push(entry);
                        }
                    }
                    entries.sort_by(|a, b| (self.order)(a.path(), b.path()));
                    self.entries = Some(entries.into_iter());
                }
                Ok(self.entries.as_mut().and_then(Iterator::next))
            }
        }
    }

    #[tokio::test]
    async fn a_backend_that_ignores_write_conditions_is_not_trusted_with_them() {
        // A backend that accepts conditional writes and then writes anyway would let two s3dal
        // processes both create the same lock file, while s3dal skipped its own check for them.
        let op = build_operator("memory", &[])
            .unwrap()
            .layer(conditions::Ignoring);
        // Past the capability gate, so the answer comes from what the writes did.
        let cap = op.info().capability();
        assert!(cap.write_with_if_not_exists && cap.write_with_if_match);
        assert!(!honors_write_conditions(&op, "p/").await.unwrap());
    }

    /// A backend that claims write conditions and ignores them.
    mod conditions {
        use std::sync::Arc;

        use opendal::raw::*;
        use opendal::{Capability, MetadataBuilder, OperationContext, Result};

        /// Over memory, which checks `if_not_exists` itself, so the conditions are dropped on
        /// the way down. `stat` reports an ETag, so OpenDAL can turn `if_not_changed` into an
        /// `if_match` at all.
        #[derive(Debug)]
        pub struct Ignoring;

        impl Layer for Ignoring {
            fn apply_service(&self, inner: Servicer) -> Servicer {
                Arc::new(IgnoringService { inner })
            }
        }

        #[derive(Debug)]
        struct IgnoringService {
            inner: Servicer,
        }

        impl Service for IgnoringService {
            type Reader = oio::Reader;
            type Writer = oio::Writer;
            type Lister = oio::Lister;
            type Deleter = oio::Deleter;
            type Copier = oio::Copier;
            type Composer = oio::Composer;

            fn info(&self) -> ServiceInfo {
                self.inner.info()
            }

            fn capability(&self) -> Capability {
                Capability {
                    write_with_if_not_exists: true,
                    write_with_if_match: true,
                    ..self.inner.capability()
                }
            }

            async fn create_dir(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpCreateDir,
            ) -> Result<RpCreateDir> {
                self.inner.create_dir(ctx, path, args).await
            }

            async fn stat(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpStat,
            ) -> Result<RpStat> {
                let len = self
                    .inner
                    .stat(ctx, path, args)
                    .await?
                    .into_metadata()
                    .content_length();
                let mut meta = MetadataBuilder::file(len);
                meta.etag(format!("\"{len}\""));
                Ok(RpStat::new(meta.build()))
            }

            fn read(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpRead,
            ) -> Result<oio::Reader> {
                self.inner.read(ctx, path, args)
            }

            fn write(
                &self,
                ctx: &OperationContext,
                path: &str,
                _args: OpWrite,
            ) -> Result<oio::Writer> {
                self.inner.write(ctx, path, OpWrite::new())
            }

            fn delete(&self, ctx: &OperationContext) -> Result<oio::Deleter> {
                self.inner.delete(ctx)
            }

            fn list(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpList,
            ) -> Result<oio::Lister> {
                self.inner.list(ctx, path, args)
            }

            fn copy(
                &self,
                ctx: &OperationContext,
                from: &str,
                to: &str,
                args: OpCopy,
            ) -> Result<oio::Copier> {
                self.inner.copy(ctx, from, to, args)
            }

            async fn rename(
                &self,
                ctx: &OperationContext,
                from: &str,
                to: &str,
                args: OpRename,
            ) -> Result<RpRename> {
                self.inner.rename(ctx, from, to, args).await
            }

            async fn presign(
                &self,
                ctx: &OperationContext,
                path: &str,
                args: OpPresign,
            ) -> Result<RpPresign> {
                self.inner.presign(ctx, path, args).await
            }
        }
    }

    #[tokio::test]
    async fn backend_without_user_metadata_is_refused() {
        // Running without user metadata would silently drop x-amz-meta-* and ETags.
        let op = build_operator("memory", &[]).unwrap();
        let err = probe(&op).await.unwrap_err().to_string();
        assert!(err.contains("write_with_user_metadata"), "{err}");
    }

    #[test]
    fn fs_gets_an_atomic_write_dir_inside_its_root() {
        // Partial uploads must stay invisible, and the final rename must not cross filesystems.
        let root = tempfile::tempdir().unwrap();
        let mut opts = vec![(
            "root".to_owned(),
            root.path().to_string_lossy().into_owned(),
        )];
        ensure_fs_atomic_write_dir(&mut opts).unwrap();
        let dir = &opts
            .iter()
            .find(|(k, _)| k == "atomic_write_dir")
            .unwrap()
            .1;
        assert!(Path::new(dir).starts_with(root.path()));
        assert!(Path::new(dir).is_dir());
    }

    /// Options for an S3 upstream on loopback that answers the `n`th request, counted from 0,
    /// with the status `answer(n, head)` and no body, and the count of requests it got. `head`
    /// is the request line and headers, lowercased.
    #[cfg(feature = "services-s3")]
    async fn fake_s3(
        answer: fn(usize, &str) -> &'static str,
    ) -> (
        Vec<(String, String)>,
        std::sync::Arc<std::sync::atomic::AtomicUsize>,
    ) {
        use std::sync::Arc;
        use std::sync::atomic::{AtomicUsize, Ordering};
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let endpoint = format!("http://{}", listener.local_addr().unwrap());
        let requests = Arc::new(AtomicUsize::new(0));
        let counted = requests.clone();
        tokio::spawn(async move {
            loop {
                let (mut socket, _) = listener.accept().await.unwrap();
                let mut head = Vec::new();
                while !head.ends_with(b"\r\n\r\n") {
                    let mut byte = [0; 1];
                    if socket.read(&mut byte).await.unwrap() == 0 {
                        break;
                    }
                    head.push(byte[0]);
                }
                let head = String::from_utf8_lossy(&head).to_ascii_lowercase();
                let status = answer(counted.fetch_add(1, Ordering::SeqCst), &head);
                let response =
                    format!("HTTP/1.1 {status}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n");
                socket.write_all(response.as_bytes()).await.unwrap();
            }
        });

        let opts = [
            ("bucket", "b"),
            ("endpoint", endpoint.as_str()),
            ("region", "us-east-1"),
            ("access_key_id", "key"),
            ("secret_access_key", "secret"),
        ]
        .map(|(k, v)| (k.to_owned(), v.to_owned()));
        (opts.to_vec(), requests)
    }

    #[cfg(feature = "services-s3")]
    #[tokio::test]
    async fn a_transient_backend_error_is_retried_before_it_reaches_the_client() {
        // A 503 from the upstream is a blip, not an answer: surfacing it would make the S3
        // client redo the whole request. The upstream here fails once, then answers 404.
        let (opts, requests) = fake_s3(|n, _| match n {
            0 => "503 Service Unavailable",
            _ => "404 Not Found",
        })
        .await;
        let op = build_operator("s3", &opts).unwrap();
        let err = op.stat("k").await.unwrap_err();
        assert_eq!(err.kind(), opendal::ErrorKind::NotFound, "{err}");
        assert_eq!(requests.load(std::sync::atomic::Ordering::SeqCst), 2);
    }

    #[cfg(feature = "services-s3")]
    #[tokio::test]
    async fn a_failing_backend_is_not_taken_for_one_that_refuses_metadata_names() {
        // Once retries run out, a temporary error comes back persistent, not temporary. Taken
        // for a refusal, it would switch on name encoding on a backend that keeps plain names.
        let (opts, _) = fake_s3(|_, _| "503 Service Unavailable").await;
        // build_operator's own retries, minus their seconds of backoff.
        let quick = RetryLayer::new()
            .with_min_delay(std::time::Duration::from_millis(1))
            .with_max_delay(std::time::Duration::from_millis(1));
        opendal::install_default();
        let op = Operator::via_iter("s3", opts).unwrap().layer(quick);
        let err = keeps_metadata_name(&op, "names").await.unwrap_err();
        assert!(format!("{err:#}").contains("503"), "{err:#}");
    }

    #[cfg(feature = "services-s3")]
    #[tokio::test]
    async fn a_backend_that_refuses_a_metadata_name_gets_names_encoded() {
        // The refusal comes back from the retry layer persistent, as an outage does, and must
        // still read as a refusal: otherwise s3dal could not start on Azure at all.
        let (opts, _) = fake_s3(|_, head| {
            if head.contains("x-amz-meta-0s3dal-probe.name_:") {
                "400 Bad Request"
            } else {
                "200 OK"
            }
        })
        .await;
        let op = build_operator("s3", &opts).unwrap();
        assert!(!keeps_metadata_name(&op, "names").await.unwrap());
    }

    #[cfg(feature = "services-s3")]
    #[tokio::test]
    async fn a_backend_that_stores_no_user_metadata_is_refused_by_name() {
        // fs claims user metadata on every filesystem, and one without user xattrs refuses each
        // write that carries some (ENOTSUP). Reported as "cannot write", the user would not
        // learn that the filesystem is what has to change.
        let (opts, _) = fake_s3(|_, head| {
            if head.contains("x-amz-meta-") {
                "400 Bad Request"
            } else {
                "200 OK"
            }
        })
        .await;
        let op = build_operator("s3", &opts).unwrap();
        let err = keeps_metadata_name(&op, "names").await.unwrap_err();
        assert!(
            format!("{err:#}").contains("refuses to store user metadata"),
            "{err:#}"
        );
    }

    #[test]
    fn fs_without_root_is_a_configuration_error() {
        let err = build_operator("fs", &[]).unwrap_err().to_string();
        assert!(err.contains("root"), "{err}");
    }
}
