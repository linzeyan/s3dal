# Contributing to s3dal

## Requirements

- Rust 1.96 or later (the MSRV, set by s3s 0.17), with rustfmt and clippy.
- Docker, for the tests on the s3, azblob and gcs backends and for the aws CLI and rclone smoke
  tests. The scripts pull pinned images: `rustfs/rustfs:1.0.2`,
  `mcr.microsoft.com/azure-storage/azurite:3.37.0`, `fsouza/fake-gcs-server:1.56.1`,
  `amazon/aws-cli:2.38.1`, `rclone/rclone:1.75.0`.
- curl 7.75 or later (for `--aws-sigv4`), used by `scripts/with-rustfs.sh` and, for plain
  requests, by `scripts/with-fake-gcs.sh`.
- Python 3, standard library only, used by `scripts/with-azurite.sh` (to sign the request that
  creates the container) and `scripts/with-fake-gcs.sh` (for `scripts/fake-gcs-proxy.py`).
- git, for the s3s end-to-end smoke test and for ceph s3-tests, which are fetched at pinned
  revisions.
- [uv](https://docs.astral.sh/uv/) and Python 3.12, for ceph s3-tests.

## Build

```sh
cargo build                          # debug build of the library and the s3dal binary
cargo build --no-default-features    # fs only, without the s3, azblob and gcs backends
```

## The standard check

Every change must pass:

```sh
cargo fmt --check && cargo clippy --all-targets -- -D warnings && cargo test
scripts/with-rustfs.sh cargo test
scripts/with-azurite.sh cargo test
scripts/with-fake-gcs.sh cargo test
```

The first line runs the unit tests and the integration tests on the fs backend. The others run
the same suite on the s3 backend against a throwaway RustFS container, on azblob against
Azurite, and on gcs against fake-gcs-server; they need Docker.

A change is not done while a test fails, is skipped, or is marked `#[ignore]`.

## Tests

- Unit tests live next to the code in `src/`.
- Integration tests in `tests/` start a real s3dal server on `127.0.0.1` and drive it with the
  AWS Rust SDK, so they exercise the same HTTP, SigV4, and checksum paths as real clients. The
  harness is `tests/common/mod.rs`.
- The backend is fs on a temporary directory, unless `S3DAL_TEST_BACKEND` names another OpenDAL
  scheme, configured by `S3DAL_TEST_BACKEND_OPTS` (`key=value,...`). `scripts/with-rustfs.sh`,
  `scripts/with-azurite.sh` and `scripts/with-fake-gcs.sh` set both. Every test server gets a
  root of its own, so tests stay independent on a shared upstream.
- A test whose expectation differs by backend takes it from the probed profile
  (`server.profile`), never from the scheme name. In turn, the profile of every scheme the
  suite runs on is pinned in `tests/server.rs`
  (`each_backend_is_probed_to_the_profile_it_is_known_to_have`), so a probe that misjudges a
  backend fails there; a new backend adds its profile to it.
- Where an emulator differs from the service it stands for, s3dal and the tests follow the
  service. `scripts/fake-gcs-proxy.py` fills in, in front of fake-gcs-server, what GCS does and
  the emulator does not (XML API multipart uploads, batch requests as OpenDAL sends them), so
  the tests exercise what s3dal sends GCS. A difference that would take a change in s3dal or a
  weaker test is listed in README.md under "Compatibility" instead.
- A test of what a client can observe runs on the suite's backend (`TestServer::start`). A test
  of gateway-only machinery (synthesized ETags, files planted behind s3dal's back, key/prefix
  conflicts, directory mtimes) runs on fs in every run (`TestServer::start_fs`).

## Smoke tests

Each script builds s3dal, starts it on a temporary fs root, runs a real client against it, and
prints a pass/fail summary of every step. Logs stay under `target/smoke/`; `SMOKE_KEEP=1` keeps
the backend root and client files as well.

| Script | Client | Needs |
|---|---|---|
| `scripts/smoke-awscli.sh` | aws CLI, including a 64 MiB multipart upload and copy | Docker |
| `scripts/smoke-rclone.sh` | rclone: copy, check, sync, server-side copy | Docker |
| `scripts/smoke-e2e.sh` | s3s's end-to-end suite (`s3s-e2e`), built from s3s v0.17.0 | git, network on first run; `S3S_SRC` can name an existing checkout |

To run one on another backend, wrap it: `scripts/with-rustfs.sh scripts/smoke-rclone.sh`, and
likewise with `scripts/with-azurite.sh` or `scripts/with-fake-gcs.sh`. `AWS_CLI_IMAGE`,
`RCLONE_IMAGE`, `RUSTFS_IMAGE`, `AZURITE_IMAGE` and `FAKE_GCS_IMAGE` override the pinned
images.

## Compatibility report

`compat/s3-tests.md` is generated, never edited by hand:

```sh
scripts/s3tests.sh fs                                      # writes compat/s3-tests/fs.json
scripts/with-rustfs.sh scripts/s3tests.sh s3-rustfs        # writes compat/s3-tests/s3-rustfs.json
scripts/with-azurite.sh scripts/s3tests.sh azblob-azurite  # writes compat/s3-tests/azblob-azurite.json
scripts/with-fake-gcs.sh scripts/s3tests.sh gcs-fake       # writes compat/s3-tests/gcs-fake.json
python3 -I scripts/s3tests-report.py                       # writes compat/s3-tests.md
```

`scripts/s3tests.sh` fetches ceph s3-tests at the revision s3s pins, builds a Python
environment with uv (cached under `target/s3tests/`), runs the whole suite against a fresh
s3dal, and records each test's outcome together with the s3dal source it ran: the git tree id
of the working tree without `compat/`, so a run before committing names the tree the commit
will have, and committing the records does not change it. A commit's id is
`git ls-tree <commit> | awk -F'\t' '$2 != "compat"' | git mktree`. Commit the JSON files and
the report together. `scripts/s3tests.sh fs -- -k multipart` runs a
subset for debugging; it never writes `compat/`.

## Continuous integration

`.github/workflows/ci.yml` runs on pushes to `main` and on pull requests: rustfmt and clippy on
a pinned Rust version (1.99), so that the lints a new Rust release adds do not fail unrelated
changes; the tests on fs, on s3 (RustFS), on azblob (Azurite) and on gcs (fake-gcs-server); a
build with Rust 1.96; building the Docker image and serving from it; and the three smoke
scripts on all four backends. ceph s3-tests take long, so `.github/workflows/s3-tests.yml` runs only when started
by hand (Actions, "s3-tests", "Run workflow"). It uploads the resulting `compat/` directory as an
artifact and never commits it. `.github/workflows/release.yml` runs on a version tag: it builds
the Docker image for amd64 and arm64, each on a runner of that architecture, serves from each,
and only then pushes them to `ghcr.io/linzeyan/s3dal` as the version, `X.Y` and `latest`.

## Releasing

`make help` lists the steps, each in `scripts/release.sh`:

```sh
# first write CHANGELOG.md's "## [X.Y.Z] - YYYY-MM-DD" section
make bump VERSION=X.Y.Z   # Cargo.toml and Cargo.lock
make compat               # the version is an edit too: compat/ must record the released tree
make tag                  # the standard check, a packaging dry run, then commit and tag
make push                 # push main; CI runs on it
make publish              # once CI passed on that commit: crates.io, then the tag
```

`make tag` refuses other uncommitted changes. It makes the commits not yet on `origin/main`,
together with the version, the changelog and `compat/`, one commit on top of `origin/main`,
whose message is the changelog section; the commits it replaced stay on the branch
`pre-squash-vX.Y.Z`. Up to `make push` a step can be undone: `git tag -d vX.Y.Z` and
`git reset --soft pre-squash-vX.Y.Z` bring the commits back with the release's files staged.
The push itself is a fast-forward. A version on
crates.io can only be yanked, so `make publish` asks first, and pushes the tag, which publishes
the image, only after the crate is up.

## Conventions

- Code, comments, documentation, and commit messages are in English.
- Formatting is `cargo fmt`; clippy runs with `-D warnings`.
- Comments explain why the code is the way it is: the constraint, the client behavior, or the
  S3 rule it follows. They do not restate what the code does.
- A test is named after the behavior it checks, and its first comment states why that behavior
  matters. For example, `delete_with_if_match_deletes_only_the_expected_etag` begins with "A
  cleanup job must not delete an object that was replaced after it decided to."
- A deliberate difference from S3 is listed in README.md under "Differences from Amazon S3".
- A new dependency must be under a license listed in `docker/about.toml`; otherwise the image
  build fails, which is the point. Adding a license there is a decision for review.
- Code adapted from s3s or s3s-fs keeps the original attribution. The file starts with

  ```rust
  // SPDX-FileCopyrightText: 2023-2026 The s3s Authors
  // SPDX-License-Identifier: Apache-2.0
  //
  // Adapted from <file and function> in s3s v<version>: <what was taken>.
  ```

  (with `#` comments in scripts), and NOTICE lists the file under the s3s entry. Follow
  `src/s3/list/source.rs` and `scripts/s3tests.sh`.

## Commits

- One logical change per commit, with the standard check passing.
- The subject is an imperative sentence in sentence case, without a trailing period or a type
  prefix, and names the behavior rather than the files: "Store user metadata values RFC
  2047-encoded", "Match AWS on empty listing parameters, v1 Prefix and empty Content-MD5".
- The body, wrapped at 72 columns, says why: what was wrong or missing, which client or test
  showed it, and why this fix over the alternatives.
