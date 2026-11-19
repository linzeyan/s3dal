# Release steps; the checks are in scripts/release.sh. In order:
#
#   make bump VERSION=X.Y.Z   after writing CHANGELOG.md's section for X.Y.Z
#   make compat               ceph s3-tests on the four backends (Docker), then the report
#   make tag                  the standard check, then one commit on origin/main, tagged
#   make push                 push main, which starts CI
#   make publish              once CI passed on that commit: crates.io, then the tag
#   make check                rustfmt, clippy and the tests, as CI runs them
#
# Recipes stay one command per line: macOS ships GNU make 3.81, without .ONESHELL.

.DEFAULT_GOAL := help
.PHONY: help bump check compat tag push publish

# CI's lint job pins its toolchain because each Rust release adds lints; lint with the same
# one, read from the workflow, or the local gate passes what CI rejects (0.4.0's first push:
# local 1.97, CI 1.99). The lint job's pin must stay the file's first `@1.x`.
LINT_RUST := $(shell sed -n 's|.*dtolnay/rust-toolchain@\(1\.[0-9]*\).*|\1|p' .github/workflows/ci.yml | head -1)

help:
	@sed -n 's/^#   //p' $(firstword $(MAKEFILE_LIST))

bump:
	@scripts/release.sh bump "$(VERSION)"

check:
	@test -n "$(LINT_RUST)" || { echo "no dtolnay/rust-toolchain@1.x pin in .github/workflows/ci.yml" >&2; exit 2; }
	cargo +$(LINT_RUST) fmt --check
	cargo +$(LINT_RUST) clippy --locked --all-targets -- -D warnings
	cargo test --locked

compat:
	scripts/s3tests.sh fs
	scripts/with-rustfs.sh scripts/s3tests.sh s3-rustfs
	scripts/with-azurite.sh scripts/s3tests.sh azblob-azurite
	scripts/with-fake-gcs.sh scripts/s3tests.sh gcs-fake
	python3 -I scripts/s3tests-report.py

tag: check
	@scripts/release.sh tag

push:
	@scripts/release.sh push

publish:
	@scripts/release.sh publish
