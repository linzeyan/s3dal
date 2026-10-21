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

help:
	@sed -n 's/^#   //p' $(firstword $(MAKEFILE_LIST))

bump:
	@scripts/release.sh bump "$(VERSION)"

check:
	cargo fmt --check
	cargo clippy --locked --all-targets -- -D warnings
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
