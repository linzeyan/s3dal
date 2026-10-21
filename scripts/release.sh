#!/usr/bin/env bash
# The steps of a release, run through the Makefile (`make help`). Each one checks what the next
# irreversible step relies on: a version with its CHANGELOG section, compat/ recorded on the
# tree being released, and CI green on the commit before anything goes to crates.io.
#
#   scripts/release.sh bump X.Y.Z | compat-current | tag | push | publish
set -euo pipefail
cd "$(dirname -- "${BASH_SOURCE[0]}")/.."

die() {
    echo "release: $*" >&2
    exit 1
}

version() { sed -n 's/^version = "\(.*\)"$/\1/p' Cargo.toml | head -1; }

has_changelog() { grep -q "^## \[$1\] - [0-9]\{4\}-[0-9][0-9]-[0-9][0-9]$" CHANGELOG.md; }

no_tag() {
    if git rev-parse -q --verify "refs/tags/v$1" >/dev/null; then die "tag v$1 exists"; fi
}

on_main() { [[ "$(git branch --show-current)" == main ]] || die "not on main"; }

# The id scripts/s3tests.sh records with each run: the working tree without compat/.
source_tree() {
    local dir tree
    dir=$(mktemp -d)
    tree=$(export GIT_INDEX_FILE="$dir/index" &&
        git read-tree HEAD &&
        git add -A -- . ':!compat' &&
        git rm -r -q --cached --ignore-unmatch -- compat &&
        git write-tree)
    rm -rf "$dir"
    git rev-parse --short "$tree"
}

# The version is an edit like any other, so the records must come from a run after it.
compat_current() {
    local want got f stale=0
    want=$(source_tree)
    for f in compat/s3-tests/*.json; do
        got=$(python3 -I -c 'import json, sys; print(json.load(open(sys.argv[1]))["s3dal_source"])' "$f")
        # Abbreviated ids can differ in length as the repository grows.
        if [[ "$got" != "$want"* && "$want" != "$got"* ]]; then
            echo "$f: recorded on $got, the tree is $want" >&2
            stale=1
        fi
    done
    ((stale == 0)) || die "compat/ is not from this tree: run make compat after the last edit"
    echo "compat/ records this tree ($want)"
}

bump() {
    local v=${1:-}
    [[ "$v" =~ ^[0-9]+\.[0-9]+\.[0-9]+$ ]] || die "usage: make bump VERSION=X.Y.Z"
    no_tag "$v"
    has_changelog "$v" || die "write CHANGELOG.md's '## [$v] - YYYY-MM-DD' section first"
    # The package's own version is the first `version =` line; dependency tables come later.
    V="$v" perl -pi -e 'if (!$done && s/^version = "[^"]*"/version = "$ENV{V}"/) { $done = 1 }' Cargo.toml
    # Cargo.lock records the package's version too.
    cargo metadata --format-version 1 --offline >/dev/null
    [[ "$(version)" == "$v" ]] || die "Cargo.toml still says $(version)"
    git diff --stat -- Cargo.toml Cargo.lock
    echo "next: make compat, then make tag"
}

# The lines of CHANGELOG.md's section for $1, without its heading.
changelog_section() {
    awk -v h="## [$1]" 'index($0, h) == 1 { on = 1; next } on && /^## \[/ { exit } on' CHANGELOG.md |
        sed '/./,$!d'
}

# A release goes out as one commit on top of origin/main: the commits not pushed yet and the
# release's files, with the changelog section as its message. Those commits stay on a branch.
tag() {
    local v other backup msg squashed
    v=$(version)
    backup="pre-squash-v$v"
    on_main
    no_tag "$v"
    has_changelog "$v" || die "CHANGELOG.md has no section for $v"
    other=$(git status --porcelain -- . ':!Cargo.toml' ':!Cargo.lock' ':!CHANGELOG.md' ':!compat')
    [[ -z "$other" ]] || die "commit these first; the release adds only its own files:"$'\n'"$other"
    git fetch -q origin main
    git merge-base --is-ancestor origin/main HEAD || die "main does not build on origin/main"
    compat_current
    cargo publish --dry-run --locked --allow-dirty
    if [[ "$(git rev-parse HEAD)" != "$(git rev-parse origin/main)" ]]; then
        if git rev-parse -q --verify "refs/heads/$backup" >/dev/null; then die "branch $backup exists"; fi
        git branch "$backup"
    fi
    git add Cargo.toml Cargo.lock CHANGELOG.md compat
    msg=$(printf 's3dal %s\n\n%s\n' "$v" "$(changelog_section "$v")")
    squashed=$(git commit-tree "$(git write-tree)" -p origin/main -m "$msg")
    git reset -q --soft "$squashed"
    git tag -a "v$v" -m "s3dal $v"
    echo "tagged v$v at $(git rev-parse --short HEAD), one commit on origin/main; next: make push"
}

push() {
    on_main
    git push origin main
    echo "CI runs on $(git rev-parse --short HEAD): gh run watch, then make publish"
}

publish() {
    local v sha ci ok
    v=$(version)
    sha=$(git rev-parse HEAD)
    [[ "$(git rev-parse -q --verify "v$v^{commit}" || true)" == "$sha" ]] || die "v$v is not HEAD: make tag"
    [[ -z "$(git status --porcelain)" ]] || die "the working tree is not clean"
    [[ "$(git rev-parse origin/main)" == "$sha" ]] || die "origin/main is not HEAD: make push"
    ci=$(gh run list --commit "$sha" --workflow ci.yml --json status,conclusion \
        --jq '.[0] | "\(.status) \(.conclusion)"')
    [[ "$ci" == "completed success" ]] || die "CI on ${sha:0:7}: ${ci:-no run} (gh run watch)"
    read -r -p "Publish s3dal $v to crates.io? It cannot be taken back, only yanked. [y/N] " ok
    [[ "$ok" == y ]] || die "not published"
    cargo publish --locked
    git push origin "v$v"
}

case "${1:-}" in
bump) bump "${2:-}" ;;
compat-current) compat_current ;;
tag) tag ;;
push) push ;;
publish) publish ;;
*) die "usage: scripts/release.sh bump X.Y.Z | compat-current | tag | push | publish" ;;
esac
