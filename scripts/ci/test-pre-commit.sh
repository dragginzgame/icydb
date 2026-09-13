#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEST_ROOT="$(mktemp -d)"

cleanup() {
  find "$TEST_ROOT" -depth -delete
}
trap cleanup EXIT

# Exercise the real hook in isolated indexes without making any commits or
# running workspace formatters. The fixture formatter can also fail after edits.
setup_fixture() {
  local fixture="$TEST_ROOT/$1"
  mkdir -p "$fixture/.githooks"
  git -C "$fixture" init -q
  cp "$ROOT_DIR/.githooks/pre-commit" "$fixture/.githooks/pre-commit"
  printf 'fmt:\n\tsed -i s/unformatted/formatted/g -- *.rs Cargo.toml\n\ttest ! -e fail\n' >"$fixture/Makefile"
  cd "$fixture"
  printf 'unformatted\n' >Cargo.toml
}

expect_failure() {
  if bash .githooks/pre-commit; then
    echo "Expected the hook to reject this fixture." >&2
    exit 1
  fi
}

setup_fixture fully-staged
printf 'unformatted\n' >'staged [1].rs'
printf 'unformatted\n' >unselected.rs
git add -- 'staged [1].rs' Cargo.toml
bash .githooks/pre-commit
test "$(git show ':staged [1].rs')" = formatted
test "$(git show :Cargo.toml)" = formatted
test "$(git ls-files -- unselected.rs)" = ''
test "$(<unselected.rs)" = formatted
git diff --exit-code
# Formatting an already formatted selection is also successful and stable.
before="$(git write-tree)"
bash .githooks/pre-commit
test "$(git write-tree)" = "$before"

setup_fixture partially-staged
printf 'unformatted staged\n' >partial.rs
git add partial.rs
printf 'unformatted unstaged\n' >>partial.rs
before="$(git write-tree)"
before_worktree="$(git hash-object partial.rs)"
expect_failure
test "$(git write-tree)" = "$before"
test "$(git hash-object partial.rs)" = "$before_worktree"
test "$(<Cargo.toml)" = unformatted

setup_fixture formatter-failure
printf 'unformatted\n' >staged.rs
git add staged.rs Cargo.toml
touch fail
before="$(git write-tree)"
expect_failure
test "$(git write-tree)" = "$before"
test "$(<staged.rs)" = formatted

setup_fixture no-formatter-inputs-staged
printf 'unformatted\n' >unselected.rs
printf 'selected prose\n' >README.md
git add README.md
before="$(git write-tree)"
bash .githooks/pre-commit
test "$(git write-tree)" = "$before"
test "$(<unselected.rs)" = formatted

echo "Pre-commit formatting tests passed."
