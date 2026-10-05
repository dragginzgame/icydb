#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
TEST_ROOT="$(mktemp -d "${TMPDIR:-/tmp}/icydb-pre-commit.XXXXXX")"

cleanup() {
  rm -rf "$TEST_ROOT"
}
trap cleanup EXIT

# Exercise selected-file preservation in disposable indexes, without commits,
# dependency resolution or formatting the caller's worktree.
setup_fixture() {
  local fixture="$TEST_ROOT/$1"
  mkdir -p "$fixture/.githooks" "$fixture/scripts/dev"
  git -C "$fixture" init -q
  cp "$ROOT_DIR/.githooks/pre-commit" "$fixture/.githooks/pre-commit"
  cp "$ROOT_DIR/scripts/dev/install-git-hooks.sh" "$fixture/scripts/dev/"
  cat > "$fixture/Makefile" <<'MAKE'
fmt:
	@for path in *.rs Cargo.toml README.md; do \
		[ -f "$$path" ] || continue; \
		perl -pi -e 's/unformatted/formatted/g' "$$path"; \
	done
	@test "$(FORMAT_TEST_FAIL)" != yes
MAKE
  cd "$fixture"
  printf 'unformatted\n' > Cargo.toml
  git add Makefile
}

expect_failure() {
  if "$@" > "$TEST_ROOT/rejection" 2>&1; then
    echo "Expected rejection of this fixture." >&2
    exit 1
  fi
}

setup_fixture fully-staged
printf 'unformatted\n' > 'staged [1].rs'
newline_path=$'staged\nnewline.rs'
printf 'unformatted\n' > "$newline_path"
printf 'unformatted\n' > unselected.rs
git add -- 'staged [1].rs' "$newline_path" Cargo.toml
bash .githooks/pre-commit
test "$(git show ':staged [1].rs')" = formatted
test "$(git show ":$newline_path")" = formatted
test "$(git show :Cargo.toml)" = formatted
test "$(git ls-files -- unselected.rs)" = ''
test "$(<unselected.rs)" = unformatted
git diff --exit-code
before="$(git write-tree)"
bash .githooks/pre-commit
test "$(git write-tree)" = "$before"

setup_fixture partially-staged
printf 'unformatted staged\n' > partial.rs
git add partial.rs
printf 'unformatted unstaged\n' >> partial.rs
before="$(git write-tree)"
cp partial.rs "$TEST_ROOT/partial-before"
expect_failure bash .githooks/pre-commit
test "$(git write-tree)" = "$before"
cmp "$TEST_ROOT/partial-before" partial.rs
test "$(<Cargo.toml)" = unformatted

setup_fixture formatter-failure
printf 'unformatted\n' > staged.rs
git add staged.rs Cargo.toml
before="$(git write-tree)"
FORMAT_TEST_FAIL=yes expect_failure bash .githooks/pre-commit
test "$(git write-tree)" = "$before"
test "$(<staged.rs)" = unformatted
test "$(<Cargo.toml)" = unformatted

setup_fixture prose-selection
printf 'unformatted\n' > unselected.rs
printf 'selected prose\n' > README.md
git add README.md
before="$(git write-tree)"
bash .githooks/pre-commit
test "$(git write-tree)" = "$before"
test "$(<unselected.rs)" = unformatted

setup_fixture installer
bash scripts/dev/install-git-hooks.sh
test "$(git config --local --get core.hooksPath)" = .githooks
bash scripts/dev/install-git-hooks.sh
git config --local core.hooksPath private-hooks
expect_failure bash scripts/dev/install-git-hooks.sh
test "$(git config --local --get core.hooksPath)" = private-hooks

setup_fixture private-default-hook
printf '#!/bin/sh\nexit 0\n' > .git/hooks/pre-push
chmod +x .git/hooks/pre-push
cp .git/hooks/pre-push "$TEST_ROOT/private-before"
expect_failure bash scripts/dev/install-git-hooks.sh
cmp "$TEST_ROOT/private-before" .git/hooks/pre-push

echo "Shared pre-commit selection, failure isolation and installer tests passed."
