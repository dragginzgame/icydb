#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 <version>" >&2
  exit 2
fi

VERSION="$1"
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")"/../.. && pwd)"

cd "$ROOT"

if [[ ! "$VERSION" =~ ^[0-9]+\.[0-9]+\.[0-9]+(-[0-9A-Za-z.-]+)?(\+[0-9A-Za-z.-]+)?$ ]]; then
  echo "invalid semantic version: $VERSION" >&2
  exit 2
fi

sed -i -E "s/Current workspace version: \`[^\`]+\`/Current workspace version: \`$VERSION\`/" README.md
sed -i -E "s/tag = \"v[0-9]+\.[0-9]+\.[0-9]+([-.+][^\"]*)?\"/tag = \"v$VERSION\"/g" README.md

if ! rg -q "Current workspace version: \`$VERSION\`" README.md; then
  echo "README workspace version did not sync to $VERSION" >&2
  exit 1
fi

STALE_TAGS=$(rg 'tag = "v[0-9]+\.[0-9]+\.[0-9]+' README.md | rg -v "tag = \"v$VERSION\"" || true)
if [[ -n "$STALE_TAGS" ]]; then
  echo "README git tag examples did not all sync to v$VERSION" >&2
  printf '%s\n' "$STALE_TAGS" >&2
  exit 1
fi

echo "✅ Synced release surface version to $VERSION"
