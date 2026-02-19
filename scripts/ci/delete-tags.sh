#!/usr/bin/env bash

set -e

PREFIX="$1"

if [ -z "$PREFIX" ]; then
  echo "Usage: $0 <version-prefix>"
  echo "Example:"
  echo "  $0 v0.1"
  echo "  $0 0.1"
  exit 1
fi

# Escape dots for regex use
REGEX_PREFIX=$(echo "$PREFIX" | sed 's/\./\\./g')

# Collect matching local tags
LOCAL_TAGS=$(git tag -l "${PREFIX}.*")

# Collect matching remote tags
REMOTE_TAGS=$(git ls-remote --tags origin \
  | awk '{print $2}' \
  | sed 's|refs/tags/||' \
  | grep "^${REGEX_PREFIX}\." | grep -v '\^{}' || true)

echo
echo "Local tags to delete:"
echo "$LOCAL_TAGS"
echo
echo "Remote tags to delete:"
echo "$REMOTE_TAGS"
echo

if [ -z "$LOCAL_TAGS" ] && [ -z "$REMOTE_TAGS" ]; then
  echo "No matching tags found."
  exit 0
fi

read -p "Proceed with deleting ALL of the above? (y/N): " CONFIRM

if [ "$CONFIRM" = "y" ]; then
  # Delete local tags
  if [ -n "$LOCAL_TAGS" ]; then
    echo "$LOCAL_TAGS" | xargs -r git tag -d
  fi

  # Delete remote tags
  if [ -n "$REMOTE_TAGS" ]; then
    echo "$REMOTE_TAGS" | xargs -r -n 1 git push origin --delete
  fi

  echo "Done."
else
  echo "Aborted."
fi
