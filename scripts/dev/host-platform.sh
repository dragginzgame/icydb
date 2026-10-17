#!/usr/bin/env bash
set -euo pipefail

# Observe and normalize only; each installer owns its supported asset matrix.
[[ $# == 0 ]] || { echo 'usage: host-platform.sh' >&2; exit 2; }
os="$(uname -s)"
architecture="$(uname -m)"
case "$os" in
    Linux|Darwin) ;;
    *) echo "unsupported tool OS: $os" >&2; exit 1 ;;
esac
case "$architecture" in
    x86_64|amd64) architecture=x86_64 ;;
    arm64|aarch64) architecture=arm64 ;;
    *) echo "unsupported tool architecture: $architecture" >&2; exit 1 ;;
esac
printf '%s:%s\n' "$os" "$architecture"
