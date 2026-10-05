# shellcheck shell=bash
# shellcheck disable=SC2034
# Sourced by installation and verification; the table owns every admitted digest.
case "$(uname -s):$(uname -m)" in
  Linux:x86_64 | Linux:amd64) platform=linux_x86_64 ;;
  Darwin:x86_64 | Darwin:amd64) platform=darwin_x86_64 ;;
  Darwin:arm64 | Darwin:aarch64) platform=darwin_arm64 ;;
  *)
    echo "unsupported Binaryen platform: $(uname -s) $(uname -m)" >&2
    exit 1
    ;;
esac

PINS="$ROOT/scripts/ci/wasm-optimizer-checksums.tsv"
BINARYEN_VERSION="$(awk '$1 == "version" {print $2}' "$PINS")"
read -r ARCHIVE_NAME ARCHIVE_SHA256 WASM_OPT_SHA256 < <(
  awk -v platform="$platform" '$1 == platform {print $2, $3, $4}' "$PINS"
)
if [[ ! "$BINARYEN_VERSION" =~ ^version_[0-9]+$ ]] ||
   [[ ! "$ARCHIVE_SHA256" =~ ^[0-9a-f]{64}$ ]] ||
   [[ ! "$WASM_OPT_SHA256" =~ ^[0-9a-f]{64}$ ]]; then
  echo "invalid Binaryen pin for $platform" >&2
  exit 1
fi
WASM_OPT_VERSION="wasm-opt version ${BINARYEN_VERSION#version_} ($BINARYEN_VERSION)"
