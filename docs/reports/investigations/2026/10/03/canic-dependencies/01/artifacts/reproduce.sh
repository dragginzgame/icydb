#!/usr/bin/env bash
# Existing SDK dependencies and CLI only. No builds, downloads or network calls.
set -euo pipefail
if [[ $# != 4 ]]; then
  echo 'Usage: bash reproduce.sh UPSTREAM_ROOT NEW_OUTPUT_DIRECTORY EXISTING_CLI_BINARY NODE_BINARY' >&2
  exit 2
fi
audit_root=$(realpath "$1")
audit_output=$(realpath -m "$2")
audit_cli=$(realpath "$3")
audit_node=$(realpath "$4")
audit_scripts=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
mkdir -m 700 -- "$audit_output"
git -C "$audit_root" apply --reverse --check --directory=.tmp/browser/caffeine clients/browser/patches/caffeine-1.1.2.patch
sha256sum -- "$audit_cli" "$audit_root/clients/browser/publication.js" > "$audit_output/source-sha256.txt"
"$audit_node" "$audit_scripts/bundle.mjs" "$audit_root" "$audit_output/budget-bundle.mjs"
"$audit_node" "$audit_output/budget-bundle.mjs" > "$audit_output/budget-result.json"
mkfifo -m 600 -- "$audit_output/input.fifo"
audit_principal=rrkah-fqaaa-aaaaa-aaaaq-cai
audit_args=(funding-history --network local --url http://127.0.0.1:1
  --identity "$audit_output/missing.pem" --operator "$audit_principal"
  --service "$audit_principal" --namespace 1 --cashier "$audit_principal"
  --payer "$audit_principal" --root-key "$audit_output/missing.der" --cursor)
audit_status=0
timeout 2s "$audit_cli" "${audit_args[@]}" "$audit_output/input.fifo" > "$audit_output/fifo.stdout" 2> "$audit_output/fifo.stderr" || audit_status=$?
[[ $audit_status == 124 && ! -s "$audit_output/fifo.stdout" && ! -s "$audit_output/fifo.stderr" ]]
audit_directory_status=0
timeout 2s "$audit_cli" "${audit_args[@]}" "$audit_output" > "$audit_output/directory.stdout" 2> "$audit_output/directory.stderr" || audit_directory_status=$?
[[ $audit_directory_status == 3 ]]
"$audit_node" --input-type=module - "$audit_output" <<'JS'
import assert from 'node:assert/strict';
import { readFileSync, writeFileSync } from 'node:fs';
import { join } from 'node:path';
const output = process.argv[2];
assert.deepEqual(JSON.parse(readFileSync(join(output, 'directory.stdout'), 'utf8')), { error: 'file' });
writeFileSync(join(output, 'fifo-result.json'), JSON.stringify({
  exitCode: 124, stdoutBytes: 0, stderrBytes: 0, directoryControlExitCode: 3,
  caveat: 'Pre-existing CLI binary; exact reviewed-commit binding not independently established.',
}, null, 2) + '\n', { flag: 'wx', mode: 0o600 });
JS
echo 'PASS: three SDK budget defects and FIFO blocking reproduced offline'
