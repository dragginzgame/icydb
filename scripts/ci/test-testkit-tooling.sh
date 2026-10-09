#!/usr/bin/env bash
set -euo pipefail

# Qualify the consumer's locked selection and execution boundary. Shared owns
# Cargo receipts; Testkit owns server authentication and lifecycle fixtures.
ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd -P)"
fixture="$(mktemp -d "${TMPDIR:-/tmp}/icydb-testkit-tools.XXXXXX")"
finish() {
  local status=$?
  if [[ "$status" == 0 ]]; then rm -rf "$fixture"
  else echo "Testkit tooling fixture retained: $fixture" >&2; fi
}
trap finish EXIT
mkdir -p "$fixture/scripts/ci" "$fixture/scripts/dev" "$fixture/make" "$fixture/bin" "$fixture/.tools/host/bin"
cp "$ROOT/Makefile" "$fixture/"
cp "$ROOT/make/tools.mk" "$ROOT/make/release.mk" "$ROOT/make/rust-format.mk" "$fixture/make/"
cp "$ROOT/scripts/ci/testkit-runner.sh" "$fixture/scripts/ci/"
cp "$ROOT/scripts/ci/actionlint-checksums.tsv" "$fixture/scripts/ci/"
printf '#!/usr/bin/env bash\nexit 0\n' > "$fixture/scripts/dev/install-host-tools.sh"
printf '[workspace]\n' > "$fixture/Cargo.toml"
cat > "$fixture/bin/cargo" <<'CARGO'
#!/usr/bin/env bash
set -euo pipefail
if [[ "$1" == test ]]; then
  [[ "$RUST_TEST_THREADS" == 2 ]]
  printf '%s\n' "$*" >> "$TEST_ROOT/native-test-requests"
  exit "${TEST_CARGO_FAIL:-0}"
fi
[[ "$1" == metadata && "$*" == *'--locked --offline --format-version 1'* ]]
[[ "$CARGO_HOME" == "$TEST_ROOT/.cache/cargo/icydb" ]]
[[ "${TEST_METADATA_FAIL:-0}" == 0 ]] || exit 19
printf '{"packages":[{"name":"ic-testkit","version":"0.25.5"}]}\n'
CARGO
cat > "$fixture/scripts/dev/install-rust-tools.sh" <<'INSTALLER'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$*" >> "$TEST_ROOT/installation-requests"
[[ "$*" == "--consumer $TEST_ROOT --package ic-testkit --version 0.25.5 --bin ic-testkit-server --profile release"* ]]
[[ "${TEST_CLI_FAIL:-0}" == 0 ]] || exit 23
printf '%s\n' "$TEST_ROOT/bin/ic-testkit-server"
INSTALLER
cat > "$fixture/bin/ic-testkit-server" <<'TESTKIT'
#!/usr/bin/env bash
set -euo pipefail
[[ "$#" == 3 && "$2" == --directory && "$3" == "$TEST_ROOT/.tools/ic-testkit-server" ]]
printf '%s\n' "$1" >> "$TEST_ROOT/server-requests"
[[ "${TEST_SERVER_FAIL:-0}" == 0 ]] || exit 29
printf '%s\n' "$TEST_ROOT/admitted-server"
TESTKIT
cat > "$fixture/scripts/ci/probe.sh" <<'PROBE'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\n' "$POCKET_IC_BIN" > "$TEST_ROOT/child-selection"
PROBE
cat >> "$fixture/Makefile" <<'MAKE'

probe:
	$(IC_TESTKIT_ENV) bash scripts/ci/probe.sh
MAKE
chmod +x "$fixture/bin/"*
export TEST_ROOT="$fixture" PATH="$fixture/bin:$ROOT/.tools/host/bin:$PATH"
cd "$fixture"

bash scripts/ci/testkit-runner.sh > "$fixture/runner"
[[ "$(cat "$fixture/runner")" == "$fixture/bin/ic-testkit-server" ]]
bash scripts/ci/testkit-runner.sh --check > "$fixture/runner"
[[ "$(tail -n 1 "$fixture/installation-requests")" == *' --check' ]]
make --silent install-testkit > "$fixture/setup"
[[ "$(cat "$fixture/server-requests")" == setup ]]
: > "$fixture/server-requests"
make --silent testkit-check > "$fixture/admitted"
[[ "$(cat "$fixture/admitted")" == "$fixture/admitted-server" ]]
make --silent probe
[[ "$(cat "$fixture/child-selection")" == "$fixture/admitted-server" ]]
[[ "$(cat "$fixture/server-requests")" == $'check\ncheck' ]]

# Every offline refusal must stop before the caller command, without setup.
for failure in TEST_METADATA_FAIL TEST_CLI_FAIL TEST_SERVER_FAIL; do
  rm -f "$fixture/child-selection"
  if env "$failure=1" make --silent probe > "$fixture/$failure.log" 2>&1; then
    echo "accepted failed admission: $failure" >&2; exit 1
  fi
  [[ ! -e "$fixture/child-selection" ]]
done
make --silent probe POCKET_IC_BIN="$fixture/explicit-server"
[[ "$(cat "$fixture/child-selection")" == "$fixture/explicit-server" ]]
if rg -q '^setup$' "$fixture/server-requests"; then exit 1; fi

# These selected CI bodies are native even though one belongs to the integration
# package. Failed infrastructure must not prevent their Cargo invocation.
: > "$fixture/server-requests"
: > "$fixture/installation-requests"
for target in _ci-workspace-tests _ci-tier-a-integration; do
  env TEST_METADATA_FAIL=1 TEST_CLI_FAIL=1 TEST_SERVER_FAIL=1 \
    make --silent "$target" > "$fixture/$target.log"
done
[[ ! -s "$fixture/server-requests" && ! -s "$fixture/installation-requests" ]]
rg -q --fixed-strings -- '--workspace --all-targets --exclude icydb-testing-integration' "$fixture/native-test-requests"
rg -q --fixed-strings -- '-p icydb-testing-integration --test sql_correctness' "$fixture/native-test-requests"
if env TEST_CARGO_FAIL=17 make --silent _ci-tier-a-integration > "$fixture/native-failure.log" 2>&1; then
  echo 'accepted failed native Cargo test' >&2; exit 1
fi
printf '[OK] Locked Testkit selection, offline refusals and caller overrides passed (substitute CLI)\n'
