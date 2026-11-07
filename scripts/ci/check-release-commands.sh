#!/usr/bin/env bash
# Shared companions: scripts/ci/prepare-script-fixture.sh
set -euo pipefail

# Execute a reviewed Makefile copy in an isolated fixture with a substitute runner.
# This is an adoption check, not a sandbox for arbitrary Makefile code.
if [[ $# -lt 1 ]]; then
    echo 'usage: check-release-commands.sh ROOT [RELATIVE_MAKE_INPUT ...]' >&2
    exit 2
fi
root="$1"
[[ "$root" == /* ]] || root="$PWD/$root"
root="$(cd -P "$root" && printf '%s/.' "$PWD")"
root="${root%/.}"
shift
[[ -f "$root/Makefile" ]] || { echo "missing Makefile: $root" >&2; exit 2; }
for input in "$@"; do
    case "$input" in
        ''|/*|..|../*|*/../*|*/..|./*|*/./*|*/.|Makefile|scripts/ci/run-release.sh|scripts/ci/run-release-batch.pl)
            echo "invalid fixture input: $input" >&2; exit 2 ;;
    esac
done
# Independent test checkout: never inherit parent release selections or identity.
unset MAKEFLAGS MFLAGS MAKEOVERRIDES GNUMAKEFLAGS MAKEFILES
unset VALIDATION_REPOSITORY_ROOT VALIDATION_RUNNER_SNAPSHOT_PATH
unset SHARED_TOOLING_ROOT
# Preserve only the consumer's explicit group selection. A parent batch's
# active flag, empty roster or saved identity must not change this fixture.
while IFS= read -r variable; do
    [[ "$variable" == RELEASE_GROUP ]] || unset "$variable"
done < <(compgen -A variable RELEASE_)
unset FROZEN_RELEASE_ACTIVE SHARED_RELEASE_BATCH SHARED_PUBLICATION_RESULT
fixture="$(mktemp -d "${TMPDIR:-/tmp}/release-commands.XXXXXX")"
finish() {
    local status=$?
    if [[ "$status" == 0 ]]; then
        rm -rf "$fixture"
    else
        echo "Release command check failed; fixture and logs retained: $fixture" >&2
    fi
}
trap finish EXIT
[[ "$fixture" == /* ]] || fixture="$PWD/$fixture"
fixture="$(cd -P "$fixture" && printf '%s/.' "$PWD")"
fixture="${fixture%/.}"
# Make's nested admission probe runs after cd; retain the same physical temp parent.
export TMPDIR="${fixture%/*}"
helper="${BASH_SOURCE[0]}"
[[ "$helper" == /* ]] || helper="$PWD/$helper"
bash "${helper%/*}/prepare-script-fixture.sh" "$root" "$fixture" Makefile "$@"
# The copier never executes payloads. Replace the admitted runner before Make
# can evaluate the selected consumer code, including parse-time expressions.
mkdir -p "$fixture/scripts/ci"
cat > "$fixture/scripts/ci/run-release.sh" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
printf '%s\0' "$@" >> "$RELEASE_COMMAND_EVENTS"
exit "$RELEASE_COMMAND_RESULT"
STUB
chmod +x "$fixture/scripts/ci/run-release.sh"
cat > "$fixture/scripts/ci/run-release-batch.pl" <<'STUB'
use strict;
use warnings;
open my $events, '>>', $ENV{RELEASE_COMMAND_EVENTS} or die $!;
print {$events} join("\0", 'batch', @ARGV), "\0";
close $events or die $!;
exit $ENV{RELEASE_COMMAND_RESULT};
STUB
export RELEASE_COMMAND_EVENTS="$fixture/events"
cd "$fixture"
# This checker supports a root snapshot only. Override reviewed Makefile
# defaults/assignments as well as the environment, so its include cannot route
# to the original checkout. Consumer Make code remains reviewed executable input.
fixture_make=(make --no-print-directory -f Makefile "SHARED_TOOLING_ROOT=$fixture")
cat > release-command-probe.mk <<'PROBE'
.PHONY: shared-release-command-selection
shared-release-command-selection:
	@printf '%s\n' '$(RELEASE_GROUPS)' '$(RELEASE_PUBLISH_TARGET)' '$(RELEASE_SETUP_TARGET)' '$(if $(filter undefined,$(origin RELEASE_DELIVERY)),direct,$(RELEASE_DELIVERY))'
PROBE
"${fixture_make[@]}" -f release-command-probe.mk shared-release-command-selection > selection
groups=$(sed -n '1p' selection)
publish_target=$(sed -n '2p' selection)
setup_target=$(sed -n '3p' selection)
delivery=$(sed -n '4p' selection)
contexts=(outer)
[[ -z "$groups" ]] || contexts+=(inner)
for context in "${contexts[@]}"; do
context_args=()
[[ "$context" != inner ]] || context_args+=(RELEASE_BATCH_ACTIVE=1)
for kind in patch minor major resume; do
    for result in 0 17; do
        export RELEASE_COMMAND_RESULT="$result"
        : > "$RELEASE_COMMAND_EVENTS"
        status=0
        "${fixture_make[@]}" ${context_args[@]+"${context_args[@]}"} "release-$kind" VERSION=0.1.1 \
            RELEASE_REMOTE=review RELEASE_BRANCH=release-review \
            > "$kind-$result.log" 2>&1 || status=$?
        if [[ "$result" == 0 ]]; then
            [[ "$status" == 0 ]] || exit 1
        else
            [[ "$status" != 0 ]] || exit 1
        fi
        if [[ "$kind" == resume ]]; then
            printf '%s\0' resume 0.1.1 review release-review > expected
        elif [[ "$context" == outer && -n "$groups" ]]; then
            batch_args=(--root "$fixture" --groups "$groups" --kind "$kind" --remote review --branch release-review --delivery "$delivery")
            [[ -z "$publish_target" ]] || batch_args+=(--publish-target "$publish_target")
            [[ -z "$setup_target" ]] || batch_args+=(--setup-target "$setup_target")
            printf '%s\0' batch "${batch_args[@]}" > expected
        else
            printf '%s\0' "$kind" review release-review > expected
        fi
        cmp expected "$RELEASE_COMMAND_EVENTS"
    done
done
done
export RELEASE_COMMAND_RESULT=0
for first in patch minor major resume; do
    for second in patch minor major resume; do
        [[ "$first" != "$second" ]] || continue
        : > "$RELEASE_COMMAND_EVENTS"
        if "${fixture_make[@]}" "release-$first" "release-$second" \
            VERSION=0.1.1 RELEASE_REMOTE=review RELEASE_BRANCH=release-review \
            > "conflict-$first-$second.log" 2>&1; then
            echo 'Makefile accepted conflicting release commands' >&2
            exit 1
        fi
        [[ ! -s "$RELEASE_COMMAND_EVENTS" ]] || exit 1
    done
done
echo 'Standard release Make adapters passed (substitute runner; no release effects)'
