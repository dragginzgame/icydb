#!/usr/bin/env bash
set -euo pipefail

ROOT_DIR="${ICYDB_RELEASE_ROOT:-$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
export PATH="$ROOT_DIR/.tools/host/bin:$PATH"
VERSION_READER="$SCRIPT_DIR/read-cargo-workspace-version.sh"
RELEASE_RECEIPT_DIR="${RELEASE_RECEIPT_DIR:-$ROOT_DIR/.cache/release-receipts}"

usage() {
    echo "Usage: $0 verify-tested-tree <tested-commit> | record <patch|minor|major> <tested-commit> | verify-staged | verify-commit" >&2
    exit 2
}

is_release_note_path() {
    local path="$1"

    case "$path" in
        CHANGELOG.md|docs/changelog/*.md)
            return 0
            ;;
        *)
            return 1
            ;;
    esac
}

validate_version() {
    local version="$1"

    if [[ ! "$version" =~ ^(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})\.(0|[1-9][0-9]{0,8})$ ]]; then
        echo "Invalid workspace version: $version" >&2
        exit 1
    fi
}

validate_transition() {
    local bump="$1"
    local candidate_version="$2"
    local release_version="$3"
    local candidate_major candidate_minor candidate_patch expected_version

    validate_version "$candidate_version"
    validate_version "$release_version"
    IFS=. read -r candidate_major candidate_minor candidate_patch <<< "$candidate_version"

    case "$bump" in
        patch)
            expected_version="$candidate_major.$candidate_minor.$((10#$candidate_patch + 1))"
            ;;
        minor)
            expected_version="$candidate_major.$((10#$candidate_minor + 1)).0"
            ;;
        major)
            expected_version="$((10#$candidate_major + 1)).0.0"
            ;;
        *)
            usage
            ;;
    esac

    if [[ "$release_version" != "$expected_version" ]]; then
        echo "Expected $bump transition $candidate_version -> $expected_version; found $release_version" >&2
        exit 1
    fi
}

project_release_surface() {
    local path="$1" previous="$2" release="$3"
    if [[ "$path" == ci/dependency-pinning-exceptions.json ]]; then
        jq --arg previous "$previous" --arg release "$release" \
            --argjson packages "$(printf '%s\n' "$RELEASE_PACKAGE_NAMES" | jq -Rn '[inputs | select(length > 0)]')" \
            -f "$(dirname "${BASH_SOURCE[0]}")/../release/pin-exceptions.jq"
        return
    fi
    if [[ "$path" != Cargo.lock ]]; then
        sed "s/${previous//./\\.}/$release/g"
        return
    fi
    # Lockfile registry packages and checksums retain their validated selection,
    # even when their numeric version happens to equal the workspace version.
    ICYDB_RELEASE_NAMES="$RELEASE_PACKAGE_NAMES" perl -0777 -e '
        my ($previous, $release) = @ARGV;
        local $/; $_ = <STDIN>;
        my %owned = map { $_ => 1 } split /\n/, $ENV{ICYDB_RELEASE_NAMES};
        s{(\[\[package\]\]\n.*?)(?=\n\[\[package\]\]|\z)}{
            my $block = $1; my ($name) = $block =~ /^name = "([^"]+)"$/m;
            if ($owned{$name // ""} && $block !~ /^source = /m) {
                $block =~ s/^version = "\Q$previous\E"$/version = "$release"/m;
            }
            for my $package (keys %owned) {
                $block =~ s/"\Q$package $previous\E"/"$package $release"/g;
            }
            $block
        }gse;
        print $_;
    ' "$previous" "$release"
}

validate_changed_paths() {
    local diff_mode="$1"
    local base="$2"
    local head="${3:-}"
    local candidate_version="$4"
    local release_version="$5"
    local path
    local -a command=(git -C "$ROOT_DIR" diff --name-only -z)

    case "$diff_mode" in
        working)
            command+=("$base" --)
            ;;
        staged)
            command+=(--cached "$base" --)
            ;;
        commits)
            command+=("$base" "$head" --)
            ;;
        *)
            echo "Unknown diff mode: $diff_mode" >&2
            exit 1
            ;;
    esac

    RELEASE_PACKAGE_NAMES="$(
        cargo metadata --manifest-path "$ROOT_DIR/Cargo.toml" --locked --offline --no-deps --format-version 1 |
        perl -MJSON::PP -0777 -e '
            my $release = shift @ARGV;
            my $metadata = decode_json(<>);
            my %members = map { $_ => 1 } @{$metadata->{workspace_members}};
            my @names = map { $_->{name} }
                grep { $members{$_->{id}} && $_->{version} eq $release }
                @{$metadata->{packages}};
            die "no workspace package identity at release version\n" unless @names;
            print join("\n", @names), "\n";
        ' "$release_version"
    )"
    while IFS= read -r -d '' path; do
        if is_release_note_path "$path"; then
            continue
        fi
        case "$path" in
            Cargo.toml|Cargo.lock|README.md|*/Cargo.toml|ci/dependency-pinning-exceptions.json)
                ;;
            *)
                echo "Release transition contains non-version path: $path" >&2
                exit 1
                ;;
        esac

        case "$diff_mode" in
            working)
                if ! cmp -s \
                    <(git -C "$ROOT_DIR" show "$base:$path" | project_release_surface "$path" "$candidate_version" "$release_version") \
                    "$ROOT_DIR/$path"; then
                    echo "Release transition contains non-version changes in: $path" >&2
                    exit 1
                fi
                ;;
            staged)
                if ! cmp -s \
                    <(git -C "$ROOT_DIR" show "$base:$path" | project_release_surface "$path" "$candidate_version" "$release_version") \
                    <(git -C "$ROOT_DIR" show ":$path"); then
                    echo "Release transition contains non-version changes in: $path" >&2
                    exit 1
                fi
                ;;
            commits)
                if ! cmp -s \
                    <(git -C "$ROOT_DIR" show "$base:$path" | project_release_surface "$path" "$candidate_version" "$release_version") \
                    <(git -C "$ROOT_DIR" show "$head:$path"); then
                    echo "Release transition contains non-version changes in: $path" >&2
                    exit 1
                fi
                ;;
        esac
    done < <("${command[@]}")
}

verify_only_staged_release_notes() {
    local path

    while IFS= read -r -d '' path; do
        if ! is_release_note_path "$path"; then
            echo "Release-sensitive path is already staged: $path" >&2
            exit 1
        fi
    done < <(git -C "$ROOT_DIR" diff --cached --name-only -z HEAD --)
}

verify_no_untracked_paths() {
    local path

    while IFS= read -r -d '' path; do
        echo "Untracked path could affect release validation: $path" >&2
        exit 1
    done < <(git -C "$ROOT_DIR" ls-files --others --exclude-standard -z)
}

verify_tested_tree() {
    local tested_commit="$1"
    local current_commit path

    if [[ ! "$tested_commit" =~ ^[0-9a-f]{40}$ ]]; then
        echo "Invalid tested commit: $tested_commit" >&2
        exit 1
    fi

    current_commit="$(git -C "$ROOT_DIR" rev-parse --verify HEAD)"
    if [[ "$current_commit" != "$tested_commit" ]]; then
        echo "Candidate HEAD changed during the release gate" >&2
        exit 1
    fi
    verify_only_staged_release_notes
    verify_no_untracked_paths

    while IFS= read -r -d '' path; do
        if ! is_release_note_path "$path"; then
            echo "Test-sensitive path changed during the release gate: $path" >&2
            exit 1
        fi
    done < <(git -C "$ROOT_DIR" diff --name-only -z HEAD --)
}

checksum_diff() {
    if command -v sha256sum >/dev/null 2>&1; then sha256sum; else shasum -a 256; fi
}

diff_hash() {
    local diff_mode="$1"
    local base="$2"
    local head="${3:-}"

    case "$diff_mode" in
        working)
            git -C "$ROOT_DIR" diff --binary "$base" -- | checksum_diff | awk '{ print $1 }'
            ;;
        staged)
            git -C "$ROOT_DIR" diff --cached --binary "$base" -- | checksum_diff | awk '{ print $1 }'
            ;;
        commits)
            git -C "$ROOT_DIR" diff --binary "$base" "$head" -- | checksum_diff | awk '{ print $1 }'
            ;;
        *)
            echo "Unknown diff mode: $diff_mode" >&2
            exit 1
            ;;
    esac
}

read_receipt() {
    local receipt="$1"
    local receipt_bytes
    local -a lines

    receipt_bytes="$(wc -c < "$receipt")"
    if (( receipt_bytes > 1024 )); then
        echo "Oversized release candidate receipt: $receipt" >&2
        exit 1
    fi
    lines=()
    while IFS= read -r line || [[ -n "$line" ]]; do
        lines[${#lines[@]}]="$line"
        [[ ${#lines[@]} -le 6 ]] || break
    done < "$receipt"
    if [[ "${#lines[@]}" -ne 6 || "${lines[0]}" != "format=1" ]]; then
        echo "Malformed release candidate receipt: $receipt" >&2
        exit 1
    fi

    RECEIPT_BUMP="${lines[1]#bump=}"
    RECEIPT_CANDIDATE_COMMIT="${lines[2]#candidate_commit=}"
    RECEIPT_CANDIDATE_VERSION="${lines[3]#candidate_version=}"
    RECEIPT_RELEASE_VERSION="${lines[4]#release_version=}"
    RECEIPT_DIFF_HASH="${lines[5]#release_diff_sha256=}"

    if [[ "${lines[1]}" != "bump=$RECEIPT_BUMP" ||
          "${lines[2]}" != "candidate_commit=$RECEIPT_CANDIDATE_COMMIT" ||
          "${lines[3]}" != "candidate_version=$RECEIPT_CANDIDATE_VERSION" ||
          "${lines[4]}" != "release_version=$RECEIPT_RELEASE_VERSION" ||
          "${lines[5]}" != "release_diff_sha256=$RECEIPT_DIFF_HASH" ||
          ! "$RECEIPT_CANDIDATE_COMMIT" =~ ^[0-9a-f]{40}$ ||
          ! "$RECEIPT_DIFF_HASH" =~ ^[0-9a-f]{64}$ ]]; then
        echo "Malformed release candidate receipt: $receipt" >&2
        exit 1
    fi

    validate_transition "$RECEIPT_BUMP" "$RECEIPT_CANDIDATE_VERSION" "$RECEIPT_RELEASE_VERSION"
}

receipt_for_current_version() {
    local current_version

    current_version="$(bash "$VERSION_READER" --stable "$ROOT_DIR/Cargo.toml")" || exit 1
    validate_version "$current_version"
    printf '%s/v%s.candidate\n' "$RELEASE_RECEIPT_DIR" "$current_version"
}

record_receipt() {
    local bump="$1"
    local tested_commit="$2"
    local candidate_commit candidate_version release_version transition_hash receipt temporary_receipt

    case "$bump" in
        patch|minor|major) ;;
        *) usage ;;
    esac

    candidate_commit="$(git -C "$ROOT_DIR" rev-parse --verify HEAD)"
    if [[ ! "$tested_commit" =~ ^[0-9a-f]{40}$ || "$candidate_commit" != "$tested_commit" ]]; then
        echo "Current HEAD is not the candidate commit that completed the full gate" >&2
        exit 1
    fi
    candidate_version="$(bash "$SCRIPT_DIR/../release/read-committed-version.sh" "$ROOT_DIR" "$candidate_commit")" || exit 1
    release_version="$(bash "$VERSION_READER" --stable "$ROOT_DIR/Cargo.toml")" || exit 1
    validate_transition "$bump" "$candidate_version" "$release_version"

    verify_only_staged_release_notes
    if git -C "$ROOT_DIR" diff --quiet --ignore-submodules HEAD --; then
        echo "Release candidate receipt requires a version transition" >&2
        exit 1
    fi

    validate_changed_paths working HEAD "" "$candidate_version" "$release_version"
    transition_hash="$(diff_hash working HEAD)"
    mkdir -p "$RELEASE_RECEIPT_DIR"
    receipt="$RELEASE_RECEIPT_DIR/v$release_version.candidate"
    temporary_receipt="$receipt.tmp.$$"
    printf '%s\n' \
        "format=1" \
        "bump=$bump" \
        "candidate_commit=$candidate_commit" \
        "candidate_version=$candidate_version" \
        "release_version=$release_version" \
        "release_diff_sha256=$transition_hash" > "$temporary_receipt"
    mv "$temporary_receipt" "$receipt"

    echo "Recorded tested release candidate $candidate_commit for v$release_version"
}

verify_staged() {
    local current_commit current_version receipt staged_hash

    receipt="$(receipt_for_current_version)"
    if [[ ! -f "$receipt" ]]; then
        echo "No release candidate receipt for the current version" >&2
        exit 1
    fi
    read_receipt "$receipt"
    current_commit="$(git -C "$ROOT_DIR" rev-parse --verify HEAD)"
    current_version="$(bash "$VERSION_READER" --stable "$ROOT_DIR/Cargo.toml")" || exit 1

    if [[ "$current_commit" != "$RECEIPT_CANDIDATE_COMMIT" ||
          "$current_version" != "$RECEIPT_RELEASE_VERSION" ]]; then
        echo "Release candidate receipt does not match the staged transition" >&2
        exit 1
    fi
    if ! git -C "$ROOT_DIR" diff --quiet --ignore-submodules --; then
        echo "Unstaged changes remain after release staging" >&2
        exit 1
    fi
    if git -C "$ROOT_DIR" diff --cached --quiet --ignore-submodules HEAD --; then
        echo "No staged release transition" >&2
        exit 1
    fi

    validate_changed_paths staged HEAD "" \
        "$RECEIPT_CANDIDATE_VERSION" "$RECEIPT_RELEASE_VERSION"
    staged_hash="$(diff_hash staged HEAD)"
    if [[ "$staged_hash" != "$RECEIPT_DIFF_HASH" ]]; then
        echo "Staged release transition differs from the tested candidate receipt" >&2
        exit 1
    fi
}

verify_commit() {
    local release_commit parent_commit current_version receipt committed_hash

    # Recovery may select a historical release while HEAD contains newer fixes.
    # Validate that exact transition; newer source does not replace its receipt.
    release_commit="${RELEASE_COMMIT:-$(git -C "$ROOT_DIR" rev-parse --verify HEAD)}"
    [[ "$release_commit" =~ ^[0-9a-f]{40}$ ]] || { echo "Invalid selected release commit" >&2; exit 1; }
    git -C "$ROOT_DIR" merge-base --is-ancestor "$release_commit" HEAD || {
        echo "Selected release commit is not on the current history" >&2
        exit 1
    }
    current_version="$(bash "$SCRIPT_DIR/../release/read-committed-version.sh" "$ROOT_DIR" "$release_commit")" || exit 1
    validate_version "$current_version"
    receipt="$RELEASE_RECEIPT_DIR/v$current_version.candidate"
    if [[ ! -f "$receipt" ]]; then
        echo "No release candidate receipt for the current version" >&2
        exit 1
    fi
    read_receipt "$receipt"
    parent_commit="$(git -C "$ROOT_DIR" show -s --format=%P "$release_commit")"

    if [[ "$parent_commit" != "$RECEIPT_CANDIDATE_COMMIT" ||
          "$current_version" != "$RECEIPT_RELEASE_VERSION" ]]; then
        echo "Release commit is not the recorded candidate transition" >&2
        exit 1
    fi
    if ! git -C "$ROOT_DIR" diff --quiet --ignore-submodules HEAD -- ||
       ! git -C "$ROOT_DIR" diff --cached --quiet --ignore-submodules HEAD --; then
        echo "Release commit verification requires a clean tracked worktree" >&2
        exit 1
    fi
    if git -C "$ROOT_DIR" diff --quiet "$parent_commit" "$release_commit" --; then
        echo "Release commit contains no version transition" >&2
        exit 1
    fi

    validate_changed_paths commits "$parent_commit" "$release_commit" \
        "$RECEIPT_CANDIDATE_VERSION" "$RECEIPT_RELEASE_VERSION"
    committed_hash="$(diff_hash commits "$parent_commit" "$release_commit")"
    if [[ "$committed_hash" != "$RECEIPT_DIFF_HASH" ]]; then
        echo "Release commit differs from the tested candidate transition" >&2
        exit 1
    fi
}

case "${1:-}" in
    verify-tested-tree)
        [[ "$#" -eq 2 ]] || usage
        verify_tested_tree "$2"
        ;;
    record)
        [[ "$#" -eq 3 ]] || usage
        record_receipt "$2" "$3"
        ;;
    verify-staged)
        [[ "$#" -eq 1 ]] || usage
        verify_staged
        ;;
    verify-commit)
        [[ "$#" -eq 1 ]] || usage
        verify_commit
        ;;
    *)
        usage
        ;;
esac
