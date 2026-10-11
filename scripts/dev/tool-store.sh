#!/usr/bin/env bash
# Shared companions: scripts/ci/verify-file-checksum.sh
# shellcheck disable=SC2154 # Caller owns consumer, tool_root, lock and active.
# Sourced by installers and evidence selection. No effects until a function is called.

# Resolve the configured root without creating it. Existing ancestor aliases
# (notably macOS /var) are allowed; the managed root itself may not be a link.
tool_store_root() {
    local configured="${SHARED_TOOLING_STORE:-${XDG_DATA_HOME:-$HOME/.local/share}/shared-tooling/tools}"
    perl -MCwd=abs_path -MFile::Basename=dirname,basename -e '
        my $p=shift;
        $p =~ m{^/} && $p !~ /[\r\n]/ or die "tool store must be an absolute path without CR/LF\n";
        $p !~ m{(?:^|/)\.{1,2}(?:/|$)} or die "tool store may not contain dot components\n";
        $p =~ s{/+\z}{};
        length($p) or die "tool store may not be the filesystem root\n";
        -l $p and die "tool store root may not be a symlink\n";
        my @tail;
        while (!-e $p) {
            -l $p and die "dangling tool store ancestor\n";
            unshift @tail, basename($p); $p=dirname($p);
        }
        -d $p or die "tool store ancestor is not a directory\n";
        $p=abs_path($p); defined $p or die "cannot resolve tool store\n";
        $p=join("/",$p,@tail); $p =~ s{/+}{/}g;
        $p !~ /[\r\n]/ or die "resolved tool store contains CR/LF\n";
        print "$p/.";
    ' "$configured"
}

tool_store_init() {
    store_kind="$1"
    case "${consumer:-}" in *$'\n'*|*$'\r'*) echo 'consumer path contains CR/LF' >&2; return 1 ;; esac
    store_root="$(tool_store_root)" || return 1
    store_root="${store_root%/.}"
    store_id="$(printf '%s\n' "$store_kind-bundle-1" "$2" | perl -MDigest::SHA=sha256_hex -0777 -ne 'print sha256_hex($_)')" || return 1
    store_slot="$store_root/$store_kind-$store_id"
    store_lock=""
}

tool_store_directory() {
    if [[ -d "$1" && ! -L "$1" && -O "$1" ]] &&
        perl -e 'exit(((stat(shift))[2] & 0022) ? 1 : 0)' "$1"; then return 0; else
        printf 'tool store directory must be physical and owned by this user: %s\n' "$1" >&2; return 1;
    fi
}

# Resolve only our generated reference, never an arbitrary symlink target.
# An optional slot binds installation/check admission to the requested pins.
tool_store_resolve() {
    local reference="$1" expected="${2:-}" target relative slot generation
    [[ -L "$reference" ]] || return 1
    target="$(perl -e 'my $p=readlink(shift); defined $p or exit 1; print "$p/."' "$reference")" || return 1
    target="${target%/.}"
    [[ "$target" == "$store_root/"* ]] || return 1
    relative="${target#"$store_root/"}"
    slot="${relative%%/*}"; generation="${relative#*/}"
    [[ "$slot" =~ ^$store_kind-[0-9a-f]{64}$ && "$generation" =~ ^set\.[[:alnum:]]+$ ]] || return 1
    [[ -z "$expected" || "$store_root/$slot" == "$expected" ]] || return 1
    tool_store_directory "$store_root" && tool_store_directory "$store_root/$slot" &&
        tool_store_directory "$target" || return 1
    printf '%s\n' "$target"
}

tool_store_acquire() {
    [[ ! -L "$store_root" && ! -L "$store_slot" ]] || { echo 'redirected tool store' >&2; return 1; }
    if [[ -e "$store_root" ]]; then tool_store_directory "$store_root" || return 1; fi
    if [[ -e "$store_slot" ]]; then tool_store_directory "$store_slot" || return 1; fi
    (umask 077; mkdir -p "$store_slot") || return 1
    tool_store_directory "$store_root" && tool_store_directory "$store_slot" || return 1
    mkdir "$store_slot/install.lock" 2>/dev/null || { echo "tool store installation locked: $store_slot/install.lock" >&2; return 1; }
    store_lock="$store_slot/install.lock"
    printf 'pid=%s\nconsumer=%s\n' "$$" "$consumer" > "$store_lock/owner"
}

tool_store_release() {
    if [[ -n "${store_lock:-}" ]]; then
        rm -f "$store_lock/owner" "$store_lock/selected"
        rmdir "$store_lock"
    fi
}

tool_store_unlocked() {
    [[ ! -e "$store_slot/install.lock" && ! -L "$store_slot/install.lock" ]] || {
        printf 'tool store installation locked: %s/install.lock; retry after its owner finishes\n' "$store_slot" >&2
        return 1
    }
}

tool_store_cached() {
    local name
    [[ -L "$store_slot/current" ]] || return 1
    name="$(perl -e 'my $p=readlink(shift); defined $p or exit 1; print "$p/."' "$store_slot/current")" || return 1
    name="${name%/.}"
    [[ "$name" =~ ^set\.[[:alnum:]]+$ ]] || return 1
    tool_store_directory "$store_slot/$name" || return 1
    printf '%s\n' "$store_slot/$name"
}

# Record each attempt/reference locally before downloads so failure collection
# can select only this checkout's evidence, without scanning the entire store.
tool_store_reference() {
    local bundle="$1" reference
    reference="$(mktemp -d "$tool_root/$store_kind-set.XXXXXX")" || return 1
    rmdir "$reference" || return 1
    ln -s "$bundle" "$reference" || return 1
    store_reference="${reference##*/}"
}

tool_store_new() {
    stage="$(mktemp -d "$store_slot/set.XXXXXX")" || return 1
    tool_store_reference "$stage"
}

tool_store_activate() {
    local bundle="$1"
    # The store's current pointer is a reuse hint; consumers point directly at
    # immutable generations, so updating the hint never upgrades another repo.
    [[ ! -e "$store_slot/current" || -L "$store_slot/current" ]] || { echo 'unmanaged tool store selection' >&2; return 1; }
    [[ ! -e "$active" || -L "$active" ]] || { echo 'unmanaged local tool selection' >&2; return 1; }
    ln -s "${bundle##*/}" "$store_lock/selected" || return 1
    perl -e 'rename($ARGV[0],$ARGV[1]) or die "select tool store bundle: $!\n"' "$store_lock/selected" "$store_slot/current" || return 1
    ln -s "$store_reference" "$lock/selected" || return 1
    perl -e 'rename($ARGV[0],$ARGV[1]) or die "activate tool bundle: $!\n"' "$lock/selected" "$active"
}
