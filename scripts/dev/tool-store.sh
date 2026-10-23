#!/usr/bin/env bash
# Shared companions: scripts/ci/verify-file-checksum.sh
# shellcheck disable=SC2154 # Caller owns consumer, tool_root, lock and active.
# Sourced by installers and evidence selection. No effects until a function is called.

# Explicit host/IC/CI asset setup retries inside curl's transient classifier.
# The retry window limits new attempts; an in-flight attempt keeps its own cap.
tool_store_download() {
    curl --proto '=https' --proto-redir '=https' --tlsv1.2 -fsSL \
        --connect-timeout 15 --max-time 300 \
        --retry 3 --retry-delay 2 --retry-max-time 60 -o "$2" "$1"
}

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

# Admit the checkout pointer before reuse or repair. An absent pointer prints
# nothing; an unmanaged pointer is fatal even during explicit installation.
tool_store_active_reference() {
    local selection
    [[ ! -L "$tool_root" ]] || { echo 'repository .tools may not be a symlink' >&2; return 1; }
    if [[ -L "$active" ]]; then
        selection="$(perl -e 'my $p=readlink(shift); defined $p or exit 1; print "$p/."' "$active")" || return $?
        selection="${selection%/.}"
        [[ "$selection" =~ ^$store_kind-set\.[[:alnum:]]+$ ]] || {
            printf 'unmanaged %s tool selection: %s\n' "$store_kind" "$active" >&2; return 1;
        }
        printf '%s\n' "$tool_root/$selection"
    elif [[ -e "$active" ]]; then
        printf 'refusing to replace unmanaged tool path: %s\n' "$active" >&2; return 1
    fi
}

# Return a verified current bundle, or empty output when explicit setup is
# needed. An unmanaged checkout pointer remains fatal, never a repair candidate.
# Verifiers retain their own authentication, probes and diagnostics.
tool_store_verified_selection() {
    local verifier="$1" selection bundle
    selection="$(tool_store_active_reference)" || return $?
    if [[ -n "$selection" ]] &&
        bundle="$(tool_store_resolve "$selection" "$store_slot")" &&
        tool_store_unlocked && "$verifier" "$bundle"; then
        printf '%s\n' "$bundle"
    fi
    return 0
}

# mkdir remains the atomic acquisition. Keep its diagnostic/status verbatim;
# an observed path is not proof of a live owner or permission to remove it.
tool_store_lock_directory() {
    local status
    if mkdir "$1"; then return 0; else status=$?; fi
    if [[ -e "$1" || -L "$1" ]]; then
        printf 'tool installation lock path already exists: %s\n' "$1" >&2
    else
        printf 'unable to acquire tool installation lock: %s\n' "$1" >&2
    fi
    return "$status"
}

# Explicit Cargo setup may wait for an admitted owner of this exact slot.
# Failed atomic acquisition is never retried: path existence cannot classify
# mkdir's error, and another process may win after the observed owner releases.
tool_store_wait_unlocked() {
    local limit="$1" waited=0 started=$SECONDS owner previous=''
    tool_store_waited=false
    [[ "$limit" != 0 ]] || return 0
    while [[ -e "$store_slot/install.lock" || -L "$store_slot/install.lock" ]]; do
        tool_store_directory "$store_root" && tool_store_directory "$store_slot" || return 1
        # Existing owners may use their caller's umask inside the admitted slot.
        # Inspect actual access, ownership and file kinds, not a new mode policy.
        [[ -d "$store_slot/install.lock" && ! -L "$store_slot/install.lock" &&
           -O "$store_slot/install.lock" && -r "$store_slot/install.lock" &&
           -w "$store_slot/install.lock" && -x "$store_slot/install.lock" &&
           -w "$store_slot" && -x "$store_slot" ]] || {
            printf 'Cannot wait on inaccessible or redirected tool installation lock: %q\n' "$store_slot/install.lock" >&2
            return 1
        }
        owner="$(perl -e '
            my $p=shift; my @s=lstat($p);
            @s && -f _ && !-l _ && $s[4]==$<
                or die "invalid tool installation owner record: $p\n";
            open my $f, "<", $p or die "read tool installation owner: $!\n";
            my $n=read($f, my $text, 8193);
            defined($n) && $n<=8192 &&
                $text =~ /\Apid=([1-9][0-9]*)\nconsumer=(\/[^\0\r\n]+)\n\z/
                or die "malformed tool installation owner record: $p\n";
            print "pid=$1 consumer=$2";
            close $f or die "close tool installation owner: $!\n";
        ' "$store_slot/install.lock/owner")" || return 1
        if [[ "$owner" != "$previous" ]]; then
            printf 'Waiting for tool installation slot: %q owner=%q (limit %ss)\n' "$store_slot" "$owner" "$limit" >&2
            previous="$owner"
        fi
        if [[ "$waited" -ge "$limit" ]]; then
            printf 'Tool installation wait timed out after %ss: %q\n' "$((SECONDS - started))" "$store_slot" >&2
            return 1
        fi
        tool_store_waited=true
        sleep 1 || return $?
        waited=$((waited + 1))
    done
    if [[ "$tool_store_waited" == true ]]; then
        printf 'Tool installation slot released after %ss: %q; rechecking inputs\n' "$((SECONDS - started))" "$store_slot" >&2
    fi
}

tool_store_acquire() {
    [[ ! -L "$store_root" && ! -L "$store_slot" ]] || { echo 'redirected tool store' >&2; return 1; }
    if [[ -e "$store_root" ]]; then tool_store_directory "$store_root" || return 1; fi
    if [[ -e "$store_slot" ]]; then tool_store_directory "$store_slot" || return 1; fi
    (umask 077; mkdir -p "$store_slot") || return $?
    tool_store_directory "$store_root" && tool_store_directory "$store_slot" || return 1
    tool_store_wait_unlocked "${1:-0}" || return $?
    tool_store_lock_directory "$store_slot/install.lock" || return $?
    store_lock="$store_slot/install.lock"
    printf 'pid=%s\nconsumer=%s\n' "$$" "$consumer" > "$store_lock/owner"
}

tool_store_release() {
    if [[ -n "${store_lock:-}" ]]; then
        rm -f "$store_lock/owner" "$store_lock/selected" || return $?
        rmdir "$store_lock" || return $?
        store_lock=''
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
    reference="$(mktemp -d "$tool_root/$store_kind-set.XXXXXX")" || return $?
    rmdir "$reference" || return $?
    ln -s "$bundle" "$reference" || return $?
    store_reference="${reference##*/}"
}

tool_store_new() {
    stage="$(mktemp -d "$store_slot/set.XXXXXX")" || return $?
    tool_store_reference "$stage"
}

# Host/IC setup owns this whole process. Register cleanup only after acquiring
# the checkout lock; failure must never remove another installer's lock.
tool_store_prepare_install() {
    local verifier="$1" bundle
    mkdir -p "$tool_root" || return $?
    lock="$tool_root/.$store_kind-tools.lock"
    tool_store_lock_directory "$lock" || return $?
    stage=''
    trap 'tool_store_finish_install "$?"' EXIT
    trap 'exit 130' INT
    trap 'exit 143' TERM
    printf 'pid=%s\nconsumer=%s\n' "$$" "$consumer" > "$lock/owner" || return $?
    tool_store_acquire || return $?
    if bundle="$(tool_store_cached)" && "$verifier" "$bundle"; then
        tool_store_reference "$bundle" || return $?
        tool_store_activate "$bundle" || return $?
    else
        tool_store_new || return $?
    fi
    # An empty stage means the verified cached bundle is already active. A new
    # stage stays unselected until the caller's download/verification succeeds.
}

tool_store_finish_install() {
    local status="$1" cleanup_status=0
    trap - EXIT
    rm -f "$lock/selected" "$lock/owner" || cleanup_status=$?
    rmdir "$lock" || cleanup_status=$?
    # Attempt both releases even if checkout cleanup failed. Preserve an earlier
    # installation failure; a cleanup-only failure must still fail the command.
    tool_store_release || cleanup_status=$?
    [[ "$status" != 0 ]] || status=$cleanup_status
    if [[ "$status" != 0 && -n "$stage" ]]; then
        echo "Failed $store_kind tool installation retained: $stage" >&2
    fi
    exit "$status"
}

tool_store_activate() {
    local bundle="$1"
    [[ -n "${store_lock:-}" && "$store_lock" == "$store_slot/install.lock" &&
       -d "$store_lock" && ! -L "$store_lock" && -d "$lock" && ! -L "$lock" ]] || {
        echo 'tool activation requires acquired store and checkout locks' >&2; return 1;
    }
    # The store's current pointer is a reuse hint; consumers point directly at
    # immutable generations, so updating the hint never upgrades another repo.
    [[ ! -e "$store_slot/current" || -L "$store_slot/current" ]] || { echo 'unmanaged tool store selection' >&2; return 1; }
    [[ ! -e "$active" || -L "$active" ]] || { echo 'unmanaged local tool selection' >&2; return 1; }
    ln -s "${bundle##*/}" "$store_lock/selected" || return $?
    perl -e 'rename($ARGV[0],$ARGV[1]) or die "select tool store bundle: $!\n"' "$store_lock/selected" "$store_slot/current" || return $?
    ln -s "$store_reference" "$lock/selected" || return $?
    perl -e 'rename($ARGV[0],$ARGV[1]) or die "activate tool bundle: $!\n"' "$lock/selected" "$active"
}
