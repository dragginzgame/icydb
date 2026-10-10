#!/usr/bin/env bash
# Shared companions: scripts/dev/install-host-tools.sh scripts/dev/install-ic-tools.sh
set -euo pipefail

# Emit NUL-separated ROOT/PATH pairs for the neutral evidence archiver.
# Full retention is the default. Compact retention rechecks the current complete
# host/IC sets with caller pins; each failed or unselected candidate stays full.
[[ $# == 3 || $# == 5 ]] || { echo 'usage: select-tool-evidence.sh full|compact REPOSITORY DIAGNOSTICS [HOST-VERSIONS IC-PINS]' >&2; exit 2; }
mode="$1"
[[ ( "$mode" == full && $# == 3 ) || ( "$mode" == compact && $# == 5 ) ]] || { echo 'invalid tool evidence selection' >&2; exit 2; }
ROOT="${BASH_SOURCE[0]}"
[[ "$ROOT" == /* ]] || ROOT="$PWD/$ROOT"
ROOT="$(cd -P "${ROOT%/*}/../.." && printf '%s/.' "$PWD")"
ROOT="${ROOT%/.}"
repository="$2"
[[ "$repository" == /* ]] || repository="$PWD/$repository"
diagnostics="$3"
[[ -d "$repository" && -d "$diagnostics" && ! -L "$diagnostics" ]] || { echo 'existing evidence roots required' >&2; exit 1; }
[[ "$diagnostics" == /* ]] || diagnostics="$PWD/$diagnostics"
identity() {
    perl -e 'my @s=lstat($ARGV[0]); @s or die "cannot identify bundle: $!\n"; print "$s[0]:$s[1]:$s[2]"' "$1"
}
selection() {
    perl -e 'my $s=readlink($ARGV[0]); defined($s) or exit 1; print $s,"/."' "$1"
}
shopt -s nullglob
for kind in host ic; do
    compact=""
    active="$repository/.tools/$kind"
    if [[ "$mode" == compact && ! -L "$repository/.tools" && -L "$active" ]]; then
        selected="$(selection "$active")" || selected=""
        selected="${selected%/.}"
        if [[ "$selected" =~ ^$kind-set\.[[:alnum:]]+$ && -d "$repository/.tools/$selected" && ! -L "$repository/.tools/$selected" ]]; then
            bundle="$repository/.tools/$selected"
            pins="$4"; [[ "$kind" != ic ]] || pins="$5"
            if [[ -f "$pins" && ! -L "$pins" ]]; then
                detail="$diagnostics/$kind"
                mkdir "$detail"
                cp -p "$pins" "$detail/caller-pins"
                before="$(identity "$bundle")" || before=""
                checker=(bash "$ROOT/scripts/dev/install-$kind-tools.sh" --consumer "$repository" --check)
                if [[ "$kind" == host ]]; then
                    checker+=(--versions "$detail/caller-pins")
                else
                    checker+=(--pins "$detail/caller-pins")
                fi
                if "${checker[@]}" > "$detail/check.log" 2>&1 &&
                    [[ -n "$before" && ! -L "$repository/.tools" && -d "$bundle" && ! -L "$bundle" && "$(identity "$bundle")" == "$before" &&
                       "$(selection "$active")" == "$selected/." ]] && cmp -s "$pins" "$detail/caller-pins"; then
                    if [[ "$kind" == host || ( -f "$bundle/pins.tsv" && ! -L "$bundle/pins.tsv" &&
                          -f "$bundle/host" && ! -L "$bundle/host" &&
                          -f "$bundle/files.sha256" && ! -L "$bundle/files.sha256" ) ]]; then
                        compact="$selected"
                        printf '%s\n' "$repository/.tools/$selected" > "$detail/selection.txt"
                        if [[ "$kind" == ic ]]; then
                            for receipt in pins.tsv host files.sha256; do cp -p "$bundle/$receipt" "$detail/$receipt"; done
                        fi
                    fi
                fi
                if [[ -z "$compact" ]]; then echo 'Full bundle retained: verification or selection changed' >> "$detail/check.log"; fi
            fi
        fi
    fi
    for bundle in "$repository/.tools/$kind-set."*; do
        [[ "${bundle##*/}" != "$compact" ]] || continue
        printf '%s\0%s\0' "$repository" ".tools/${bundle##*/}"
    done
done
