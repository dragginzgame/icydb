# Canonical admission for the common IC matrix. Optional tool projects its version;
# records emits the validated selection (callers sort it for comparison).
BEGIN { FS = "\t" }
/^#/ { next }
NF != 4 { bad=1; next }
$1 !~ /^(quill|icp|didc|ic-wasm|wasm-opt)$/ { bad=1 }
$3 !~ /^(linux-x86_64|darwin-x86_64|darwin-arm64)$/ { bad=1 }
length($4) != 64 || $4 ~ /[^0-9a-f]/ { bad=1 }
$1 == "wasm-opt" && $2 !~ /^[1-9][0-9]*$/ { bad=1 }
$1 != "wasm-opt" && $2 !~ /^[0-9]+\.[0-9]+\.[0-9]+$/ { bad=1 }
{
    if (seen[$1 SUBSEP $3]++) bad=1
    if ($1 in versions && versions[$1] != $2) bad=1
    versions[$1]=$2; count[$3]++
    rows[$1 SUBSEP $3]=$1 "\t" $2 "\t" $3 "\t" $4
}
END {
    if (bad || count["linux-x86_64"] != 5 || count["darwin-x86_64"] != 5 || count["darwin-arm64"] != 5) exit 1
    if (tool != "") {
        if (!(tool in versions)) exit 1
        print versions[tool]
    }
    if (records) for (key in rows) print rows[key]
}
