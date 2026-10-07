# Select one candidate; preserve historical notes, including undated versions
# at or below an explicitly supplied canonical previous version.
function historical(value, a, b, n) {
    if (previous == "") return 0
    split(value, a, "."); split(previous, b, ".")
    for (n = 1; n <= 3; n++) {
        if (length(a[n]) != length(b[n])) return length(a[n]) < length(b[n])
        if (("x" a[n]) != ("x" b[n])) return ("x" a[n]) < ("x" b[n])
    }
    return 1
}
{
    lines[NR] = $0
    heading = $0
    sub(/[ \t]+$/, "", heading)
    if (heading ~ /^## / && first == 0) first = NR
    if (heading == "## [" version "] - " date) { finalized++; finalized_line=NR }
    else if (index(heading, "## [" version "] - ") == 1) conflict = 1
    heading_version = substr($2, 2, length($2)-2)
    if (heading ~ /^## \[(Draft|[0-9]+\.[0-9]+\.[0-9]+)\]$/ &&
        ($2 == "[Draft]" || !historical(heading_version))) {
        drafts++
        start = NR
        if (heading != "## [Draft]" && heading != "## [" version "]") conflict = 1
    } else if (start && !finish && NR > start && heading ~ /^## /) {
        finish = NR
    }
}
END {
    if (conflict || drafts > 1 || (finalized &&
        (!allow_finalized || finalized != 1 || drafts || finalized_line != first))) {
        print "ambiguous or already finalized release candidate" > "/dev/stderr"
        exit 1
    }
    if (finalized) {
        for (i = 1; i <= NR; i++) print lines[i]
        exit
    }
    if (!first) first = NR + 1
    if (!finish) finish = NR + 1
    for (i = 1; i <= NR + 1; i++) {
        if (i == first) {
            print "## [" version "] - " date
            if (start) {
                for (j = start + 1; j < finish; j++) print lines[j]
            } else {
                print ""
            }
        }
        if (i <= NR && (!start || i < start || i >= finish)) print lines[i]
    }
}
