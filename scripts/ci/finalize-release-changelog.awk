# Shared Tooling owns a changelog version rather than Cargo package metadata.
# Select one undated candidate and preserve finalized historical sections.
{
    lines[NR] = $0
    if ($0 ~ /^## / && first == 0) first = NR
    if ($0 == "## [" version "] - " date) finalized = 1
    if ($0 ~ /^## \[(Draft|[0-9]+\.[0-9]+\.[0-9]+)\]$/) {
        drafts++
        start = NR
        if ($0 != "## [Draft]" && $0 != "## [" version "]") conflict = 1
    } else if (start && !finish && NR > start && $0 ~ /^## /) {
        finish = NR
    }
}
END {
    if (finalized || conflict || drafts > 1) {
        print "ambiguous or already finalized release candidate" > "/dev/stderr"
        exit 1
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
