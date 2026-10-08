# Select one candidate; preserve historical notes, including undated versions
# at or below an explicitly supplied canonical previous version.
# A non-consuming separator reads the text whole, including its terminal LF.
# Supported host awks implement regular-expression RS; no GNU RT dependency.
BEGIN { RS = "^$" }
function historical(value, a, b, n) {
    if (previous == "") return 0
    split(value, a, "."); split(previous, b, ".")
    for (n = 1; n <= 3; n++) {
        if (length(a[n]) != length(b[n])) return length(a[n]) < length(b[n])
        if (("x" a[n]) != ("x" b[n])) return ("x" a[n]) < ("x" b[n])
    }
    return 1
}
function emit(i, followed) {
    printf "%s", lines[i]
    if (i < count || final_lf || followed) printf "\n"
}
{
    if (NR != 1) { input_error = 1; next }
    count = split($0, lines, "\n")
    final_lf = substr($0, length($0), 1) == "\n"
    if (final_lf) count--
    for (line = 1; line <= count; line++) {
        heading = lines[line]
        sub(/[ \t]+$/, "", heading)
        gsub(/[ \t]+/, " ", heading)
        if (heading ~ /^## / && first == 0) first = line
        if (heading == "## [" version "] - " date) { finalized++; finalized_line=line }
        else if (index(heading, "## [" version "] - ") == 1) conflict = 1
        split(heading, fields, " ")
        heading_version = substr(fields[2], 2, length(fields[2])-2)
        if (heading ~ /^## \[(Draft|[0-9]+\.[0-9]+\.[0-9]+)\]$/ &&
            (fields[2] == "[Draft]" || !historical(heading_version))) {
            drafts++
            start = line
            if (heading != "## [Draft]" && heading != "## [" version "]") conflict = 1
        } else if (start && !finish && line > start && heading ~ /^## /) {
            finish = line
        }
    }
}
END {
    if (input_error || conflict || drafts > 1 || (finalized &&
        (!allow_finalized || finalized != 1 || drafts || finalized_line != first))) {
        print "ambiguous or already finalized release candidate" > "/dev/stderr"
        exit 1
    }
    if (finalized) {
        for (i = 1; i <= count; i++) emit(i, 0)
        exit
    }
    if (!first) first = count + 1
    if (!finish) finish = count + 1
    for (i = 1; i <= count + 1; i++) {
        if (i == first) {
            print "## [" version "] - " date
            if (start) {
                for (j = start + 1; j < finish; j++) emit(j, start > first)
            } else {
                print ""
            }
        }
        if (i <= count && (!start || i < start || i >= finish)) emit(i, first == count + 1)
    }
}
