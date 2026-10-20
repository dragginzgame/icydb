# Select one candidate; preserve historical notes, including undated versions
# at or below an explicitly supplied canonical previous version.
# A non-consuming separator reads the text whole, including its terminal LF.
# Supported host awks implement regular-expression RS; no GNU RT dependency.
BEGIN { RS = "^$" }
function reject(reason, number) {
    conflict = 1
    if (diagnostic == "") {
        diagnostic = reason
        offending_line = number
    }
}
function historical(value, a, b, n) {
    if (previous == "") return 0
    split(value, a, "."); split(previous, b, ".")
    for (n = 1; n <= 3; n++) {
        if (length(a[n]) != length(b[n])) return length(a[n]) < length(b[n])
        if (("x" a[n]) != ("x" b[n])) return ("x" a[n]) < ("x" b[n])
    }
    return 1
}
function emit(i) {
    printf "%s", lines[i]
    if (i < count || final_lf) printf "\n"
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
        if (heading ~ /^## / && first == 0) {
            first = line
            top = heading
        }
        if (heading == "## [Draft]") reject("unnumbered draft heading", line)
        if (line != first && (heading == "## [" version "]" ||
            index(heading, "## [" version "] - ") == 1))
            reject("duplicate or misplaced selected candidate", line)
        if (heading == "## [" version "] - " date) { finalized++; finalized_line=line }
        else if (index(heading, "## [" version "] - ") == 1 &&
            heading != "## [" version "] - Unreleased")
            reject("conflicting selected candidate date or suffix", line)
        split(heading, fields, " ")
        heading_version = substr(fields[2], 2, length(fields[2])-2)
        if (heading ~ /^## \[[0-9]+\.[0-9]+\.[0-9]+\]( - Unreleased)?$/ &&
            !historical(heading_version)) {
            drafts++
            start = line
            if (heading != "## [" version "]" && heading != "## [" version "] - Unreleased")
                reject("competing pending version", line)
        } else if (start && !finish && line > start && heading ~ /^## /) {
            finish = line
        }
    }
}
END {
    # Direct callers must not turn retained previous history into a downgrade.
    if (previous != "" && historical(version))
        reject("selected version " version " must be greater than previous version " previous, 0)
    if (!first || (top != "## [" version "]" &&
        top != "## [" version "] - Unreleased" &&
        !(allow_finalized && top == "## [" version "] - " date) &&
        !(previous != "" && (top == "## [" previous "]" ||
            index(top, "## [" previous "] - ") == 1))))
        reject("unexpected first release heading", first)
    if (input_error || conflict || drafts > 1 || (finalized &&
        (!allow_finalized || finalized != 1 || drafts || finalized_line != first))) {
        if (diagnostic == "") {
            diagnostic = "ambiguous or already finalized release candidate"
            offending_line = first
        }
        print "changelog: " diagnostic > "/dev/stderr"
        if (offending_line)
            print "line " offending_line ": " lines[offending_line] > "/dev/stderr"
        print "expected pending heading: ## [" version "]" > "/dev/stderr"
        print "optional pending suffix: - Unreleased (replaced with the release date during preparation)" > "/dev/stderr"
        if (allow_finalized)
            print "exact-date recovery heading: ## [" version "] - " date > "/dev/stderr"
        exit 1
    }
    if (finalized) {
        for (i = 1; i <= count; i++) emit(i)
        exit
    }
    if (!finish) finish = count + 1
    for (i = 1; i <= count + 1; i++) {
        if (i == first) {
            print "## [" version "] - " date
            if (start) {
                for (j = start + 1; j < finish; j++) emit(j)
            } else {
                print ""
            }
        }
        if (i <= count && (!start || i < start || i >= finish)) emit(i)
    }
}
