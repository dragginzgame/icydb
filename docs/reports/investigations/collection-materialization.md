> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# Collection materialization — nullable boundary correction

## Result and disposition

Retain the smaller null check as a correctness/simplification correction, not
as a measured collection-query speedup. Raw actor Wasm falls from **4,255,817
to 4,255,565 bytes (-252)**. All **54** measured messages have exactly the same
typed results, query instructions and explicit-call cycles as the
[frozen baseline](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/19/collection-materialization/01/report.md). Defined functions remain **10,133**.

The measurement fixture uses a named collection type and therefore the
canonical CatalogValue decoder, not the changed by-kind boundary. This run is
a matched actor-size comparison and a query non-regression control. It does
**not** measure the removed traversal's cost. The initial candidate selection
overestimated its relevance to this fixture; the result resolves that rather
than supporting an allocation-only or universal performance claim.

C1 concludes with this bounded correction and a **no-build decision for a
streaming predicate path**. The remaining collection cost is real, but no
measured allocation attribution or simpler accepted-admission replacement was
established. Existing ownership is retained. Reopening streaming needs new
evidence at the actual canonical consumer, not another query/decoder route
justified by these unchanged numbers. W1/ICYDB-033 remains awaiting Canic.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/19/collection-materialization/02/report.md).
