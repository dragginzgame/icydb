# Module surface hardening

Apply the [common audit contract](README.md). Review a named module or crate's
retained/exposed code, generated consumers and runtime shape. This is a targeted
reachability and authority review; [code hygiene](code-hygiene.md) owns ordinary
quality checks, and [flow convergence](flow-convergence-and-duplication.md) owns
duplicated semantic flows. Use [module cleanup](module-cleanup.md) only when
implementation is authorized.

## Inventory and authority

Inventory public/restricted items, re-exports, hidden/generated boundaries,
cfg-gated branches, one-caller helpers, diagnostics and test-only consumers.
Group adjacent items with one responsibility rather than requiring line-by-line
prose. Inspect callers, macro expansion/generated artifacts, runtime registration,
features and tests; absence from a text search is not deletion proof.

For each candidate ask what current invariant or supported caller would fail
if it disappeared or became narrower, and whether that caller itself has current
authority. Product overlays supply accepted runtime sources, facade contracts,
generated boundary names and retained-state/recovery obligations.

Classify as live authority, live generated boundary, live diagnostics, live test
support, stale compatibility, stale generated fallback, orphaned helper,
overexposed internal, duplicate surface or unclear. Test-only code is not dead
merely because production does not call it; distinguish useful test support from
tests that unnecessarily keep production APIs public.

## Proof before disposition

| Confidence | Evidence and permitted recommendation |
| --- | --- |
| High | No maintained external/generated/runtime role; inspected callers and focused checks can prove deletion. |
| Medium | Current consumers can move to a canonical owner without changing their contract; name that change and proof. |
| Low | Public, generated or runtime authority is involved, or consumer evidence is incomplete; resolve the uncertainty before deletion. |
| Blocked | An owner decision or required artifact/proof is unavailable; name the missing evidence. |

Always inspect facade changes, persisted formats, lifecycle hooks, backup/restore,
unfinished effects and generated exports at their real boundaries. Apply the
baseline's compatibility and hard-cut rules without discarding retained state or
same-contract recovery. Reachability alone does not establish that removal is safe.

Classify runtime shape as cold, warm, hot, encode/decode-sensitive, Wasm-sensitive
or test-only, allowing relevant combinations. A shorter implementation can add
allocation, clone/formatting work, dynamic dispatch or generic code growth.
Record that risk before proposing a hot-path rewrite. Require the consumer's
approved metric and comparable inputs; source review cannot claim a performance
win. For Wasm consumers record raw deployable bytes when size is the question;
compressed size is a separate measure.

## Result

Use dispositions: delete, narrow, inline, move owner, move to test, retain with
owner, defer with named trigger, retain hot path, measure first, patch with named
proof, reject cleanup, or blocked. These are recommendations, not new authority.

The report pairs each candidate with current owner, consumers, class, confidence,
runtime risk, disposition and required proof. Keep the report compact for simple
internal modules; expand evidence at actual public/generated/persisted/recovery
boundaries. Include intentional retention and unresolved uncertainty. Do not
delete useful diagnostics, boundary checks or optimized structure to reduce LOC.
