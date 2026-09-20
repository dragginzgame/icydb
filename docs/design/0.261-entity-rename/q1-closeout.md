# Q1 — Entity rename closeout

2026-09-20. **Ready within the agreed 0.261 entity-rename scope.**
E1, E2, E3 and the explicitly authorised I1 investigation are complete.
This is a reviewable worktree verdict, not release publication or full-suite
certification. Findings were reported before recording this closeout; no
production correction was required by the audit.

The user subsequently authorised the D1 `ic-timers` 0.8.0 update. This Q1
measurement record used 0.7.1; the [tracker](0.261-status.md) owns the dependency
follow-up and its current-candidate validation. Historical measurements below
are not relabelled as measurements of the updated dependency.

## Delivered contract

- Existing `from_name` performs the same-store populated Item to CatalogItem
  rename. The production correction admits only explicitly versioned companion
  transitions whose complete accepted source meaning is reproduced by reversing
  same-plan relation-target names. Other differences and meaningless bumps reject.
- Native coverage preserves accepted identities, exact row/index/reverse bytes,
  layout/generations, lineage and row-mutation revision. It enforces relations,
  rejects stale bindings/cursors and invalid transitions, and exercises exact
  replay plus interruption of the existing compound publication marker.
- Generated actors preserve rows, indexed lookup, typed bindings, constraints
  and exact receipts across upgrade, retry and restart. Both rename scenarios
  report zero row rewrites and zero rebuilt indexes. The existing field/cast
  rehearsal remains intact.
- There is one accepted catalog, publication/recovery flow and current version-1
  encoding. No alias, generated-model runtime fallback, new persisted state,
  migration opcode, or production execution mode was introduced.

## Findings and retained limitations

The [I1 controls](i1-read-investigation.md) explain the previously unattributed
read increase: accepted runtime/catalog preparation recurs in query-only
traffic. Repeated post-publication SQL reads cost 34.59–34.65 million instructions;
after one normally returning update that catches restrictive-delete errors,
they cost 2.04–2.20 million with unchanged rows and accepted description.
Restart alone still costs about 35.6 million. This cost remains in the product;
I1 diagnoses it and does not optimize it. The qualification set no numerical
cost acceptance gate, so it is an explicit scope limitation rather than an
unresolved correctness blocker.

Production preparation policy is a separately reviewable follow-up if requested.
Any proposed change must measure both the replicated-call cost and subsequent
query savings through the existing runtime owner. Per-function instruction
attribution, migration-body instructions and production composed/release Wasm
size remain unmeasured. No application deployment or downstream qualification
is implied by these fixture results.

Both historical E3 artifacts remain available and their sizes/BLAKE3 hashes
match the recorded evidence. I1 actors use a different Cargo-home path:
8,922,245 / 8,953,795 raw bytes, a source-to-successor delta of +31,550 bytes.
The longer embedded paths explain 3,051 bytes of each actor's difference from
E3; residual differences are left unattributed. Command-envelope cycle counts
match E3 exactly. These are test-actor build/lifecycle comparisons, not runtime
optimization deltas.

## Validation and handoff

Fresh validation passes: six native populated/admission/recovery tests, eleven
planner tests, the occupied-target source-binding test, two source-digest tests,
seven migration tests, four macro tests and one grammar target containing three
UI cases: **32 Rust tests**. Both refined generated rename scenarios also pass,
for **34 selected tests** in the final evidence. The earlier repeated-query
experiment passed both actor scenarios as well.

The three unchanged physical-cast scenarios and all-feature actor/schema lint
results are retained from E3; they were not rerun for the host-only investigation.
The affected integration lint gate passes freshly. Formatting, whitespace and
local documentation links pass. Trybuild emits the existing non-blocking unused
workspace-patch warnings for crates outside its fixture graph. Full repository
and workspace suites remain user-owned.

I1/Q1 changes one host test file and eight documentation files, approximately
+280 net lines, including +52 Rust lines. Test observation complexity increases
modestly; production runtime shape and state space are unchanged from E2/E3.
Two disposable PocketIC servers were started and stopped; the initial direct
wrapper invocation failed before startup and was corrected by using Bash.
No application network changed. Cargo versions and dependency pins remain
unchanged at the 0.260.0 baseline; active release notes remain 0.261.0.
No commits, pushes, tags or publication were performed.

The [tracker](0.261-status.md), [design](0.261-design.md),
[migration guide](../../guides/schema-migrations.md),
[root notes](../../../CHANGELOG.md) and [detailed notes](../../changelog/0.261.md)
now describe the delivered scope and measured limits. No planned 0.261 landing
slice remains. A different minor line requires fresh explicit authorisation.
