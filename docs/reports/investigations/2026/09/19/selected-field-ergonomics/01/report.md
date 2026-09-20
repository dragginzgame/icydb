# Selected-field catalogue reads — R2 investigation

## Decision

The maintained structural API already expresses the catalogue label query.
Do not implement a new executor, cache, generated partial entity or public
projection abstraction in this investigation. The demonstrated gap is the
ergonomic distance between complete generated rows and explicitly decoded
selected values, not missing database selection capability.

Before proposing a new typed facade, measure the existing selected-field path
against a full-row query followed by the same label projection, with identical
final output. That measurement is a proposed follow-up, not a delivered gain.
No application changes are authorised or made. R2 is complete as an
investigation; further implementation requires a scope decision.

## Subject and application evidence

IcyDB HEAD `4698cecb8dbba6786079b759b43494b020559997` plus the current dirty
C1/R1 worktree. The prior [R1 report](../../typed-catalogue-reads/01/report.md)
records that candidate's source inputs. Lock SHA-256 remains
`8024973c209ab9244ac24687f3a8d42e772f0c3c4454d7a5f57aeaaa0c34c658`.
Toko Miner was inspected read-only at clean HEAD
`bb04f9b53f4149b95980bfe3494c5908ddbc8e80`; no build or publication is inferred.

Application source, under `apps/toko_miner/game_shard/src/`:

- `nft_collection/mod.rs:280`: `list` fetches Items once and passes them through
  preview/recipe projection. The earlier repeated-read amplification has
  already been removed by the application.
- `crafting_recipe.rs:176`: `material_label` uses only Item `id`, `key`, `name`.
- `rocket/mod.rs:353`: `read_items` requests complete typed Items ordered by
  key, and rejects continuation or excessive rows. It is not a collect-all
  pagination helper. Other inventory/placeable callers need fuller Items;
  narrowing this shared reader globally would not be a safe drop-in change.
- `design/entity/item.rs:61`: complete Items also contain description, optional
  nested placement/production/voxel data, capacity, recipe and timestamps.

Source hashes: `rocket/mod.rs` is
`c37c084f3858eb0bed07eec367206eae8879e60f82b55a2877d904fd14afe3c0`;
`crafting_recipe.rs` is
`791e661ca8e883683f094632e4d020b43e5c289dbeb585600e73cf6fce85fc34`.
These are source observations, not fresh deployment qualification.

## Existing application-facing alternative

The following query construction uses maintained surfaces and generated field
constants, not SQL or raw field-name strings. It is an illustrative application
example, not a downstream edit or a freshly compiled example:

```rust,ignore
let request = DynamicQuery::new(Item::ENTITY)
    .select([Item::ID.as_str(), Item::KEY.as_str(), Item::NAME.as_str()])
    .order_by(asc(Item::KEY))
    .limit(257);
let page = database.execute_live_page(&request, continuation.as_deref())?;
```

The caller checks columns and exact row arity, consumes `OutputValue` through
`into_public`, and matches `Ulid`, `Text`, `Text` into its label DTO. Unexpected
shapes return a typed application error; do not use positional indexing that
can panic, rendered strings, default values or a full Item decoder. Preserve
the continuation until conversion succeeds. Keep an explicit aggregate row/
page bound if traversing internally; selection does not waive public admission.
The existing `advance_live_page`/`LivePageStep::commit` contract can own that
decode-before-adopt ordering when an adapter traverses pages.

`docs/guides/public-facade-api.md:141` already documents this selection route.
`Query<E>` deliberately has no scalar `select` method and decodes all fields
of `E::Row` (`crates/icydb/src/db/query/typed.rs:114,237`; generated field loop
in `crates/icydb-model-macros/src/node/entity.rs:1221`). Mapping a complete typed
row into a smaller DTO afterwards cannot undo its earlier materialisation.
Do not fabricate missing fields or make every field optional to disguise it.

Low-level binding/output preparation can map partial columns, but those
doc-hidden adapter seams are not a ready-made public typed projection API.
Dynamic requests resolve current accepted names; generated constants supply
source spelling, not immutable accepted identity. A future binding-aware facade
must preserve that distinction and retain current-authority validation.

## Existing execution owner and semantic limits

`DynamicQuery::select` records explicit output order. The ordinary structural
and typed page methods converge on the existing scalar page executor in
`crates/icydb-core/src/db/session/query/dynamic.rs`. Projection/filter/order
requirements determine retained slots, rather than an application decoder
inventing a second storage interpretation.

`executor/terminal/row_decode/mod.rs::decode_indexed_slot_values` retains dense
and sparse decoding in the existing owner. `StructuralSlotReader` permits
lazy field access. Direct projection materialises selected fields and checks
primary-key consistency; a malformed unrelated payload may remain unread.
Selecting that payload must reject it. An eager full-row reader validates all
declared slots. This is an existing intentional distinction, not permission
to weaken current corruption checks. A label read is not an integrity audit.

The row runtime still fetches the raw stored row before selecting values
(`executor/terminal/page/row_runtime.rs::read_full_row_retained`). Selection
can avoid unrelated decoding, typed conversion and output allocation; it does
not establish fewer stable bytes fetched or an index-only query. Filters and
ordering can require fields not present in the outward selection.

## Choices and follow-up gate

| Choice | Benefit | Cost / disposition |
| --- | --- | --- |
| Keep complete typed rows | No caller changes; receives R1's binding improvement | Still materialises fields unused by labels; maintained default |
| Existing structural selection | Requests exactly the label fields using the same executor | Caller owns checked value conversion; first comparison to measure |
| New typed DTO projection surface | Could remove repetitive selected-value conversion | Requires accepted field/type/nullability/identity, pagination and error contracts; not approved or implemented |

If a matched measurement shows worthwhile savings and real callers find the
conversion burdensome, extend the existing facade/output-conversion owner.
Keep the existing structural request and page executor as the convergence
point. No tuple-arity framework, derive family, planner mode, cache, cursor
format or generated-model runtime fallback is justified by this investigation.
There is no automatic projection available from an opaque later Rust `.map`.

## Validation and exclusions

Four focused existing core tests passed, zero failed/ignored: lazy unrelated
payload avoidance, selected-field/primary-key corruption boundaries, eager
rejection and uncached invalid materialisation, and stored nested/repeated
projection parity. These establish maintained boundaries, not a measured
catalogue plan or application runtime. Locked/offline SQL/migration core test
configuration reused existing local build outputs. No full suites, PocketIC,
network lifecycle actions, sibling edits, version changes or publication.

This slice changes only this report and the 0.260 tracker (two documentation
files, approximately 140 added lines). Runtime shape/state space is unchanged.
No new Wasm, cycles or instruction comparison was run; selected-vs-complete
cost is **unmeasured**. Do not transfer R1's 13.1% cycle saving to selection.
Canic W1/W2 remain separately outstanding; no new minor release was started.
