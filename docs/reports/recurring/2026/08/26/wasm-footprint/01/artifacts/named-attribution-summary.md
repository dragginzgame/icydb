# Symbol-Bearing Query Attribution

This supporting capture used `scripts/ci/wasm-query-attribution.sh` against
the same source revision as the authoritative footprint matrix. The
`wasm-attribution` profile preserves names and disables Candid export so Twiggy
can identify Rust owners. These byte counts are diagnostic only: they are not
deployable-size authority and cannot be compared directly with the final
Binaryen `-Oz` artifacts.

| Actor | Compiler artifact | Named shrunk artifact |
| --- | ---: | ---: |
| `one_entity_typed_query` | 15,830,845 | 3,193,284 |
| `one_entity_dynamic_query` | 21,987,369 | 4,471,809 |
| `one_entity_sql_query` | 24,211,529 | 4,930,610 |
| `ten_entity_typed_query` | 21,042,790 | 4,276,746 |

## Retained Ownership Signals

| Signal | Typed actor | SQL actor | Interpretation |
| --- | ---: | ---: | --- |
| Generated `startup_driver_attempt` retained bytes | 1,066,322 | 1,009,362 | shared startup/schema application dominates both actors |
| `lower_application_candidates` retained bytes | 271,360 | 247,799 | catalog-native application is a large child of startup retention |
| `lower_existing_store_candidate` shallow / retained bytes | 55,212 / 124,104 | 55,225 / 124,118 | one identical shared floor hotspot, not SQL-specific duplication |
| Public query export retained bytes | 126,581 | 1,194,182 | SQL ingress retains a much broader executor/frontend closure; subtraction remains directional |

The final deployable SQL artifact independently attributes 1,125,250 retained
bytes, or 34.23%, to `query_one_entity_sql`. That final-artifact result is the
authoritative footprint signal; the named result only identifies candidate
owners within it.

## Generic Bloat Signals

Twiggy's monomorphization estimate reports these four leading sort families.
Values are approximate bloat in the named diagnostic artifact, not predicted
final savings.

| Actor | Unstable quicksort | Stable quicksort | Stable drift | Unstable ipnsort | Four-family total |
| --- | ---: | ---: | ---: | ---: | ---: |
| one-entity typed | 62,773 | 19,601 | 15,134 | 16,350 | 113,858 |
| one-entity SQL | 80,533 | 37,648 | 33,140 | 20,626 | 171,947 |
| ten-entity typed | 70,275 | 31,836 | 28,355 | 18,187 | 148,653 |

These sort bodies serve different types and ordering contracts. Erasure,
indirect comparison or allocation could reduce code while increasing
instruction cost and complexity. No rewrite is authorized from this signal
alone; any candidate requires a paired final-raw-Wasm and query-instruction
experiment.
