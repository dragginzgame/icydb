### LLMs PLEASE IGNORE THIS FILE
### It's just here so I can manage multiple prompts without scrolling up and down constantly


• Here’s the performance-focused top 10 I’d put next, excluding the changes we already landed.

  1. Stream generic grouped finalize instead of finalize -> align -> collect -> page -> maybe project in crates/icydb-
     core/src/db/executor/aggregate/runtime/grouped_fold/candidate_rows/mod.rs, crates/icydb-core/src/db/executor/
     aggregate/runtime/grouped_fold/page_finalize.rs, and crates/icydb-core/src/db/executor/aggregate/contracts/grouped/
     engine.rs. This is still the biggest grouped pipeline break. Impact: high.
  2. Compile grouped projection once, the same way scalar projection already does, instead of scanning group fields and
     aggregate specs per row in crates/icydb-core/src/db/executor/aggregate/runtime/grouped_output/projection.rs,
     crates/icydb-core/src/db/executor/projection/grouped.rs, and crates/icydb-core/src/db/executor/projection/eval/
     mod.rs. This is an obvious “optimized in scalar, not in grouped” mismatch. Impact: high.
  3. Remove sibling iterator alignment after grouped engine finalize in crates/icydb-core/src/db/executor/aggregate/
     runtime/grouped_fold/candidate_rows/mod.rs. Right now multi-aggregate grouped queries still finalize separate
     vectors and then zip them back together by canonical key. Impact: high.
  4. Replace the generic grouped short-circuit done_group_keys linear scan in crates/icydb-core/src/db/executor/
     aggregate/runtime/grouped_fold/ingest.rs. Each row still scans completed group keys with canonical compare; that
     should become a hashed/set membership path keyed by the same borrowed/owned grouped key contract. Impact: medium-
     high.
  5. Improve grouped borrowed lookup beyond HashMap<StableHash, Vec<GroupKey>> bucket scans in crates/icydb-core/src/db/
     executor/aggregate/contracts/grouped/engine.rs. The BTreeMap is gone, but this is still a linear scan inside same-
     hash buckets and clones GroupKey into the side index. Impact: medium.
  6. Carry cached order keys into cursor-boundary filtering after sort in crates/icydb-core/src/db/executor/order.rs and
     crates/icydb-core/src/db/executor/terminal/page.rs. We fixed sort/select recomputation, but the post-sort cursor
     filter still recomputes row-side order values per row. Impact: medium.
  7. Reduce double-scan behavior from residual-filter retry in crates/icydb-core/src/db/executor/kernel/mod.rs and
     crates/icydb-core/src/db/executor/route/hints/load.rs. The current bounded probe plus full fallback can pay for two
     scans when residual selectivity is awkward; an adaptive widening strategy would likely save a lot of instructions
     on selective queries. Impact: high.
  8. Stop cloning whole dense slot templates per key in covering SQL paths in crates/icydb-core/src/db/executor/
     pipeline/operators/terminal/runtime.rs. The retained row is dense now, but the constant/template covering builders
     still clone a full slot vector for every row. Impact: medium.
  9. Reduce cloning in ranking materialization in crates/icydb-core/src/db/executor/terminal/ranking/materialized/
     mod.rs. The algorithm is fixed, but it still decodes from (data_key.clone(), raw_row.clone()) and carries cloned
     DataRow payloads through ranking when some projections only need ids or values. Impact: medium.
  10. Make structural slot-row projection more borrow-friendly in crates/icydb-core/src/db/executor/projection/
     materialize/structural.rs. Direct field projection is good now, but generic slot-row projection still clones values
     per field/expression; a compiled borrowed reader path would shave constant factors across SQL projection. Impact:
     medium.

  If you want the shortest path to a real win, I’d do them in this order: 1, 2, 7, 4, 3.