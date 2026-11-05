> Historical assessment, not current-code qualification. Raw output and superseded runs
> were retired from the working tree under Shared 0.30.4. Git history retains them.
> Current follow-up authority is GitHub issues; selected results below retain their
> original source, method and limitations, not a fresh measurement.

# ICYDB-038 — Actual application catalogue reads

2026-09-21. User-authorised I2 investigation on the existing 0.261 line.

## Scope and disposition

The concrete integration candidates belong to Toko Miner's catalogue consumers:
Resource inventory projection, OrderCustomer presentation and NftToken order
eligibility. Existing generated field references and dynamic selection can
express all three required projections. No new executor, persisted cache,
schema format, runtime mode or query-attribution API is proposed.

The managed comparison passes: selected fields reduce Resource read/projection
instructions by **46.4–47.1%**, customer instructions by **51.4–52.0%**, and NFT
token instructions by **20.4–20.9%** across four lifecycle states. All returned
values and order match; same-release restoration preserves the complete result.
This completes the bounded I2 investigation. ICYDB-038 remains a candidate until
matched evidence identifies
the dominant actual action cost and qualifies a simpler change or establishes
that no upstream change is warranted. A read-only diagnostic endpoint does not
by itself establish savings in `execute_actions` or close that acceptance.

## Source findings

The frozen source, including dirty downstream work, is identified in
[sources.json](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/21/toko-miner-catalogue-reads/01/sources.json). The working snapshot is
`/tmp/icydb-038-investigation`; the sibling application is read-only.

| Consumer | Current work | Bounded alternative and constraint |
| --- | --- | --- |
| `rocket::read_resources` and inventory projection | Complete Resource rows in catalogue order, limit 257; reject an unfinished page. Inventory identity, validation and summaries use ID, key, name and maximum stack. | Compare those four selected fields with full rows reduced to the same values. Other Resource consumers need additional fields, so changing the shared full-row function globally is not justified. Reuse caller-owned rows first where the actual request already has them. |
| `outpost::orders::catalogue::summary` customer | Read every customer in catalogue order, limit 64, validate every row's two colour fields, then find one customer by ID. | Selecting ID, username, both colours and avatar preserves the all-customer validation scope. A primary-key-filtered query is a separate comparison: it no longer checks unrelated customer colours or the whole-catalogue overflow boundary. Equal successful results do not prove equivalent failure behavior. |
| Same Orders summary, eligible patterns | Complete NftToken rows for one collection in token-number order, limit 100; reject continuation; filter rarity and return IDs. | Select ID and rarity with the same filter, order, limit and continuation rejection. Decode rarity with the existing generated `TypedOutputValue` implementation and current NftToken binding. Retain the same ordered eligibility result. |

Resource has an accepted catalogue-order index; OrderCustomer has its primary
key and unique catalogue-order index; NftToken has the accepted unique
collection/token-number index. Inspection does not establish a missing index.
The probe preserves existing admission and uses generated field/entity names.

The outpost catalogue and map owners already retain rebuildable projections.
Several initialisation/reconciliation paths also call `read_resources`; merely
counting its source callers would overstate work in an ordinary warm action.
No additional persistent application cache is justified by this inspection.

[Original report and recorded commands](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/investigations/2026/09/21/toko-miner-catalogue-reads/01/report.md).
