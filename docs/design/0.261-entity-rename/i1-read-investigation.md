# I1 — Post-publication read instructions

2026-09-20. The user explicitly authorised investigation followed by Q1.
The elevated read cost is reproducible accepted-schema runtime preparation.
It recurs across separate query messages until a successful replicated call
retains that preparation. Restart alone does not remove it. No production
runtime change or optimization is included in this investigation.

## Controlled comparison

Extend the existing two [rename rehearsals](../../../testing/integration/tests/entity_rename.rs)
with repeated queries before any ordinary update, reads before post-restart
updates, and the existing catalog instruction probe on either side of the first
ordinary update. Both scenarios pass with identical rows, generated bindings,
constraints, receipts and zero reported row rewrites/index rebuilds.

The first ordinary update is `check_entity_rename_deletes`: it catches the two
expected restrictive-delete errors and returns normally. It changes neither
entity's rows; the complete CatalogItem description also compares equal before
and after. Catalog and SQL probes are separate query messages, so the catalog
probe cannot prime the following SQL probe across calls.

| Instructions, same actor and rows within each scenario | Direct advance | Restart before advance |
| --- | ---: | ---: |
| Source SQL after seed update | 1,818,723 | 1,818,723 |
| Successor SQL after publication/retry | 34,651,563 | 34,591,800 |
| Identical successor query in another message | 34,651,563 | 34,591,800 |
| Catalog before rejected deletes | 33,959,635 | 34,022,525 |
| Catalog after rejected deletes | 1,557,834 | 1,477,279 |
| SQL after rejected deletes | 2,204,369 | 2,044,411 |
| SQL after all existing constraint updates | 1,992,261 | 2,072,427 |
| SQL after successor restart/retry, before ordinary updates | 35,633,016 | 35,607,886 |
| Catalog after restart, before rejected deletes | 34,969,380 | 34,970,127 |
| Catalog after restart and rejected deletes | 1,242,514 | 1,245,341 |
| SQL after restart and rejected deletes | 1,971,455 | 1,971,743 |
| SQL after restart and all constraint updates | 1,981,331 | 2,061,331 |

The first ordinary update reduces the subsequent SQL interval by 32,447,194 /
32,547,389 instructions and the catalog interval by 32,401,801 / 32,545,246.
These are lifecycle-state comparisons, not an implementation improvement.
The original E3 post-restart measurement followed constraint updates; it did
not establish that restarting alone reduced read cost.

## Attribution and limits

The [runtime-root owner](../../../crates/icydb-core/src/db/session/accepted_schema.rs)
matches the cached store roots against current accepted roots. A mismatch
compiles every accepted entity into one database-wide runtime root, including
inspection and row-decode contracts. SQL and catalog reads both use that owner.
The [store owner](../../../crates/icydb-core/src/db/schema/store.rs) also invalidates
its verified bundle cache when the accepted root changes. These are heap caches.
The [metadata migration owner](../../../crates/icydb-core/src/db/schema/application.rs)
publishes the successor and returns status; terminal command replay does not
perform an ordinary session read that would retain the current runtime root.

Separate queries rebuild from the same replicated heap and discard their
preparation. The ordinary update traverses the shared accepted runtime owner
and returns successfully, retaining its heap preparation even though its
attempted row deletes correctly return constraint errors. The controlled
catalog result localizes the dominant extra work to accepted runtime/catalog
preparation rather than row scanning or rename-specific query execution.
Individual compile/decode functions were not instrumented; their exact shares
remain unmeasured. Startup, query planning and allocator state can also affect
the remaining interval. No claim applies to every application schema size.

This is a real query-only cost limitation, not a harmless first-query warmup.
The SQL fixture contains many accepted entities beyond Item/Holder, so the
absolute cost is not a minimal two-entity application estimate. A follow-up
could evaluate preparing the existing runtime root in a replicated lifecycle
owner, measuring the cost moved into that call as well as query savings. That
would be separate production work; no new cache, mode, timer or persisted state
is proposed here, and applications are not advised to issue artificial writes.

## Reproducibility and measurements

Base HEAD, toolchain, lockfile, declarations, fixed controls, build features,
input/output hashes and instruction/cycle scopes match [E3](e3-evidence.md).
Only the host rehearsal changes. This run uses the repository-local Cargo home
`.cache/cargo/icydb` and target `target/icydb`; E3 artifacts embed
`/home/adam/.cargo/registry/src/` paths instead.

| Final non-gzipped actor | Bytes | BLAKE3 |
| --- | ---: | --- |
| Source | 8,922,245 | `e1c38cc43348d7fe37274f18dcae22507db7042d6a3f15c65d5c771b34b6850c` |
| Successor | 8,953,795 | `d0ea594347d585b1291171a35b0926d2fb71b88426627b46eea9e7195a455eba` |

Successor minus source is +31,550 raw bytes. Compared with E3, artifacts are
+3,060 / +3,057 bytes; 113 longer embedded Cargo-home paths account for 3,051
bytes each. This build-environment difference is not an I1 runtime size change.
The residual byte difference is not attributed to a particular section here.
Both I1 runs use exactly the same source/successor actor hashes.
The final host rehearsal SHA-256 is
`9bbae409a387472bf11b25db41004ffe02071e04ed2a98f96bd162157070ca6f`.

Initial Advance cycles are unchanged from E3: 405,294,975 / 405,445,154.
Immediate retries remain 132,531,995 / 132,745,522; retries after restart remain
152,194,485 / 152,104,427. Thus measured command-envelope cycle deltas against
E3 are zero. Migration-body instructions and production release size remain
unmeasured. No numerical cost acceptance gate was part of this qualification.

Two disposable PocketIC servers were started and stopped for the repeated-query
run and the refined catalog control. An initial wrapper invocation failed before
startup because the script is not executable; invoking it through Bash resolved
that launch failure. No application network was changed.
