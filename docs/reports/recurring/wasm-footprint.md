# Historical Wasm footprint conclusion

Recorded 2026-08-26, code snapshot `b6b5c287d`, method `WASM-3.0`,
profile `wasm-release`, SQL enabled. These are historical observations, not
qualification of current IcyDB or a new performance claim.

| Canister | Total uncompressed module bytes | Code-section bytes |
| --- | ---: | --- |
| `one_entity_typed_query` | 2,010,198 | unmeasured |
| `one_entity_dynamic_query` | 2,864,615 | unmeasured |
| `one_entity_sql_query` | 3,179,832 | unmeasured |

The retained receipts did not record code-section bytes. The cleanup retires
compressed-size columns and bulk Twiggy output; no missing measurement is inferred.

The typed actor used a planner-free get while the dynamic actor retained the
structural planner/executor. Neither directional gap from SQL was an exact
matched removable-overhead measurement. The historical owner audit concluded
that no broad removable owner conservatively justified its 32 KiB build threshold.
Repeated SQL parsing remained a separate instruction/attribution investigation;
no second AST cache or runtime mode was justified by this size comparison.

Historical capture command: `bash scripts/ci/wasm-audit-report.sh --skip-build`.
Current captures write to `target/evidence/`; only a selected compact conclusion
belongs in Git. Old raw report tables and binaries are not retained in the
working tree. Git history retains the original report and recorded receipts;
that is not proof that old binaries or an installed environment remain available.

Recorded Rust toolchain: `rustc 1.97.1 (8bab26f4f 2026-07-14)
binary: rustc
commit-hash: 8bab26f4f68e0e26f0bb7960be334d5b520ea452
commit-date: 2026-07-14
host: x86_64-unknown-linux-gnu
release: 1.97.1
LLVM version: 22.1.6`.

[Original report](https://github.com/dragginzgame/icydb/blob/5bf0845a043448b0ed07d39f294da0663a16fefe/docs/reports/recurring/2026/08/26/wasm-footprint/02/report.md).
