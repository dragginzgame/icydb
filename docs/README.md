# IcyDB documentation

Start with the maintained guides and contracts below. Design notes, audits and
release history describe their original source and evidence; they do not replace
current product contracts.

## Product documentation

| Topic | Entry point |
| --- | --- |
| Architecture | [Foundations](architecture/FOUNDATIONS.md) and [architecture conventions](architecture/) |
| Supported features | [Feature contract](roadmap/1.0-FEATURES.md) and [readiness work](roadmap/1.0-TODO.md) |
| Authoring and API use | [Schema authoring](guides/schema-authoring.md), [public facade](guides/public-facade-api.md) and [read intent](guides/read-intent.md) |
| Product contracts | [Query contract](contracts/QUERY_CONTRACT.md), [SQL subset](contracts/SQL_SUBSET.md), [durability](contracts/DURABILITY.md) and [persisted formats](contracts/PERSISTED_FORMAT_INVENTORY.md) |
| Operations | [Durability guide](operations/DURABILITY_GUIDE.md), [schema migrations](guides/schema-migrations.md) and [startup readiness](guides/startup-readiness.md) |
| Local engineering rules | [Agent operating manual](governance/agent-operating-manual.md), [documentation maintenance](governance/documentation.md) and [IcyDB tooling adoption](governance/toko-tooling.md) |
| Design and evidence | [Design notes](design/), [audit overlay](audits/README.md), [review status](https://github.com/dragginzgame/icydb/blob/69d885525d136060e2dbf7d3331910a24ff893a7/docs/code-review/status.md) and [report retention](reports/README.md) |
| Product release history | [Database ledger](../releases/database/CHANGELOG.md), [CLI ledger](../releases/cli/CHANGELOG.md) and [earlier coordinated notes](changelog/) |

## Shared tooling references

Shared reference guides stay upstream at the reviewed revision linked below.
The root baseline and required executable/runtime files are installed and verified
by the [snapshot receipt](../.toko-tooling.snapshot). Keep IcyDB-specific guidance
in the product topic locations above.

| Topic | Entry point |
| --- | --- |
| Shared engineering baseline | [TOKO-TOOLING.md](../TOKO-TOOLING.md) |
| Tooling installation and migration | [Adoption guide](https://github.com/dragginzgame/toko-tooling/blob/5806e6e4943f6be600cd53385498ce08a5f804c4/docs/adoption/consuming-snapshots.md), [migration history](https://github.com/dragginzgame/toko-tooling/blob/5806e6e4943f6be600cd53385498ce08a5f804c4/docs/adoption/migrations.md) and [Make interface](https://github.com/dragginzgame/toko-tooling/blob/5806e6e4943f6be600cd53385498ce08a5f804c4/docs/adoption/make-interface.md) |
| Host tools and compiler | [Host qualification](https://github.com/dragginzgame/toko-tooling/blob/5806e6e4943f6be600cd53385498ce08a5f804c4/docs/tools/supported-hosts.md), [Rust toolchains](https://github.com/dragginzgame/toko-tooling/blob/5806e6e4943f6be600cd53385498ce08a5f804c4/docs/tools/rust-toolchains.md) and [IC tools](https://github.com/dragginzgame/toko-tooling/blob/5806e6e4943f6be600cd53385498ce08a5f804c4/docs/tools/ic-tools.md) |
| Shared validation | [Validation runner](https://github.com/dragginzgame/toko-tooling/blob/5806e6e4943f6be600cd53385498ce08a5f804c4/docs/validation/validation-runner.md) and [Cargo graph checks](https://github.com/dragginzgame/toko-tooling/blob/5806e6e4943f6be600cd53385498ce08a5f804c4/docs/validation/cargo-graph-checks.md) |
| Shared release mechanics | [Release guide](https://github.com/dragginzgame/toko-tooling/blob/5806e6e4943f6be600cd53385498ce08a5f804c4/docs/releases/releases.md), [release groups](https://github.com/dragginzgame/toko-tooling/blob/5806e6e4943f6be600cd53385498ce08a5f804c4/docs/releases/release-group-validation.md) and [Cargo publication](https://github.com/dragginzgame/toko-tooling/blob/5806e6e4943f6be600cd53385498ce08a5f804c4/docs/releases/cargo-publication.md) |

Toko Tooling 0.39.5 uses online reference documentation instead of a managed
shared-doc directory. See the [migration guide](https://github.com/dragginzgame/toko-tooling/blob/5806e6e4943f6be600cd53385498ce08a5f804c4/docs/adoption/migrations.md#online-reference-documentation-in-0395).
