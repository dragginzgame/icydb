# Candid application boundary check
<!-- Shared companions: scripts/ci/check-candid-boundary.pl -->

Use `scripts/ci/check-candid-boundary.pl` to enforce the source-level part of the
[application encoding rule](../rules/engineering-details.md#ic-application-encoding).
This is a read-only lexical check using the shared Rust source masker. It runs
without Cargo, compilation, installation or network access.

```sh
perl scripts/ci/check-candid-boundary.pl "$PWD" ci/candid-boundary.json crates apps
```

All policy/source paths are literal paths relative to the selected repository.
Select existing first-party Rust roots, including independent test/example and
fixture workspaces. Each root must contain Rust source. The traversal excludes
`target`, `.git`, `.tools` and `node_modules`; it refuses symlinks and unreadable
inputs. Select generated source explicitly when it lives outside these roots.

The policy is required. Start with `{"schema":1,"allowances":[]}`. For a required
external DTO, record its exact declaration prefix:

```json
{
  "schema": 1,
  "allowances": [{
    "path": "crates/client/src/http/wire.rs",
    "kind": "external-protocol",
    "protocol": "IC HTTP gateway",
    "reason": "Gateway response converted into the owned application response",
    "declaration": "#[derive(CandidType, Deserialize)] pub struct HttpResponse"
  }]
}
```

Prefixes compare Rust tokens, ignoring comments and whitespace. They must name a
struct, enum, union, trait, type, function, or named trait/inherent implementation.
They contain no body/braces/semicolon and match exactly once in the selected file. Use the
shortest unambiguous prefix covering the Candid reference. For example,
`impl CandidType for HttpResponse` or `fn decode<T: CandidType>` admits that
specific external implementation/helper. An inherent implementation must include
its complete named header through any where-clause, immediately before its body
brace; for example `impl<T: CandidType> QueryArchiveFn<T>`. A partial trait-name
prefix cannot admit a trait implementation. Conditional derives must include their
attribute and the following named declaration. Every admitted trait reference
must be covered once. Missing, stale, ambiguous, overlapping and unused entries
fail. Directory globs and whole-module allowances are not supported. Adding a
second DTO therefore requires a separate reviewed declaration.

Use `codec-comparison` only for named competitor benchmark/codec fixtures, with
an explicit protocol and reason. Ordinary application tests are not exempt.
Named external Caffeine, HTTP gateway, management and ledger contracts retain
Candid; owned reports, storage types and application request/reply DTOs use Wire.
Prefer existing Platform external types and explicit conversion before adding an
allowance. Do not label a general application module an external adapter.

## Coverage and limitations

The default mode covers Candid trait references only. It does not prove that an
allowed DTO comes from Platform or establish protocol byte compatibility. The
optional CBOR source mode below complements dependency admission. Apply the
separate dependency/import obligations in
the [encoding contract](../rules/engineering-details.md#ic-application-encoding)
to Rust and browser roots; report adoption and coverage gaps explicitly.

The checker detects literal `CandidType` references outside imports, including
qualified paths, manual implementations, generic bounds and conditional derives
in every source cfg branch. It follows explicit same-file `use ... as ...` aliases
conservatively across scopes. This can over-report shadowed identifiers; remove
ambiguous aliases or review the exact external declaration rather than exempting
the file. An import alone is not a violation. Comments, strings and Wire helpers
named `encode_one`/`decode_one` are not violations.

This is **not Rust type resolution or macro expansion**. It cannot discover an
alias imported from another file/dependency under an unrelated name, prove the
origin of a wildcard import, or inspect procedural-macro expansion, generated
strings, `include!` content outside selected roots, or types acquiring the trait
through blanket implementations. Literal Candid references inside macro/quote
bodies are detected, but macro definitions cannot receive a broad allowance;
prefer Platform-owned code generation or an explicitly reviewed separate check.
Do not claim semantic completeness. Review generated implementations and
cross-module re-exports when adopting or changing macros. The checker does not
validate Rust syntax, external protocol compatibility, field bounds or the truth
of the policy's protocol/reason. Those remain code-review and focused-test duties.

## Consumer integration

Export the checker through the normal snapshot closure; its lexer comes with it.
Add one lightweight source-check target to the existing consumer validation owner.
For group-selected checks, select affected roots and their corresponding policy
file; entries outside the selected roots fail rather than silently disappearing.
Do not run the same complete source inventory once per group or replay Shared's
producer fixtures in consumers. Keep explicit source selection in reviewed Make/CI
configuration, not a moving sibling checkout. Consumer adoption is separate from
the supplying change; no automatic dependency refresh or release is required.

## Optional CBOR source enforcement

Run the same selected inventory with `--cbor` before the root argument to reject
literal Rust crate references from `ci/cbor-dependencies.json`, including imports,
re-exports, raw identifiers and paths in macro bodies. Comments and strings are
masked. Typed `ic_platform::certification::cbor` calls remain permitted. Pair this
with the Cargo `--external-ic` check: Cargo resolves renamed dependencies; the
source check cannot resolve an arbitrary alias imported from another module or
inspect generated strings, macro expansion, local copied parsers or blanket impls.
Select generated Rust files explicitly and review unsupported dynamic paths.

An optional `cbor_allowances` array in the same schema-1 policy records exact
`path`, normalized Rust `crate` name, nonempty `reason` and `review` strings.
For example, an approved Platform certificate implementation may name its exact
source file and `serde_cbor`, with review on external-protocol change. Each entry
must be in the selected inventory and have a live crate reference; duplicate,
unknown and stale entries fail. This is a file-and-crate exception, not approval
for other codecs or files. Consumers need explicit maintainer approval for a
comparison exception, recorded in `reason` with its retirement/review condition
in `review`. The tool validates fields and live use, not human authorization.
Policies containing CBOR allowances require `--cbor`; they cannot silently run
with that enforcement disabled. Cargo dependency restrictions remain independent.
