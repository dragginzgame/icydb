# Compact diagnostics

IcyDB errors carry a stable numeric E-code and a bounded list of numeric facts.
They deliberately omit schema names, SQL text, keys, rows, and values so normal
canisters do not retain a large diagnostic prose catalog. One narrow exception
lets a failed query return the caller's own rejected field reference as bounded,
typed `query_field` context; the E-code and numeric facts remain authoritative.

The CLI can always explain a code:

```console
icydb diagnostic E210
```

Pass the facts printed by the caller as repeated `--fact TAG=VALUE` arguments.
Tags may use their numeric identity or maintained CLI label:

```console
icydb diagnostic E210 \
  --fact accepted_schema_fingerprint_method=1 \
  --fact accepted_schema_fingerprint_high=123 \
  --fact accepted_schema_fingerprint_low=456 \
  --fact entity_tag=17 \
  --fact constraint_id=4 \
  --fact constraint_kind=5 \
  --fact mutation_operation=1 \
  --fact batch_position=0
```

Without exact accepted-schema metadata, this remains a complete numeric report.
Categorical values such as constraint kind and mutation operation still receive
host-owned labels.

## Live schema resolution

When the deployed canister explicitly exports `icydb_schema`, add its canister
name. The CLI uses the selected environment and the deployed endpoint only:

```console
icydb diagnostic E210 --canister app --environment production \
  --fact accepted_schema_fingerprint_method=1 \
  --fact accepted_schema_fingerprint_high=123 \
  --fact accepted_schema_fingerprint_low=456 \
  --fact entity_tag=17 \
  --fact constraint_id=4
```

Method-not-found, authorization, and replica failures are authoritative. They
do not cause the CLI to infer names from local source. In particular, E25 means
the deployed schema method retained controller authority, while E261 means its
application guard denied this request. Method-not-found means the standard
schema method is absent. A later request may succeed after application policy
changes; the CLI emits no endpoint manifest and does not guess the configured
authorization mode.

## Offline diagnostic artifact

Export a bounded host-only artifact while the explicit schema endpoint is
available:

```console
icydb schema diagnostic-artifact app --environment production \
  --output app.diagnostic.json
```

The output path must be new. Use the artifact later without deploying a schema
endpoint:

```console
icydb diagnostic E210 --artifact app.diagnostic.json \
  --fact accepted_schema_fingerprint_method=1 \
  --fact accepted_schema_fingerprint_high=123 \
  --fact accepted_schema_fingerprint_low=456 \
  --fact entity_tag=17 \
  --fact constraint_id=4
```

Names are used only when the fingerprint method, complete 128-bit accepted
fingerprint, and entity tag match exactly. If they do not, the CLI withholds
artifact names and prints numeric identities. When `--artifact` and
`--canister` are combined, artifact provenance must also match the selected
deployment; an exact live introspection result is the next resolver.

## Generated/source metadata

Bind an exact accepted artifact to the generated package or source tree that
will carry its diagnostic labels:

```console
icydb schema diagnostic-source-metadata \
  --artifact app.diagnostic.json \
  --source schema/app \
  --output app.source-diagnostic.json
```

The command preserves every accepted fingerprint, entity tag, numeric ID, and
label from the validated deployment artifact. It changes only the bounded
host-side provenance. Source declarations alone cannot create this file
because they do not own accepted database identities.

Use the bound metadata when neither an exact deployment artifact nor live
schema introspection is available:

```console
icydb diagnostic E210 --source-metadata app.source-diagnostic.json \
  --fact accepted_schema_fingerprint_method=1 \
  --fact accepted_schema_fingerprint_high=123 \
  --fact accepted_schema_fingerprint_low=456 \
  --fact entity_tag=17 \
  --fact constraint_id=4
```

Resolution order is exact deployment artifact, explicit live introspection,
exact source metadata, then numeric fallback. Deployment and source provenance
are not interchangeable, and any identity mismatch withholds names.

The JSON artifact is a tooling format, not database state. It cannot authorize
a write, apply a schema, recover data, or replace accepted schema authority.
Only the current pre-1.0 artifact shape is accepted.

## Rejected query fields

An E3 planning failure may include this optional public record:

```text
query_field : opt record {
  field : text;
  role : nat8;
}
```

The role identifies predicate, projection, group-by, HAVING, order-by, or
aggregate-target use. The field is the exact nonempty reference presented to
the accepted-schema resolver after parsing and existing qualifier or alias
normalization. It is present only when its UTF-8 representation is at most 256
bytes; longer references are omitted rather than truncated.

Structured Rust consumers should call `Error::validated_query_field()` before
using decoded context. The shared CLI does the same and renders, for example:

```text
E_QUERY_PLAN: query planning failed; order_by field `id`; facts term_index=1
```

Terminal controls, newlines, quotes, backslashes, and backticks are escaped in
host output while the structured field remains exact. An unknown role,
disallowed E-code/role pair, invalid bound, or mismatched numeric fact schema
keeps the base diagnostic available but withholds the untrusted field and
reports a context mismatch. No schema lookup or spelling suggestion is used.

SQL endpoints and dynamic, fluent, or generated query APIs share this context
only when they reach the same canonical planner failure. SQL parse failures and
frontend-only lowering failures that have no singular resolver field do not
invent one.

## Unsupported Nested Paths

Accepted schema classifies named-record scalar paths separately from paths
that cross lists, sets, maps, tuples, or newtype wrappers. Unsupported paths
reject before query execution or index-catalog mutation and never receive a
scan or multikey fallback. Exact capability cells are defined in the
[nested storage contract](../contracts/NESTED_STORAGE.md).

Use each surface's typed diagnostic code and structured context; do not match
messages or assume that every frontend exposes the same internal variant.
Structural mutation accepts root fields only; an unknown root or dotted
subpath target returns the existing executor-origin `RuntimeUnsupported`
diagnostic before commit preparation.
