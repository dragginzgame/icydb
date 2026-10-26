# Cross-runtime RPC budgets

This rule is part of the [engineering baseline](../DRAGGINZGAME.md#ic-application-encoding).
It applies when an owned RPC contract generates clients for another runtime,
including Rust canisters with browser clients. A server-admitted response can
still fail at a client configured with independently maintained limits.

## Ownership and admission

- Give product RPC resource policy one explicit owner. Generate client byte,
  node and depth settings from that owner through the existing generator and
  supplier APIs. Do not maintain a second set of client literals or patch output.
- Keep request and response policies separate. Map client request encoding to
  server request decoding, and server response encoding to client response
  decoding, including the actual endpoint envelopes. Transport ceilings remain
  independently owned; document whether each byte limit counts payload, frame or
  transport bytes rather than equating those sizes.
- An intentionally different client limit belongs in the same product policy,
  with its direction, resource, value, product reason and behavioral tests.
  Document what the client rejects and how callers handle it. Neither an inline
  comment beside a stray literal nor an untested override establishes an exception.
- Matching numbers prove configuration agreement only. Codec traversal can count
  wrappers, recursion, collections and bulk bytes differently across runtimes.
  Qualify the actual supported boundary; document differences and the product's
  disposition rather than claiming identical admission from numeric equality.
- Native allocation allowances are not JavaScript heap limits. Keep each
  runtime's supported controls explicit; do not translate an allocation budget
  into an unsupported browser option or claim a measured heap bound from it.
- Do not raise or disable limits merely to make qualification pass. Changes to
  admitted inputs or replies need product review and compatibility assessment
  under the [package-impact rules](changelogs.md#classify-the-released-packages-own-impact).

Shared Tooling owns this rule and recipe, not numeric defaults, codec traversal
or an RPC framework. IC Wire owns its codec, generator and admission semantics.
Consumers own policy, generator integration, endpoint/transport limits and tests.
Missing supplier capabilities remain owning issues; policy publication is not
proof that those capabilities or consumer adoption have shipped.

## Consumer qualification recipe

Use the normal local/CI generation entrypoint and existing test harness. Keep
Linux and native macOS workflows supported; no extra generation path, source-text
constant scanner or shared runtime is required.

1. Inventory the policy owner, supported endpoint shapes, supplier version and
   request/reply controls in both runtimes. Identify transport limits separately.
   Record any missing codec support as a qualification gap, not a passing case.
2. Feed the owner's structured policy into the generator. Compare the effective
   settings used by the generated client with the expected projection from that
   owner, including declared asymmetric settings. Inspect the settings actually
   supplied to the codec through its supported configuration interface; an unused
   exported policy object is insufficient. Use structured output or observable
   calls, not regexes over generated constant names.
3. Deliberately change a generated request/reply byte, node or depth setting in
   isolated test output while leaving the owner unchanged. The ordinary check
   must reject each mutation. Regenerate from unchanged policy and require a
   pass. Keep source, retained artifacts and normal generated outputs untouched.
4. Exercise an explicit asymmetric policy through that same generator. The
   configuration check must accept its declared projection, while an undeclared
   difference still fails. Run boundary cases demonstrating the intended narrower
   client contract and caller handling; a nonempty reason alone is not a pass.
5. Run the following cases with the real native codec and generated client at
   the selected supplier version. For each resource, include below, exactly at
   and above the admitted boundary while keeping other budgets nonbinding.
   Count actual codec units rather than assuming collection length equals nodes
   or domain recursion depth equals codec depth. Small test budgets can make
   cases tractable, but also cover the product's largest supported shapes under
   its actual configured limits.

| Boundary | Required observation |
| --- | --- |
| Request bytes and reply bytes separately | Actual envelope/frame sizes and expected encode/decode admission in both directions; preserve distinct transport checks. |
| Nodes | Scalars, options/enums, records and collections, including endpoint wrappers that consume traversal budget. |
| Depth | Nested and supported recursive shapes with request/reply wrappers; unsupported recursion remains an explicit gap. |
| Bulk data | Bytes/blobs and ordinary element collections, testing their potentially different traversal accounting. |
| Intentional asymmetry | Values accepted by the producer but rejected at the declared client boundary, with expected typed failure and caller handling. |

Retain the selected source/policy, supplier version, generated artifact identity,
commands, runtime/host and expected/observed outcomes using existing evidence
locations. Distinguish configuration checks, actual codec checks and installed
endpoint/transport qualification. A substitute decoder or a Node-only check
does not qualify browser-specific behavior. Bind relevant browser execution to
the product's existing browser harness. Do not infer Wasm size, IC instruction
cost or heap safety from a policy comparison.

## Example adoption

IcyDB's reported integration illustrates the boundary: `icydb-schema::wire`
owns native policy, while `testing/build/examples/wire_bindings/main.rs` supplies
browser generator settings. The reported browser node/depth literals were
independent of the schema policy even though byte settings were reused. See
[#307 — Browser RPC budget policy and qualification](https://github.com/dragginzgame/shared-tooling/issues/307)
for the original evidence; these paths describe an example, not required layout
or a statement of current consumer qualification.

The smallest adoption is to project the existing policy's request/reply bytes,
nodes and depth into the existing generator configuration, leaving the native
allocation allowance and transport ceilings at their owners. If smaller browser
budgets are intentional, declare that projection at the product policy owner
and qualify the restricted contract instead of silently copying larger values.
Keep the normal generation/check target authoritative and test its generated
configuration as well as the actual client codec. Wire's supplier work remains
separate from this consumer adoption.

For the configuration-only part, an existing JavaScript test can use structural
equality on the effective settings. This runnable assertion example uses tiny
illustrative budgets, not fleet defaults or a generator/codec implementation:

```javascript
import assert from 'node:assert/strict';

const owner = {
  request: { bytes: 256, nodes: 64, depth: 8 },
  response: { bytes: 512, nodes: 128, depth: 12 },
};
const expected = structuredClone(owner);
const generated = structuredClone(expected); // Substitute only for this example.
assert.deepStrictEqual(generated, expected);
for (const direction of ['request', 'response']) {
  for (const resource of ['bytes', 'nodes', 'depth']) {
    const changed = structuredClone(generated);
    changed[direction][resource] -= 1;
    assert.throws(() => assert.deepStrictEqual(changed, expected));
  }
}
const asymmetricOwner = {
  ...owner,
  browserResponse: { ...owner.response, nodes: 96 },
  reason: 'Browser view intentionally accepts fewer collection elements',
};
const asymmetricExpected = {
  request: asymmetricOwner.request,
  response: asymmetricOwner.browserResponse,
};
const asymmetricGenerated = structuredClone(asymmetricExpected);
assert.deepStrictEqual(asymmetricGenerated, asymmetricExpected);
assert.throws(() => assert.deepStrictEqual(asymmetricGenerated, expected));
```

Replace the substituted objects with observations of the actual generated client
in a consumer test. This example proves only that the comparison detects drift
and distinguishes a declared projection; it does not qualify generation,
asymmetric runtime behavior or codec equivalence. Steps 2–5 remain required.
