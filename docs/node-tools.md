# Node/npm prerequisite checks

Opt-in Node consumers can adopt `scripts/ci/check-node-tools.mjs` as a snapshot
entrypoint. Select the Node executable before invoking it:

```sh
/path/to/node scripts/ci/check-node-tools.mjs /path/to/package /path/to/npm
```

The package root owns `.nvmrc`, `package.json` and `package-lock.json`. The checker
requires an exact Node version and `npm@X.Y.Z` package-manager declaration, then
compares the lock root's production, development, optional and peer dependencies,
plus engine declarations. It executes only the selected npm's `--version`, with a
30-second timeout and 4 KiB output bound. It performs no installation, network
request or lockfile mutation and does not claim a resolved dependency audit.

The npm executable is optional and defaults to `npm` on the caller's PATH.
Paths are individual arguments, including paths with spaces. Package roots and
executables are trusted caller selections; this is not a sandbox.

Build scripts may import `checkNodeInputs(packageRoot)` for Node and declaration
admission without spawning npm, or `checkNodeTools(packageRoot, npmExecutable)`
for the complete check. Both return the admitted root and version selections.
Importing the module alone has no checking or execution side effect.

The standalone fixture runs with `node scripts/ci/test-node-tools.mjs`.
The portable suite includes it when Node is available and reports a skip otherwise;
Node remains an opt-in consumer prerequisite, not a portable-shell prerequisite.
Actual browser engines, SDK preparation, compiler inputs and product tests stay
with consumers. Adopt this checker from a reviewed committed snapshot before
removing a local equivalent.

IC SDK consumers also follow the [npm IC boundary](../rules/dependency-pinning.md#ic-javascript-and-typescript-dependencies):
use the independent `@tokostudios/ic-platform` package for supported SDK Core
capabilities. This prerequisite checker does not inspect SDK imports or prove
provider compatibility; those remain consumer qualification obligations.

## IC SDK boundary check

After qualified Platform npm adoption, opt in to the standalone, read-only check:

```sh
node scripts/ci/check-ic-sdk-boundary.mjs clients/browser tests/browser scripts
```

Replace these example roots with the consumer's actual first-party package,
source, test and build roots. Each argument can be a file or directory; missing
roots, symlinks and selections with no supported files refuse rather than produce
a vacuous pass. Overlapping selections are counted once. Root paths are trusted
caller inputs, not a filesystem sandbox.

The checker reads `package.json` and JavaScript/TypeScript files (`js`, `mjs`,
`cjs`, `jsx`, `ts`, `mts`, `cts`, `tsx`). It rejects SDK Core and the legacy
`@dfinity/agent`, `principal`, `identity`, `identity-secp256k1` and `candid`
packages in all four direct dependency sections, including npm aliases. It also
rejects plain literal imports, re-exports, `require`/`require.resolve`, dynamic
imports and imports embedded in npm scripts or generated-source snippets.
Literal import examples in source comments are also checked; this is a
conservative source guard, not a JavaScript parser. Escaped or computed module
names and arbitrary loaders require the consumer's build/resolution checks.

Recursive scans skip `node_modules`, `.git`, `.tools`, `target` and `dist`.
Select an actual generated-source directory or file explicitly when it lives
under a skipped build directory. Prepared dependency trees, lockfile transitive
entries and provider overrides are not inspected by this check. Auth/Caffeine
providers and executable tools such as `@icp-sdk/ic-wasm` remain permitted.
SDK-free roots pass without introducing a Platform dependency.

The check never installs packages, executes selected source, changes locks or
resolves modules. It does not prove a single installed SDK identity, compatible
provider peers, a published Platform version or product acceptance. Keep those
consumer checks alongside it; the Platform producer legitimately imports SDK
Core and is not a consumer root for this guard.

Consumers can import `checkIcSdkBoundary(roots)` from the module; it returns
`{ manifests, sources }` on success. Importing the module has no checking side
effect. Adopt `scripts/ci/check-ic-sdk-boundary.mjs` as an explicit snapshot
entrypoint using the [canonical exporter](consuming-snapshots.md#refresh).
It has no external runtime dependencies or shared companions. Shared owns its
regression matrix in `test-ic-sdk-boundary.mjs`; consumers retain their actual
root selection and invocation proof rather than replaying that producer matrix.
