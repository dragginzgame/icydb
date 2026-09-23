# Documentation Maintenance

Current contracts define supported behavior. Guides explain its use. Design
reports and published release notes retain evidence for their original source
and artifacts; they do not create another current feature backlog.

## Owners And Checks

- README owns release-pinned dependency examples. Installation and other guides
  link there; release tooling updates that one surface.
- The compiled `testing/model-facade-only` package owns the complete onboarding
  example. README links to its manifest, declarations, build script and actor.
  Its native test exercises startup, typed writes and reads; a separate ignored
  PocketIC qualification covers upgrade.
- Codec modules own encodings, bounds, checksums and rejection semantics. The
  persisted-format inventory names those owners. Existing codec tests execute
  current round trips and malformed/corrupt rejection; documentation checks
  must not substitute a required sentence for that evidence.
- Schema authoring contains two marked data lists: primary-key primitives and
  reserved identifiers. Owner-local Rust tests compare those sets with compiled
  capabilities and the actual reserved-word collection. Surrounding prose,
  ordering and line wrapping are free to change.
- The resource model owns the documented numeric read/input ceilings. Its marked
  table is compared with compiled policy accessors and input constants, not
  source spellings or required sentences. Runtime owners remain authoritative.
- `docs/1.0-TODO.md` owns remaining readiness work. Each item distinguishes
  implementation, qualification, or a product decision. Reconcile affected
  entries when implementation changes rather than appending another backlog.

`make test-documentation` runs the focused native example, capability-list and
codec checks. These tests also participate in their normal package test lanes.
`perl scripts/ci/check-documentation.pl` is the inexpensive structural gate in
`check-invariants`: it checks local Markdown file/directory targets in current
entry points, guides and contracts, inventory source-owner paths, and README
dependency tags against the workspace version. It ignores fenced examples;
it does not validate anchors, remote links, every Markdown extension, or the
truth of arbitrary prose. Explicit document arguments support isolated checks.
Its focused fixture tests cover relative/absolute links, encoded spaces,
reference links, fenced examples and missing-target rejection.

The separate persisted-format version scan remains a static code policy guard,
not codec qualification. Other architecture source scans keep their existing
scope. Do not add paragraph/heading/link-label assertions to either gate.

## Updating A Contract

Change the semantic owner and its focused behavioral tests first, then update
the contract and affected guides/checklist. Prefer links to compiled examples
over copied complete applications. Use a marked data block only for facts that
can be compared directly with a compiled owner; do not invent a second format
registry or a generated prose system.

For a documentation-only correction, inspect the linked owners and run the
structural check. Run the relevant native evidence when capabilities, example
code, or codec claims change. Historical measurements remain explicitly scoped;
they never certify a new dependency set or release candidate automatically.
