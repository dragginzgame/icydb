# Code hygiene

Apply the [common audit contract](README.md). Review concrete maintenance and
change-safety risks after a substantial API, generator or tooling change, or
when requested. This method uses the [Rust hygiene baseline](../docs/principles/rust-code-hygiene.md)
where applicable; consumers supply other language conventions and commands.

## Review

1. Inventory public APIs, trust-state transitions, source/test/generated roots,
   and tracked versus ignored artifacts. Separate raw, decoded, validated and
   authoritative data, including constructors and deserialization paths.
2. Classify panic/unwrap/expect, unsafe code, unchecked conversions, broad lint
   suppressions and language escape hatches by reachability and consequence.
   Presence alone is not a defect. Invalid input and recoverable state need typed
   failures; public panic contracts and safety assumptions need documentation.
3. Inspect boundary validation and negative tests. Errors should retain actionable
   typed context through callers. Golden-path tests, rendered-string matching,
   and tests of private layout do not substitute for the maintained contract.
4. Check public documentation and examples against current behavior. Identify
   accidental visibility, test-only production APIs, obsolete comments and
   dependency/feature declarations with no maintained purpose.
5. Inspect large files by responsibility and callers. Authored data volume,
   generated code and atomic recovery transitions are not defects merely because
   they are large. Route deletion/exposure investigations to
   [module surface hardening](module-surface-hardening.md), repeated semantic
   ownership to [flow convergence](flow-convergence-and-duplication.md).
6. Inspect the selected quality gates and, within authorization, run the smallest
   relevant non-mutating formatting, lint, type, generated-parity or focused
   test checks. Broad CI, builds and release gates retain their own authority.
   Verify test selections execute the intended assertions, not zero tests.

## Findings

Classify work as mechanical, behavioral or design, with concrete impact and a
smallest owning correction. Record justified exceptions and their drift checks.
Repository artifacts, frontend wire decoding, shell error propagation and Rust
APIs may need different evidence; a language-wide pass is not a product verdict.

Vulnerability freshness, protocol correctness, deployed behavior and performance
are separate questions. Route an observed defect to its owning method/issue;
do not imply those domains passed because hygiene checks passed. An audit does
not automatically fix even mechanical findings.
