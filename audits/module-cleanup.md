# Authorized module cleanup

This procedure implements findings from [module surface hardening](module-surface-hardening.md)
or another named owning review. It does not issue an independent audit verdict.
Apply the [common authority and evidence contract](README.md).

1. Confirm the requested module and implementation scope, current source/dirty
   state, owner, finding and required proof. Existing bounded cleanup authority
   remains valid; an inspection-only request stops at recommendations.
2. Inspect callers and classify the candidate before editing. A small internal
   change can use a compact finding; public/generated/persisted/recovery and
   hot-path boundaries need their actual proof, not a mandatory large template.
3. Make the smallest justified deletion, visibility reduction, inline or owner
   move. Preserve unrelated work, product invariants, retained-state obligations
   and useful tests. Do not redesign the module or add generic machinery merely
   to shorten it. Low-confidence deletion waits for the missing evidence.
4. Update directly affected callers, generated producers/outputs, tests, docs
   and current changelog together within the authorized scope. Use the canonical
   generator. A compatibility-breaking removal needs the correct pending release
   line; metadata changes and release execution retain separate authority.
5. Format changed source and run the smallest meaningful owner/boundary checks.
   Apply the named measurement proof for changed hot or Wasm-sensitive shape.
   Do not substitute a broad gate for missing focused assertions or run one
   without authority. Preserve failed artifacts and report remaining limits.
6. Report changed behavior, validation, state-space/ownership effect, intentional
   retention and unresolved work. List every removed function, method and type
   with former owner, reason and replacement if any; distinguish moves/renames.

The original finding remains evidence. Record implementation and new checks with
their own source identity, and update the owning GitHub issue under the baseline's
standing issue authorization. Never rewrite a historical audit to imply the
repair was already present.
