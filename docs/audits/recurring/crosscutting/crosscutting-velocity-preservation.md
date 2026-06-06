# WEEKLY AUDIT — Velocity Preservation

`icydb-core`

## Purpose

Evaluate whether the current code organization preserves future feature
velocity.

This audit asks:

* Where would the next feature become hard to add?
* Which modules force unrelated owners to change together?
* Which boundaries are too porous or too implicit?
* Which decision surfaces will multiply update sites as the product grows?

This is NOT:

* a recent delivery-speed audit
* a PR file-count audit
* a correctness audit
* a DRY audit
* a style audit
* a redesign proposal exercise

This audit measures future extension friction from the code as it exists now.
Do not score recent patch history, commit size, or files touched by previous
slices. Those belong to delivery governance, not this recurring score.

## Audit Identity

Keep this as a distinct recurring audit.

Do not merge it with:

* Complexity accretion
* DRY consolidation
* Layer violation
* Module structure

Velocity is the extension-friction lens: whether future features can be added
through clear owners and stable contracts without cross-layer amplification.

Historical change data may be used only as non-scoring context after the score
is computed. It must not raise or lower the score.

---

# Core Principle

Low-risk velocity architecture has:

* clear owner boundaries
* stable contracts between layers
* few cross-cutting update requirements
* localized decision surfaces
* obvious extension paths for representative future features

Velocity degrades when:

* a feature must edit several owners before behavior can change
* planner, executor, cursor, storage, or facade code depends on each other's
  internal decisions
* module roots become coordination gravity wells
* one enum or decision family requires repeated switch-site edits across layers
* generated or facade surfaces force runtime semantics to follow fixture shape

---

# STEP 0 — Run Metadata + Method (Mandatory)

Capture method metadata before scoring.

Required run metadata:

* code snapshot identifier
* dirty-worktree status
* method tag/version
* subsystem taxonomy version
* boundary-crossing regex/rule set version
* fan-in/fanout definition
* hub-family taxonomy version
* decision-surface rule version
* facade/adapters inclusion mode

Produce:

| Method Component | Current |
| ---- | ---- |
| code snapshot identifier |  |
| dirty-worktree status |  |
| method tag/version | `VP-FEF-1.0` |
| subsystem taxonomy |  |
| boundary crossing rule set |  |
| fan-in/fanout definition |  |
| hub-family taxonomy |  |
| decision-surface rule |  |
| facade/adapters inclusion |  |

Rules:

* Score only the current code snapshot.
* If the worktree is dirty, state whether dirty files affect the audited
  surfaces.
* Do not compute deltas against prior reports in the scored sections.
* Prior reports may appear only in an optional non-scoring appendix.

---

# STEP 1 — Scope + Ownership Map (Mandatory)

Map the current code ownership before judging risk.

Fixed subsystem taxonomy:

* planner/query
* executor/runtime
* cursor/continuation
* access/index
* storage/recovery
* schema/catalog
* SQL parser/lowering/session
* facade/adapters
* generated/test support

Produce:

| Subsystem | Primary Owner Modules | Public/Crate Boundary | Runtime Authority | Notes |
| ---- | ---- | ---- | ---- | ---- |

Flag:

* runtime behavior owned by generated/test/facade code
* owner modules that expose broad `pub(crate)` surfaces without nonlocal need
* module roots that coordinate unrelated policy families

---

# STEP 2 — Future Feature Probes (Mandatory)

Choose 3-5 plausible future feature probes. These are hypothetical extension
paths, not recent landed slices.

Selection rules:

* Pick probes from product-shaped growth areas, not from commit history.
* Include at least one query/planner or executor probe when relevant.
* Include a schema/storage or generated/facade probe only if the current code
  suggests pressure there.
* Prefer probes that would naturally stress known boundaries.

Examples:

* add a new SQL aggregate behavior
* add a new order/cursor policy
* add a new persisted scalar kind
* add a new generated canister endpoint class
* add a new index scan route shape

Produce:

| Future Feature Probe | Expected Owner | Required Modules | Layers Crossed | Contract Blockers | Risk |
| ---- | ---- | ----: | ----: | ---- | ---- |

Method:

* Read current code and imports to estimate expected edit points.
* Count only modules that would need semantic changes, not tests or fixtures.
* Treat a probe as high risk if ownership is unclear or if unrelated layers must
  change before the feature can be expressed.

---

# STEP 3 — Boundary Leakage (Mechanical + Triaged)

Track current code crossings with two-stage classification.

Required checks:

* planner/query -> executor runtime internals
* executor/runtime -> query/sql internals
* index/access -> query/sql AST or lowering types
* cursor/continuation -> executable plan internals
* storage/recovery -> query semantics
* generated/facade -> runtime semantic authority

Produce:

| Boundary | Mechanical Crossings | Allowed Contract Crossings | Suspect Crossings | Risk |
| ---- | ----: | ----: | ----: | ---- |

Method:

* First pass must be mechanical using the current rule set.
* Second pass triages into allowed contract crossings vs suspect crossings.
* A crossing is suspect when the callee owns behavior the caller should not know
  about, or when adding a feature would require the caller to mirror callee
  decisions.

---

# STEP 4 — Owner / Contract Clarity

Evaluate whether future features have obvious places to land.

Produce:

| Surface | Owner | Contract Type | Ambiguity | Extension Impact | Risk |
| ---- | ---- | ---- | ---- | ---- | ---- |

Contract types:

* `facade-public`
* `crate-boundary`
* `subsystem-boundary`
* `owner-private`
* `generated-boundary`
* `test-support`

Flag:

* broad `pub(crate)` or `pub` surfaces without external authority
* re-exports that turn owner-private helpers into subsystem contracts
* runtime fallbacks reconstructed from generated models
* test-only helpers that widen production visibility
* modules whose header responsibility does not match their imports or exports

---

# STEP 5 — Gravity Wells + Hub Containment

Fan-in/fanout definitions for this audit:

* `fan_in = number of runtime modules referencing the module`
* `fanout = number of runtime module families referenced by the module`
* count import and type-reference sites at module granularity
* exclude tests by default
* exclude generated code by default unless facade/adapters are in scope
* count re-export-driven runtime references when resolved

Produce gravity-well table:

| Module | Class | LOC | Fan-In | Fanout | Domains | Owner Clarity | Risk |
| ---- | ---- | ----: | ----: | ----: | ----: | ---- | ---- |

Gravity-well classes:

* `coordination hub`: intentionally gathers contracts for one owner
* `decision hub`: contains policy branches for multiple feature families
* `mixed-owner hub`: mixes policy, dispatch, and runtime behavior from
  different owners
* `stable large module`: large but owner-clear and not cross-domain

Produce hub containment table:

| Hub Module | Contract Boundary | Cross-Layer Families | Allowed Max | Status | Risk |
| ---- | ---- | ----: | ----: | ---- | ---- |

Required hubs:

* `executor/route/planner/mod.rs`
* SQL execution/session roots
* cursor/continuation roots
* any current module root over local threshold that imports more than one
  subsystem family

Gate guidance:

* route-planner high-impact cross-layer families target `<=1`
* a mixed-owner hub with no clear split point is high future-friction risk
* large owner-clear modules are monitor items, not automatic failures

---

# STEP 6 — Decision Shock Radius

Track current decision surfaces that would make future feature additions
expensive.

Produce:

| Decision Surface | Variants / Cases | Change-Relevant Sites | Modules | Subsystems | Shock Radius | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |

Definitions:

* `change-relevant site`: a place that would need semantic updates for a new
  case, not every syntactic match
* `shock_radius = variants_or_cases × change_relevant_sites × subsystems`

Flag:

* enums used as cross-layer policy buses
* match sites spread across planner, executor, storage, and facade layers
* generated fixtures that must change for runtime-only semantics
* decision surfaces with no owner-local strategy table or adapter

---

# STEP 7 — Subsystem Independence

Measure whether each subsystem can evolve without importing another subsystem's
private decisions.

Produce:

| Subsystem | Internal Imports | External Imports | LOC | Independence | Private Decision Imports | Risk |
| ---- | ----: | ----: | ----: | ----: | ----: | ---- |

Definitions:

* `independence = internal / (internal + external)`
* `private decision import`: an import of a helper/type that encodes another
  subsystem's policy rather than a stable contract

Low independence is only high risk when the imported surface is a private
decision surface or when future feature probes must cross that boundary.

---

# STEP 8 — Extension Path Rehearsal

For each future feature probe from STEP 2, write the expected extension path.

Produce:

| Probe | Ideal Owner-Local Path | Actual Current Path | Extra Owners Required | Main Blocker | Risk |
| ---- | ---- | ---- | ----: | ---- | ---- |

Guidance:

* The ideal path should name one primary owner and any stable contracts it would
  call.
* The actual path should name the modules that would probably need changes.
* Count an extra owner only when semantic behavior must change there.
* Tests, fixtures, and generated expectations are support cost, not semantic
  owner count, unless they own runtime behavior.

---

# STEP 9 — Future Extension Friction Index

Score each bucket from `1` to `10`, then apply the weighted aggregate.

Lower is better.

Weighted buckets:

* future feature probe friction ×3
* boundary leakage ×2
* owner/contract clarity ×2
* gravity-well and hub containment ×2
* decision shock radius ×2
* subsystem independence ×1

Produce:

| Area | Score | Weight | Weighted Score | Evidence |
| ---- | ----: | ----: | ----: | ---- |

`future_extension_friction_index = weighted_sum / weight_sum`

Rubric anchors:

Future feature probe friction:

* `2`: probes land through one owner plus stable contracts
* `5`: one probe needs two owners or an unclear adapter
* `8`: multiple probes require cross-layer semantic edits
* `10`: routine probes require broad planner/executor/storage/facade edits

Boundary leakage:

* `2`: suspect crossings `<=2`, and no authority inversion
* `5`: suspect crossings `3-5`, or one moderate authority leak
* `8`: suspect crossings `>=6`, or a high-impact private decision import
* `10`: boundary inversion where runtime authority depends on facade/generated
  shape

Owner/contract clarity:

* `2`: most extension surfaces are owner-private or explicit contracts
* `5`: one important surface is overexposed or ambiguously owned
* `8`: multiple extension surfaces require reading unrelated owners
* `10`: no clear owner for routine feature classes

Gravity-well and hub containment:

* `2`: hubs are owner-clear and below cross-layer family limits
* `5`: one coordination hub needs monitoring
* `8`: one mixed-owner hub blocks clear extension paths
* `10`: multiple mixed-owner hubs absorb unrelated feature policy

Decision shock radius:

* `2`: new cases localize behind owner adapters or strategy tables
* `5`: one moderate decision surface has scattered update sites
* `8`: multiple decision surfaces require cross-subsystem updates
* `10`: a common feature class requires updating widespread switch sites

Subsystem independence:

* `2`: subsystems mostly import stable contracts
* `5`: moderate external imports, but few private decision imports
* `8`: private decision imports shape routine feature paths
* `10`: subsystems cannot express routine changes without mutual policy edits

Interpretation:

* `1-3`: low future extension friction
* `4-6`: moderate future extension friction
* `7-8`: high future extension friction; needs active organization work
* `9-10`: critical future extension friction; routine features are structurally
  blocked

Do not adjust this score for:

* recent files touched
* PR count
* commit size
* generated fixture churn
* docs/audit report breadth

---

# STEP 10 — Non-Scoring Delivery Context (Optional)

Use this section only when recent work explains why a future-friction risk was
noticed. It is not part of the score.

Produce:

| Context Signal | Observation | Why Non-Scoring |
| ---- | ---- | ---- |

Allowed context:

* a recent wide slice revealed an unclear owner
* a repeated review comment exposed a contract ambiguity
* fixture churn made a generated boundary suspicious

Forbidden context:

* raising risk because a recent slice touched many files
* lowering risk because a recent cleanup reduced file count
* using historical deltas as score evidence

---

# STEP 11 — Final Output + Verification Readout

Final output order:

1. Run Metadata + Method
2. Scope + Ownership Map
3. Future Feature Probes
4. Boundary Leakage
5. Owner / Contract Clarity
6. Gravity Wells + Hub Containment
7. Decision Shock Radius
8. Subsystem Independence
9. Extension Path Rehearsal
10. Future Extension Friction Index
11. Non-Scoring Delivery Context, if relevant
12. Verification Readout (`PASS`/`FAIL`/`BLOCKED`)

Verification readout must include:

* whether the score used only current-code evidence
* whether all mandatory steps/tables are present
* whether historical change data was excluded from scoring
* whether any dirty-worktree files affected the audited surfaces

---

# Anti-Shallow Rule

Do NOT say:

* "Seems modular"
* "Looks maintainable"
* "Separation is clear"

Every claim must include:

* subsystems involved
* dependency or boundary evidence
* expected future change multiplier
* growth vector

---

# Why This Audit Matters

Velocity audits should identify code that will slow future work before it does.

The useful output is not "the last slice was too wide." The useful output is
"this owner boundary, hub, or decision surface should be organized before the
next feature has to cross it."
