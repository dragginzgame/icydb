# Shared audit methods

These methods apply the [engineering baseline](../DRAGGINZGAME.md) to a named
review scope. Shared Tooling owns the common questions and evidence contract;
consumers own product invariants, source roots, focused commands and report
locations. Adopting these files does not schedule audits or add release gates.
The [task catalog](../tasks/README.md) selects bounded recurring audit work and
links these methods; it does not maintain a second set of audit questions.

## Choose a method

| Question | Method |
| --- | --- |
| Are errors, APIs, tests, documentation and repository artifacts maintained safely? | [Code hygiene](code-hygiene.md) |
| Does equivalent behavior have one semantic owner and converge through one flow? | [Flow convergence and duplication](flow-convergence-and-duplication.md) |
| Do current behavior axes and ownership spread create demonstrated maintenance friction? | [Complexity and technical debt](complexity-and-technical-debt.md) |
| Which retained or exposed code units lack a current authority reason? | [Module surface hardening](module-surface-hardening.md) |
| How should an authorized module cleanup implement those findings? | [Module cleanup](module-cleanup.md), an implementation procedure, not another audit |
| How do those methods apply to Candid services, canister lifecycle and Wasm budgets? | [IC canister application addendum](ic-canister-applications.md), alongside the selected method |

Select the smallest method and affected owners that answer the request. Follow
contracts through producers, consumers, generated boundaries and recovery paths;
changed lines alone do not define the scope. State excluded families and reasons.
A missing required proof is a gap, not an exclusion. A recurring label does not
require a whole-system sweep; broad baselines need an explicit request.

Security properties, storage correctness, lifecycle protocols, deployment
qualification and performance measurements remain with their owning domain
methods. Share exact evidence where obligations overlap, without merging those
distinct questions into a general structural verdict.

## Authority and execution

- Inspection or feedback requests return findings in the conversation unless
  saving a report is requested. Running an audit permits its new report and
  necessary focused verification outputs within the user's scope. An explicit
  read-only constraint prohibits repository writes and execution that writes.
- An audit does not authorize fixes, dependency updates, broad CI/release gates,
  service mutation, deployment or publication. Honor existing explicit authority
  for a bounded repair; do not request it again. Commit ownership and other effect
  boundaries remain those of the baseline.
- Read the local overlay and selected method before execution. Inspect command
  effects before running them. Keep failed evidence, avoid repeated expensive
  failures, and report unavailable checks without substituting weaker proof.
- Keep the method fixed during a run. Improving a method is a separate scope
  unless already requested. Do not silently alter the questions to obtain PASS.

## Evidence and report contract

Record the method path, exact Shared Tooling revision and consumer overlay
identity, source commit and relevant dirty changes, trigger, scope, baseline,
and inspection/execution mode. Behavioral or measured evidence also identifies
commands, toolchain, features, dependencies, host, configuration and artifacts.
Distinguish source inspection, substitutes, native execution and deployed state.

Reuse evidence only when its identities and assertions prove the selected
obligation. Link the original artifact; do not count it as a new execution or
infer proof from another report's PASS. Method/scope changes make affected
comparisons `N/A (method change)`; name any unchanged comparable anchors.

Use a compact report with:

1. Scope, identities, comparison limits and verdict.
2. The selected method's owner/inventory/trace evidence.
3. Findings: consequence, evidence, severity, owner, disposition and action trigger.
4. Intentional retention/no-action decisions and verification gaps.
5. Checks performed and their results, skipped checks and remaining adoption work.

Use `LOW`, `MEDIUM` or `HIGH` severity per finding, justified by present impact.
HIGH means an evidenced serious correctness, integrity, security, availability
or authority failure; MEDIUM means a bounded defect, material proof gap or
maintenance burden requiring an owner decision; LOW has limited present impact.
Missing proof is not itself proof of a runtime defect. Counts and file sizes
guide inspection; do not turn them into composite health or risk scores.

| Verdict | Meaning within the declared scope |
| --- | --- |
| `PASS` | Required evidence is sufficient; no actionable findings remain. |
| `PASS WITH FINDINGS` | Required obligations are supported; bounded findings remain without a demonstrated contract failure. |
| `FAIL` | Evidence demonstrates a violated required contract or failed required check. |
| `BLOCKED` | Necessary evidence is unavailable, so the scoped conclusion cannot be established. |

Report known failures even when other proof is blocked. Never label excluded or
unverified behavior PASS. Prioritize at most five actionable structural findings
in the summary; retain any additional evidenced defects and link their owning
issues so a presentation limit never hides a serious finding. No finding is a
valid outcome. Do not manufacture work to fill a template.
When the requester explicitly asks for a ranked backlog, expand the finding list
and group it by severity and owner; keep evidence and follow-up in their existing
owners instead of creating a second tracker.

## Ownership and history

One underlying cause has one owning finding, selected by the violated contract.
Adjacent methods link that evidence and explain their own consequence. GitHub
issues remain the sole active follow-up tracker; reports are immutable evidence,
not another debt ledger. Relevant issue work has the baseline's standing
authorization only in `dragginzgame/*`; issue actions in other GitHub repositories
require explicit authorization. Report a prepared external proposal until that
authorization is available. Repository file edits retain separate authority.

Reports and necessary artifacts stay in the audited repository's established
report hierarchy. If none exists, use a new
`docs/reports/audits/YYYY/MM/DD/<scope>/<run>/report.md`, with numbered runs from
`01` and artifacts alongside. Never overwrite historical runs, relabel old
evidence, or move consumer reports into Shared Tooling during consolidation.

## Consumer overlay and adoption

Adopt a reviewed committed revision through the
[snapshot procedure](../docs/consuming-snapshots.md#audit-method-adoption).
Keep shared files unchanged at `audits/`. A local method should contain only:

- the selected shared method and its snapshot identity;
- product authorities, invariants and relevant source/entrypoint families;
- local scope exclusions, generated consumers and retained-state obligations;
- focused verification commands, their effects and product-approved cost metrics;
- local report destination and any approved exceptions.

For a first run without an adopted overlay, state provisional scope and product
authorities in that report and identify the missing adoption evidence. Review
what the available evidence supports; missing overlay adoption alone is not a
runtime failure. Mark conclusions needing unavailable product invariants BLOCKED,
and do not claim baseline adoption or comparability with a qualified prior run.

Replace duplicated generic instructions when the reviewed snapshot is installed.
Preserve product obligations by mapping them to the local overlay or an owning
domain method before retiring an old definition. Update catalogs, callers and
method identity records together; preserve historical definitions where needed
to reproduce frozen reports, marking them ineligible for new runs. Do not claim
old score histories are comparable to these finding-based methods.

Sources and deliberate differences are recorded in [provenance](../docs/provenance.md).
