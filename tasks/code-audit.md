# Bounded code audit

Apply the [common run contract](README.md) and
[shared audit contract](../audits/README.md). Choose one repository and a concrete
module, public boundary or changed flow. Use recent changes or an unreviewed
high-impact boundary to choose scope; record the selection and exclusions.

Choose the existing method that answers the question:

- [Code hygiene](../audits/code-hygiene.md) for error handling, boundaries and tests.
- [Complexity](../audits/complexity-and-technical-debt.md) for demonstrated maintenance friction.
- [Module surface](../audits/module-surface-hardening.md) for unjustified retained or exposed code.

For IC applications, include the [canister addendum](../audits/ic-canister-applications.md)
where relevant. Read product authorities from the local overlay. Follow the
selected method through callers, generated consumers and retained-data or
recovery obligations; do not infer correctness from changed lines alone.

Compare with the last compatible report when one exists. State whether findings
are new, resolved, unchanged or unverified. Keep methods and evidence fixed for
this run, and use the existing verdict/severity definitions. A no-finding result
is valid; file size alone is not a defect.

Return a bounded report with exact source/scope, demonstrated consequences,
smallest owning corrections and issue links. Retain proof gaps and intentional
retention decisions. This task does not perform cleanup or claim that unreviewed
modules, security properties or deployed behavior passed.
