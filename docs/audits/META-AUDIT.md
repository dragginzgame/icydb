Perform a META-AUDIT of the existing audit documents under docs/audits/.

This is NOT a code audit.
This is an audit of the audit quality itself.

For each audit file:

1. Scope Discipline
   - Is the scope clearly constrained?
   - Does the audit stay within its declared focus?
   - Does it drift into style, refactor, or feature suggestions?

2. Invariant Focus
   - Does the audit explicitly identify invariants?
   - Are invariants described precisely?
   - Are boundary conditions enumerated?

3. Structural Depth
   - Does it reason about layer boundaries?
   - Does it compare normal execution vs recovery where applicable?
   - Does it identify idempotence and envelope risks?

4. Signal-to-Noise Ratio
   - Does it contain vague statements?
   - Does it contain generic advice?
   - Does it contain non-actionable commentary?

5. Risk Identification Quality
   - Are risks ranked?
   - Are high-risk findings clearly separated?
   - Are hypothetical risks clearly labeled as hypothetical?

6. Redundancy Detection
   - Do multiple audits duplicate the same reasoning?
   - Are some invariants audited in multiple documents without need?

7. Missing Dimensions
   - Identify invariant categories not yet covered by any audit.
   - Identify boundaries not yet audited.

Output Requirements:

- Audit Quality Score per document (1–10)
- Structural Weaknesses per document
- Drift Warnings (if any)
- Consolidation Opportunities
- New Audit Types Recommended (if any)

Do not evaluate code.
Only evaluate audit discipline and structural rigor.
