Concrete Codex prompt (you can use this)

Here’s a practical version you can hand to Codex:

Prompt:

Scan the icydb-core codebase and build a randomized SQL test generator for scalar SELECT queries over a single table.

The generator should construct WHERE clauses using combinations of:

arithmetic expressions (+, -, *, /)
comparison operators (=, <, >, <=, >=)
boolean operators (AND, OR, NOT)
scalar functions (LOWER, UPPER, LENGTH, TRIM, ABS, ROUND, CEIL, FLOOR, COALESCE, NULLIF)
searched CASE expressions
BETWEEN and IN (literal lists only)
IS TRUE / IS FALSE

The generator must vary:

expression nesting depth (1–4 levels)
operand types (integer, decimal, text, boolean)
null placement
pushdown vs residual shapes

For each generated query:

Execute normally
Execute via prepared query (fallback path)
Capture results and any errors

Then classify each query into:

accepted and correct
unexpected rejection
result mismatch between execution modes
internal error or panic

Log all failing or mismatching queries along with:

the generated SQL
classification
simplified minimized form if possible

The goal is to discover:

scalar WHERE admission gaps
type or null semantics inconsistencies
predicate vs residual execution mismatches
prepared execution divergences

Keep generation within the current admitted SQL surface (no JOINs, no subqueries, no aggregates in WHERE).

Optional upgrade (very powerful)

Add differential testing against projection:

For each WHERE expression:

SELECT *, <expr> AS test_expr FROM Character

Then compare:

WHERE <expr>

vs

WHERE test_expr IS TRUE

You’re checking:

WHERE semantics == projection boolean semantics

This catches a ton of subtle bugs.

Why this works well for your architecture

Because your system now has:

unified expression model
explicit predicate/residual split
compiled execution

So the space is:

large but structurally regular

Which is perfect for matrix fuzzing.

Bottom line

Yes — you absolutely can (and should) build a multi-dimensional search like this.

But the trick is:

don’t search randomly — search across dimensions that reflect your architecture

If you do this, you’ll quickly find:

real edge-case bugs
hidden admission gaps
inconsistencies between predicate and expression paths

And those map directly to the kinds of slices you’re already doing (0.102, 0.103, 0.104, 0.105).
