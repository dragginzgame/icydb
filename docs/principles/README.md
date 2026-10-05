# Shared Engineering Principles

These documents contain repository-neutral decision guidance. They are
maintained from experience across consuming repositories, initially Canic and
IcyDB.

The [engineering baseline](../../DRAGGINZGAME.md) is mandatory for all `dragginzgame`
repositories; every consumer must adopt a reviewed revision and identify its
local overlay. These principles explain the baseline's decision tests. Product
architecture, exact commands, qualification gates, release targets and deployment
identities remain local. Conflicts with common rules require maintainer-approved
exceptions with scope and reason.

Current principles:

- [Simplicity and maintainability](simplicity-and-maintainability.md)
- [Decision artifact discipline](decision-artifact-discipline.md)
- [Reviewable changes](reviewable-changes.md)
- [Rust code hygiene baseline](rust-code-hygiene.md)

When experience differs between repositories, update the common decision test
here and preserve the valid local choices in consumer overlays.
