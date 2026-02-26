### LLMs PLEASE IGNORE THIS FILE
### It's just here so I can manage multiple prompts without scrolling up and down constantly

Codex Prompt

You are performing a structural architecture audit on the IcyDB codebase.

Target file:
crates/icydb-core/src/db/executor/direction.rs

Your objective is to:

Determine whether Direction is defined in more than one domain.

Identify all imports of Direction across the codebase.

Detect any layering violations (e.g., executor depending on query::plan::Direction).

Consolidate Direction into a single canonical definition.

Refactor imports and re-exports so that:

Lower layers do not depend on higher layers.

Query, executor, and index share the same canonical type.

Ensure no semantic changes to ordering behavior.

Investigation Steps

Search the entire repository for:

enum Direction

pub enum Direction

use .*Direction

query::plan::Direction

executor::direction::Direction

index::.*Direction

Determine:

How many distinct Direction definitions exist.

Which module should be the canonical owner (likely a neutral layer such as db/order.rs or db/direction.rs).

Build a dependency map:

Which top-level domains import Direction?

query

executor

index

codec

lowering

Identify any upward dependency such as:

executor importing from query

index importing from query

executor depending on query plan types

These must be eliminated.

Refactoring Rules

If multiple Direction enums exist:

Create a single canonical definition in a neutral module:
Example location:
crates/icydb-core/src/db/order.rs

Canonical shape:

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Direction {
    Asc,
    Desc,
}

Remove duplicate definitions.

Replace all imports of:

query::plan::Direction

executor::direction::Direction

or any other duplicate

With:

crate::db::order::Direction

If necessary, re-export in higher layers for ergonomics:

In query::plan::mod.rs:

pub use crate::db::order::Direction;

In executor::mod.rs:

pub use crate::db::order::Direction;

Ensure no circular dependencies are introduced.

Validation Phase

After refactor:

Confirm:

Only one enum Direction exists.

No module depends upward on query internals.

All ordering behavior remains identical.

Run:

cargo check

cargo test

Ensure pagination, cursor, and index-range tests pass.

Search again for stray Direction definitions.

Deliverables

Produce:

A summary of findings:

How many definitions existed.

Where violations were found.

A clear list of modified files.

The final canonical Direction definition.

A dependency explanation confirming clean layering.

Do not change any ordering semantics.
Do not introduce new behavior.
This is a structural consolidation only.