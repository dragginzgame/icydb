//! Module: query::plan::validate::tests
//! Collects planner validation owner-level tests that span multiple grouped validation leaves.
//! Does not own: leaf-local helper invariants inside individual grouped validation modules.
//! Boundary: covers grouped validation policy/cursor behavior at the validate subsystem root.

mod cursor_policy;
mod fluent_policy;
mod grouped;
mod intent_policy;
