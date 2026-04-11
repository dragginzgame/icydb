//! Module: query::plan::validate::tests
//! Collects planner validation owner-level tests that span multiple grouped validation leaves.
//! Does not own: leaf-local helper invariants inside individual grouped validation modules.
//! Boundary: covers grouped validation policy/cursor behavior at the validate subsystem root.

mod grouped;
