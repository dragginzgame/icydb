//! Native session-level correctness harnesses that require the complete SQL facade.

pub(in crate::db::session) mod cardinality_tiebreak;
mod tier_c_reference;
mod unit_ordering;
