//! Module: db::session::query::paging
//! Responsibility: scalar paging and grouped query cursor orchestration.
//! Does not own: fluent terminal adaptation, explain rendering, or diagnostics attribution.
//! Boundary: delegates scalar and grouped paging to owner-focused child modules.

mod grouped;
mod scalar;
