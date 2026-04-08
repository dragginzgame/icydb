//! Module: db::executor::authority
//! Responsibility: executor-owned entity authority surfaces.
//! Does not own: route-local fast-path selection or store lifecycle gating.
//! Boundary: keeps structural entity identity under one executor root without
//! reintroducing the removed secondary-read authority resolver.

mod entity;

pub use entity::EntityAuthority;
