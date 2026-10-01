//! Module: db::executor::authority
//! Responsibility: executor-owned entity authority surfaces.
//! Does not own: route-local fast-path selection or store lifecycle gating.
//! Boundary: keeps structural entity identity under one executor root.

mod entity;

pub(in crate::db) use entity::EntityAuthority;
