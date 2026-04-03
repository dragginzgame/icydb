//! Module: db::session::sql::projection
//! Responsibility: module-local ownership and contracts for db::session::sql::projection.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

mod labels;
mod payload;

pub(in crate::db::session::sql) use crate::db::session::sql::projection::{
    labels::{
        projection_labels_from_entity_model, projection_labels_from_projection_spec,
        sql_projection_rows_from_kernel_rows,
    },
    payload::SqlProjectionPayload,
};
