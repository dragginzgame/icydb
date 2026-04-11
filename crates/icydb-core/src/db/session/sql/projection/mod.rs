//! Module: db::session::sql::projection
//! Responsibility: session-owned SQL projection labels and payload shaping
//! helpers used by SQL dispatch result construction.
//! Does not own: structural projection execution or row materialization.
//! Boundary: keeps outward SQL projection naming and payload types together.

mod labels;
mod payload;

pub(in crate::db::session::sql) use crate::db::session::sql::projection::{
    labels::{
        projection_labels_from_fields, projection_labels_from_projection_spec,
        sql_projection_rows_from_kernel_rows,
    },
    payload::SqlProjectionPayload,
};
