mod labels;
mod payload;

pub(in crate::db::session::sql) use crate::db::session::sql::projection::{
    labels::{
        projection_labels_from_entity_model, projection_labels_from_structural_query,
        sql_projection_rows_from_kernel_rows,
    },
    payload::SqlProjectionPayload,
};
