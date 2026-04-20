//! Module: query::explain::json
//! Responsibility: canonical JSON rendering helpers for execution explain descriptors.
//! Does not own: execution decision derivation or text-tree rendering.
//! Boundary: deterministic JSON field ordering for execution explain output.

use crate::db::query::explain::{
    ExplainExecutionNodeDescriptor,
    access_projection::write_access_json,
    execution::{execution_mode_label, ordering_source_label},
    nodes::{
        execution_mode_detail_label, fast_path_reason, fast_path_selected, predicate_pushdown_mode,
    },
    writer::JsonWriter,
};

impl ExplainExecutionNodeDescriptor {
    /// Render this execution subtree as canonical JSON.
    #[must_use]
    pub fn render_json_canonical(&self) -> String {
        let mut out = String::new();
        let mut node_id_counter = 0_u64;
        write_execution_node_json(self, &mut node_id_counter, &mut out);
        out
    }
}

fn write_execution_node_json(
    node: &ExplainExecutionNodeDescriptor,
    node_id_counter: &mut u64,
    out: &mut String,
) {
    let node_id = *node_id_counter;
    *node_id_counter = node_id_counter.saturating_add(1);
    let mut object = JsonWriter::begin_object(out);

    object.field_u64("node_id", node_id);
    object.field_str("node_type", node.node_type().as_str());
    object.field_str("layer", node.node_type().layer_label());
    object.field_str(
        "execution_mode",
        execution_mode_label(node.execution_mode()),
    );
    object.field_str(
        "execution_mode_detail",
        execution_mode_detail_label(node.execution_mode()),
    );
    object.field_with("access_strategy", |out| {
        match node.access_strategy().as_ref() {
            Some(access) => write_access_json(access, out),
            None => out.push_str("null"),
        }
    });
    object.field_str("predicate_pushdown_mode", predicate_pushdown_mode(node));
    match node.predicate_pushdown() {
        Some(predicate_pushdown) => object.field_str("predicate_pushdown", predicate_pushdown),
        None => object.field_null("predicate_pushdown"),
    }
    match node.filter_expr() {
        Some(filter_expr) => object.field_str("filter_expr", filter_expr),
        None => object.field_null("filter_expr"),
    }
    match fast_path_selected(node) {
        Some(selected) => object.field_bool("fast_path_selected", selected),
        None => object.field_null("fast_path_selected"),
    }
    match fast_path_reason(node) {
        Some(reason) => object.field_str("fast_path_reason", reason),
        None => object.field_null("fast_path_reason"),
    }
    match node.residual_predicate() {
        Some(residual_predicate) => {
            object.field_value_debug("residual_predicate", residual_predicate);
        }
        None => object.field_null("residual_predicate"),
    }
    match node.projection() {
        Some(projection) => object.field_str("projection", projection),
        None => object.field_null("projection"),
    }
    match node.ordering_source() {
        Some(ordering_source) => {
            object.field_str("ordering_source", ordering_source_label(ordering_source));
        }
        None => object.field_null("ordering_source"),
    }
    match node.limit() {
        Some(limit) => object.field_u64("limit", u64::from(limit)),
        None => object.field_null("limit"),
    }
    match node.cursor() {
        Some(cursor) => object.field_bool("cursor", cursor),
        None => object.field_null("cursor"),
    }
    match node.covering_scan() {
        Some(covering_scan) => object.field_bool("covering_scan", covering_scan),
        None => object.field_null("covering_scan"),
    }
    match node.rows_expected() {
        Some(rows_expected) => object.field_u64("rows_expected", rows_expected),
        None => object.field_null("rows_expected"),
    }
    object.field_with("children", |out| {
        out.push('[');
        for (index, child) in node.children().iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            write_execution_node_json(child, node_id_counter, out);
        }
        out.push(']');
    });
    object.field_debug_map("node_properties", node.node_properties());

    object.finish();
}
