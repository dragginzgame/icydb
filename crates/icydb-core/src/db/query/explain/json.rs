//! Module: query::explain::json
//! Responsibility: canonical JSON rendering helpers for execution explain descriptors.
//! Does not own: execution decision derivation or text-tree rendering.
//! Boundary: deterministic JSON field ordering for execution explain output.

use crate::db::{
    TraceReuseArtifactClass,
    query::{
        admission::QueryAdmissionSummary,
        explain::{
            ExplainExecutionNodeDescriptor, FinalizedQueryDiagnostics,
            access_projection::write_access_json,
            execution::{execution_mode_label, ordering_source_label},
            nodes::{
                execution_mode_detail_label, fast_path_reason, fast_path_selected,
                predicate_pushdown_mode,
            },
            writer::JsonWriter,
        },
    },
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

impl FinalizedQueryDiagnostics {
    /// Render this finalized execution diagnostics artifact as canonical JSON.
    #[must_use]
    pub(in crate::db) fn render_json_canonical(&self) -> String {
        let mut out = String::new();
        let mut object = JsonWriter::begin_object(&mut out);
        object.field_with("admission", |out| match self.admission() {
            Some(admission) => write_admission_json(admission, out),
            None => out.push_str("null"),
        });
        object.field_with("execution", |out| {
            let mut node_id_counter = 0_u64;
            write_execution_node_json(self.execution(), &mut node_id_counter, out);
        });
        object.field_str_slice("route_diagnostics", &self.route_diagnostics);
        object.field_str_slice("logical_diagnostics", &self.logical_diagnostics);
        object.field_with("reuse", |out| {
            let Some(reuse) = self.reuse else {
                out.push_str("null");
                return;
            };
            let mut reuse_object = JsonWriter::begin_object(out);
            reuse_object.field_str(
                "artifact",
                match reuse.artifact_class() {
                    TraceReuseArtifactClass::SharedPreparedQueryPlan => {
                        "shared_prepared_query_plan"
                    }
                },
            );
            reuse_object.field_str("outcome", if reuse.is_hit() { "hit" } else { "miss" });
            reuse_object.finish();
        });
        object.finish();

        out
    }
}

fn write_admission_json(admission: &QueryAdmissionSummary, out: &mut String) {
    let mut object = JsonWriter::begin_object(out);
    object.field_str("lane", admission.lane().as_str());
    object.field_str("decision", admission.decision().as_str());
    match admission.rejection() {
        Some(rejection) => object.field_str("reason", rejection.as_str()),
        None => object.field_null("reason"),
    }
    object.field_str("plan_shape", admission.plan_shape().as_str());
    object.field_str("selected_access", admission.selected_access().as_str());
    match admission.selected_index() {
        Some(selected_index) => object.field_str("selected_index", selected_index),
        None => object.field_null("selected_index"),
    }
    write_optional_u32(&mut object, "limit", admission.limit());
    write_optional_u32(&mut object, "offset", admission.offset());
    write_optional_u64(&mut object, "scan_bound", admission.scan_bound());
    object.field_str("scan_bound_kind", admission.scan_bound_kind().as_str());
    write_optional_u32(
        &mut object,
        "returned_row_bound",
        admission.returned_row_bound(),
    );
    object.field_str(
        "returned_row_bound_kind",
        admission.returned_row_bound_kind().as_str(),
    );
    write_optional_u32(
        &mut object,
        "response_byte_bound",
        admission.response_byte_bound(),
    );
    object.field_str(
        "response_byte_bound_kind",
        admission.response_byte_bound_kind().as_str(),
    );
    write_optional_u32(
        &mut object,
        "primary_key_input_terms",
        admission.primary_key_input_terms(),
    );
    write_optional_u32(
        &mut object,
        "primary_key_input_payload_bytes",
        admission.primary_key_input_payload_bytes(),
    );
    object.field_str("residual_filter", admission.residual_filter().as_str());
    object.field_str("ordering", admission.ordering().as_str());
    object.field_with("materialization", |out| {
        let materialization = admission.materialization();
        let mut materialization_object = JsonWriter::begin_object(out);
        materialization_object.field_bool("materialized_sort", materialization.materialized_sort());
        write_optional_u32(
            &mut materialization_object,
            "materialized_rows",
            materialization.materialized_rows(),
        );
        materialization_object
            .field_str("row_bound_kind", materialization.row_bound_kind().as_str());
        materialization_object.finish();
    });
    object.field_with("grouped", |out| {
        let Some(grouped) = admission.grouped() else {
            out.push_str("null");
            return;
        };
        let mut grouped_object = JsonWriter::begin_object(out);
        grouped_object.field_u64("group_field_count", u64::from(grouped.group_field_count()));
        grouped_object.field_u64("aggregate_count", u64::from(grouped.aggregate_count()));
        grouped_object.field_u64(
            "distinct_aggregate_count",
            u64::from(grouped.distinct_aggregate_count()),
        );
        grouped_object.field_u64("max_groups", grouped.max_groups());
        grouped_object.field_u64("max_group_bytes", grouped.max_group_bytes());
        grouped_object.field_bool("having_filter", grouped.has_having_filter());
        grouped_object.finish();
    });
    object.finish();
}

fn write_optional_u32(object: &mut JsonWriter<'_>, key: &str, value: Option<u32>) {
    match value {
        Some(value) => object.field_u64(key, u64::from(value)),
        None => object.field_null(key),
    }
}

fn write_optional_u64(object: &mut JsonWriter<'_>, key: &str, value: Option<u64>) {
    match value {
        Some(value) => object.field_u64(key, value),
        None => object.field_null(key),
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
    match node.residual_filter_expr() {
        Some(residual_filter_expr) => {
            object.field_str("residual_filter_expr", residual_filter_expr);
        }
        None => object.field_null("residual_filter_expr"),
    }
    match fast_path_selected(node) {
        Some(selected) => object.field_bool("fast_path_selected", selected),
        None => object.field_null("fast_path_selected"),
    }
    match fast_path_reason(node) {
        Some(reason) => object.field_str("fast_path_reason", reason),
        None => object.field_null("fast_path_reason"),
    }
    match node.residual_filter_predicate() {
        Some(residual_filter_predicate) => {
            object.field_value_debug("residual_filter_predicate", residual_filter_predicate);
        }
        None => object.field_null("residual_filter_predicate"),
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
