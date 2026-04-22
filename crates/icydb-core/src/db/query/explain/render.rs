//! Module: query::explain::render
//! Responsibility: text/json rendering for explain execution descriptors.
//! Does not own: planner or executor decision derivation.
//! Boundary: consumes explain projection types and emits deterministic render output.

use crate::db::query::explain::{
    ExplainExecutionNodeDescriptor, ExplainPropertyMap, FinalizedQueryDiagnostics,
    execution::{execution_mode_label, ordering_source_label},
    nodes::{
        execution_mode_detail_label, fast_path_reason, fast_path_selected, predicate_pushdown_mode,
    },
};
use crate::db::query::plan::explain_access_strategy_label;
use std::fmt::Write;

impl ExplainExecutionNodeDescriptor {
    /// Render this execution subtree as a compact text tree.
    #[must_use]
    pub fn render_text_tree(&self) -> String {
        let mut out = String::new();
        let mut node_id_counter = 0_u64;
        self.render_text_tree_into(0, &mut node_id_counter, &mut out);
        out
    }

    /// Render this execution subtree as a verbose text tree with properties.
    #[must_use]
    pub fn render_text_tree_verbose(&self) -> String {
        self.render_text_tree_verbose_with_indent("")
    }

    /// Render this execution subtree as one verbose text tree with one
    /// caller-owned line prefix applied to every emitted line.
    #[must_use]
    pub fn render_text_tree_verbose_with_indent(&self, indent: &str) -> String {
        let mut out = String::new();
        let mut node_id_counter = 0_u64;
        self.render_text_tree_verbose_into(indent, 0, &mut node_id_counter, &mut out);
        out
    }

    fn render_text_tree_into(&self, depth: usize, node_id_counter: &mut u64, out: &mut String) {
        let node_id = *node_id_counter;
        *node_id_counter = node_id_counter.saturating_add(1);
        push_rendered_line_prefix(out, depth);
        let _ = write!(
            out,
            "{} execution_mode={}",
            self.node_type.as_str(),
            execution_mode_label(self.execution_mode)
        );
        let _ = write!(out, " node_id={node_id}");
        let _ = write!(out, " layer={}", self.node_type.layer_label());
        let _ = write!(
            out,
            " execution_mode_detail={}",
            execution_mode_detail_label(self.execution_mode)
        );
        let _ = write!(
            out,
            " predicate_pushdown_mode={}",
            predicate_pushdown_mode(self)
        );
        if let Some(fast_path_selected) = fast_path_selected(self) {
            let _ = write!(out, " fast_path_selected={fast_path_selected}");
        }
        if let Some(fast_path_reason) = fast_path_reason(self) {
            let _ = write!(out, " fast_path_reason={fast_path_reason}");
        }

        if let Some(access_strategy) = self.access_strategy.as_ref() {
            out.push_str(" access=");
            out.push_str(explain_access_strategy_label(access_strategy).as_str());
        }
        if let Some(predicate_pushdown) = self.predicate_pushdown.as_ref() {
            let _ = write!(out, " predicate_pushdown={predicate_pushdown}");
        }
        if let Some(filter_expr) = self.filter_expr.as_ref() {
            let _ = write!(out, " filter_expr={filter_expr}");
        }
        if let Some(residual_filter_expr) = self.residual_filter_expr.as_ref() {
            let _ = write!(out, " residual_filter_expr={residual_filter_expr}");
        }
        if let Some(residual_filter_predicate) = self.residual_filter_predicate.as_ref() {
            let _ = write!(
                out,
                " residual_filter_predicate={residual_filter_predicate:?}"
            );
        }
        if let Some(projection) = self.projection.as_ref() {
            let _ = write!(out, " projection={projection}");
        }
        if let Some(ordering_source) = self.ordering_source {
            let _ = write!(
                out,
                " ordering_source={}",
                ordering_source_label(ordering_source)
            );
        }
        if let Some(limit) = self.limit {
            let _ = write!(out, " limit={limit}");
        }
        if let Some(cursor) = self.cursor {
            let _ = write!(out, " cursor={cursor}");
        }
        if let Some(covering_scan) = self.covering_scan {
            let _ = write!(out, " covering_scan={covering_scan}");
        }
        if let Some(rows_expected) = self.rows_expected {
            let _ = write!(out, " rows_expected={rows_expected}");
        }
        if !self.node_properties.is_empty() {
            out.push_str(" node_properties=");
            write_node_properties(out, &self.node_properties);
        }

        for child in &self.children {
            child.render_text_tree_into(depth.saturating_add(1), node_id_counter, out);
        }
    }

    fn render_text_tree_verbose_into(
        &self,
        base_indent: &str,
        depth: usize,
        node_id_counter: &mut u64,
        out: &mut String,
    ) {
        let node_id = *node_id_counter;
        *node_id_counter = node_id_counter.saturating_add(1);

        // Emit the node heading line first so child metadata stays visually scoped
        // without rebuilding indentation strings per node.
        push_rendered_line_prefix_with_base_depth(out, base_indent, depth);
        let _ = write!(
            out,
            "{} execution_mode={}",
            self.node_type.as_str(),
            execution_mode_label(self.execution_mode)
        );
        push_rendered_line_prefix_with_base_depth(out, base_indent, depth.saturating_add(1));
        let _ = write!(out, "node_id={node_id}");
        push_rendered_line_prefix_with_base_depth(out, base_indent, depth.saturating_add(1));
        let _ = write!(out, "layer={}", self.node_type.layer_label());
        push_rendered_line_prefix_with_base_depth(out, base_indent, depth.saturating_add(1));
        let _ = write!(
            out,
            "execution_mode_detail={}",
            execution_mode_detail_label(self.execution_mode)
        );
        push_rendered_line_prefix_with_base_depth(out, base_indent, depth.saturating_add(1));
        let _ = write!(
            out,
            "predicate_pushdown_mode={}",
            predicate_pushdown_mode(self)
        );
        if let Some(fast_path_selected) = fast_path_selected(self) {
            push_rendered_line_prefix_with_base_depth(out, base_indent, depth.saturating_add(1));
            let _ = write!(out, "fast_path_selected={fast_path_selected}");
        }
        if let Some(fast_path_reason) = fast_path_reason(self) {
            push_rendered_line_prefix_with_base_depth(out, base_indent, depth.saturating_add(1));
            let _ = write!(out, "fast_path_reason={fast_path_reason}");
        }

        // Emit all optional node-local fields in a deterministic order.
        self.render_text_tree_verbose_node_fields(base_indent, depth.saturating_add(1), out);

        // Recurse in execution order to preserve stable tree topology.
        for child in &self.children {
            child.render_text_tree_verbose_into(
                base_indent,
                depth.saturating_add(1),
                node_id_counter,
                out,
            );
        }
    }

    fn render_text_tree_verbose_node_fields(
        &self,
        base_indent: &str,
        field_depth: usize,
        out: &mut String,
    ) {
        if let Some(access_strategy) = self.access_strategy.as_ref() {
            push_rendered_line_prefix_with_base_depth(out, base_indent, field_depth);
            out.push_str("access_strategy=");
            out.push_str(explain_access_strategy_label(access_strategy).as_str());
        }
        if let Some(predicate_pushdown) = self.predicate_pushdown.as_ref() {
            push_rendered_line_prefix_with_base_depth(out, base_indent, field_depth);
            let _ = write!(out, "predicate_pushdown={predicate_pushdown}");
        }
        if let Some(filter_expr) = self.filter_expr.as_ref() {
            push_rendered_line_prefix_with_base_depth(out, base_indent, field_depth);
            let _ = write!(out, "filter_expr={filter_expr}");
        }
        if let Some(residual_filter_expr) = self.residual_filter_expr.as_ref() {
            push_rendered_line_prefix_with_base_depth(out, base_indent, field_depth);
            let _ = write!(out, "residual_filter_expr={residual_filter_expr}");
        }
        if let Some(residual_filter_predicate) = self.residual_filter_predicate.as_ref() {
            push_rendered_line_prefix_with_base_depth(out, base_indent, field_depth);
            let _ = write!(
                out,
                "residual_filter_predicate={residual_filter_predicate:?}"
            );
        }
        if let Some(projection) = self.projection.as_ref() {
            push_rendered_line_prefix_with_base_depth(out, base_indent, field_depth);
            let _ = write!(out, "projection={projection}");
        }
        if let Some(ordering_source) = self.ordering_source {
            push_rendered_line_prefix_with_base_depth(out, base_indent, field_depth);
            let _ = write!(
                out,
                "ordering_source={}",
                ordering_source_label(ordering_source)
            );
        }
        if let Some(limit) = self.limit {
            push_rendered_line_prefix_with_base_depth(out, base_indent, field_depth);
            let _ = write!(out, "limit={limit}");
        }
        if let Some(cursor) = self.cursor {
            push_rendered_line_prefix_with_base_depth(out, base_indent, field_depth);
            let _ = write!(out, "cursor={cursor}");
        }
        if let Some(covering_scan) = self.covering_scan {
            push_rendered_line_prefix_with_base_depth(out, base_indent, field_depth);
            let _ = write!(out, "covering_scan={covering_scan}");
        }
        if let Some(rows_expected) = self.rows_expected {
            push_rendered_line_prefix_with_base_depth(out, base_indent, field_depth);
            let _ = write!(out, "rows_expected={rows_expected}");
        }
        if !self.node_properties.is_empty() {
            push_rendered_line_prefix_with_base_depth(out, base_indent, field_depth);
            out.push_str("node_properties:");

            // Expand each stable property onto its own line so verbose explain
            // stays readable even when route diagnostics grow.
            for (key, value) in self.node_properties.iter() {
                push_rendered_line_prefix_with_base_depth(
                    out,
                    base_indent,
                    field_depth.saturating_add(1),
                );
                let _ = write!(out, "{key}={value:?}");
            }
        }
    }
}

impl FinalizedQueryDiagnostics {
    /// Render the frozen verbose diagnostics artifact as deterministic text.
    #[must_use]
    pub(crate) fn render_text_verbose(&self) -> String {
        self.render_text_verbose_with_tree_indent("")
    }

    /// Render the frozen verbose diagnostics artifact with one caller-owned
    /// indent prefix applied to the execution tree only.
    #[must_use]
    pub(crate) fn render_text_verbose_with_tree_indent(&self, tree_indent: &str) -> String {
        let mut lines = vec![
            self.execution()
                .render_text_tree_verbose_with_indent(tree_indent),
        ];
        lines.extend(self.route_diagnostics.iter().cloned());
        lines.extend(self.logical_diagnostics.iter().cloned());
        if let Some(reuse) = self.reuse {
            let artifact = match reuse.artifact_class() {
                crate::db::TraceReuseArtifactClass::SharedPreparedQueryPlan => {
                    "shared_prepared_query_plan"
                }
            };
            let outcome = if reuse.is_hit() { "hit" } else { "miss" };
            lines.push(format!("diag.s.semantic_reuse_artifact={artifact}"));
            lines.push(format!("diag.s.semantic_reuse={outcome}"));
        }

        lines.join("\n")
    }
}

fn push_rendered_line_prefix(out: &mut String, depth: usize) {
    if !out.is_empty() {
        out.push('\n');
    }

    for _ in 0..depth {
        out.push_str("  ");
    }
}

fn push_rendered_line_prefix_with_base_depth(out: &mut String, base_indent: &str, depth: usize) {
    if !out.is_empty() {
        out.push('\n');
    }
    out.push_str(base_indent);

    for _ in 0..depth {
        out.push_str("  ");
    }
}

fn write_node_properties(out: &mut String, node_properties: &ExplainPropertyMap) {
    let mut first = true;
    for (key, value) in node_properties.iter() {
        if first {
            first = false;
        } else {
            out.push(',');
        }
        let _ = write!(out, "{key}={value:?}");
    }
}
