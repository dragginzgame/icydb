//! Module: query::explain::render
//! Responsibility: text/json rendering for explain execution descriptors.
//! Does not own: planner or executor decision derivation.
//! Boundary: consumes explain projection types and emits deterministic render output.

use crate::db::query::explain::{
    ExplainExecutionNodeDescriptor, ExplainPropertyMap,
    access_projection::write_access_strategy_label,
    execution::{execution_mode_label, ordering_source_label},
    nodes::{
        execution_mode_detail_label, fast_path_reason, fast_path_selected, predicate_pushdown_mode,
    },
};
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
        let mut out = String::new();
        let mut node_id_counter = 0_u64;
        self.render_text_tree_verbose_into(0, &mut node_id_counter, &mut out);
        out
    }

    fn render_text_tree_into(&self, depth: usize, node_id_counter: &mut u64, out: &mut String) {
        let node_id = next_node_id(node_id_counter);
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
            write_access_strategy_label(out, access_strategy);
        }
        if let Some(predicate_pushdown) = self.predicate_pushdown.as_ref() {
            let _ = write!(out, " predicate_pushdown={predicate_pushdown}");
        }
        if let Some(residual_predicate) = self.residual_predicate.as_ref() {
            let _ = write!(out, " residual_predicate={residual_predicate:?}");
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
        depth: usize,
        node_id_counter: &mut u64,
        out: &mut String,
    ) {
        let node_id = next_node_id(node_id_counter);
        // Emit the node heading line first so child metadata stays visually scoped.
        let node_indent = "  ".repeat(depth);
        let field_indent = "  ".repeat(depth.saturating_add(1));
        push_rendered_line_prefix_with_indent(out, &node_indent);
        let _ = write!(
            out,
            "{} execution_mode={}",
            self.node_type.as_str(),
            execution_mode_label(self.execution_mode)
        );
        push_rendered_line_prefix_with_indent(out, &field_indent);
        let _ = write!(out, "node_id={node_id}");
        push_rendered_line_prefix_with_indent(out, &field_indent);
        let _ = write!(out, "layer={}", self.node_type.layer_label());
        push_rendered_line_prefix_with_indent(out, &field_indent);
        let _ = write!(
            out,
            "execution_mode_detail={}",
            execution_mode_detail_label(self.execution_mode)
        );
        push_rendered_line_prefix_with_indent(out, &field_indent);
        let _ = write!(
            out,
            "predicate_pushdown_mode={}",
            predicate_pushdown_mode(self)
        );
        if let Some(fast_path_selected) = fast_path_selected(self) {
            push_rendered_line_prefix_with_indent(out, &field_indent);
            let _ = write!(out, "fast_path_selected={fast_path_selected}");
        }
        if let Some(fast_path_reason) = fast_path_reason(self) {
            push_rendered_line_prefix_with_indent(out, &field_indent);
            let _ = write!(out, "fast_path_reason={fast_path_reason}");
        }

        // Emit all optional node-local fields in a deterministic order.
        self.render_text_tree_verbose_node_fields(&field_indent, out);

        // Recurse in execution order to preserve stable tree topology.
        for child in &self.children {
            child.render_text_tree_verbose_into(depth.saturating_add(1), node_id_counter, out);
        }
    }

    fn render_text_tree_verbose_node_fields(&self, field_indent: &str, out: &mut String) {
        if let Some(access_strategy) = self.access_strategy.as_ref() {
            push_rendered_line_prefix_with_indent(out, field_indent);
            let _ = write!(out, "access_strategy={access_strategy:?}");
        }
        if let Some(predicate_pushdown) = self.predicate_pushdown.as_ref() {
            push_rendered_line_prefix_with_indent(out, field_indent);
            let _ = write!(out, "predicate_pushdown={predicate_pushdown}");
        }
        if let Some(residual_predicate) = self.residual_predicate.as_ref() {
            push_rendered_line_prefix_with_indent(out, field_indent);
            let _ = write!(out, "residual_predicate={residual_predicate:?}");
        }
        if let Some(projection) = self.projection.as_ref() {
            push_rendered_line_prefix_with_indent(out, field_indent);
            let _ = write!(out, "projection={projection}");
        }
        if let Some(ordering_source) = self.ordering_source {
            push_rendered_line_prefix_with_indent(out, field_indent);
            let _ = write!(
                out,
                "ordering_source={}",
                ordering_source_label(ordering_source)
            );
        }
        if let Some(limit) = self.limit {
            push_rendered_line_prefix_with_indent(out, field_indent);
            let _ = write!(out, "limit={limit}");
        }
        if let Some(cursor) = self.cursor {
            push_rendered_line_prefix_with_indent(out, field_indent);
            let _ = write!(out, "cursor={cursor}");
        }
        if let Some(covering_scan) = self.covering_scan {
            push_rendered_line_prefix_with_indent(out, field_indent);
            let _ = write!(out, "covering_scan={covering_scan}");
        }
        if let Some(rows_expected) = self.rows_expected {
            push_rendered_line_prefix_with_indent(out, field_indent);
            let _ = write!(out, "rows_expected={rows_expected}");
        }
        if !self.node_properties.is_empty() {
            push_rendered_line_prefix_with_indent(out, field_indent);
            out.push_str("node_properties=");
            write_node_properties(out, &self.node_properties);
        }
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

fn push_rendered_line_prefix_with_indent(out: &mut String, indent: &str) {
    if !out.is_empty() {
        out.push('\n');
    }
    out.push_str(indent);
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

const fn next_node_id(node_id_counter: &mut u64) -> u64 {
    let node_id = *node_id_counter;
    *node_id_counter = node_id_counter.saturating_add(1);
    node_id
}
