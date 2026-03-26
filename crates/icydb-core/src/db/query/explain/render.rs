//! Module: query::explain::render
//! Responsibility: text/json rendering for explain execution descriptors.
//! Does not own: planner or executor decision derivation.
//! Boundary: consumes explain projection types and emits deterministic render output.

use crate::{
    db::query::explain::{
        ExplainExecutionNodeDescriptor,
        access_projection::write_access_strategy_label,
        execution::{execution_mode_label, ordering_source_label},
        nodes::{
            execution_mode_detail_label, fast_path_reason, fast_path_selected,
            predicate_pushdown_mode,
        },
    },
    value::Value,
};
use std::{collections::BTreeMap, fmt::Write};

impl ExplainExecutionNodeDescriptor {
    /// Render this execution subtree as a compact text tree.
    #[must_use]
    pub fn render_text_tree(&self) -> String {
        let mut lines = Vec::new();
        let mut node_id_counter = 0_u64;
        self.render_text_tree_into(0, &mut node_id_counter, &mut lines);
        lines.join("\n")
    }

    /// Render this execution subtree as a verbose text tree with properties.
    #[must_use]
    pub fn render_text_tree_verbose(&self) -> String {
        let mut lines = Vec::new();
        let mut node_id_counter = 0_u64;
        self.render_text_tree_verbose_into(0, &mut node_id_counter, &mut lines);
        lines.join("\n")
    }

    fn render_text_tree_into(
        &self,
        depth: usize,
        node_id_counter: &mut u64,
        lines: &mut Vec<String>,
    ) {
        let node_id = next_node_id(node_id_counter);
        let mut line = format!(
            "{}{} execution_mode={}",
            "  ".repeat(depth),
            self.node_type.as_str(),
            execution_mode_label(self.execution_mode)
        );
        let _ = write!(line, " node_id={node_id}");
        let _ = write!(line, " layer={}", self.node_type.layer_label());
        let _ = write!(
            line,
            " execution_mode_detail={}",
            execution_mode_detail_label(self.execution_mode)
        );
        let _ = write!(
            line,
            " predicate_pushdown_mode={}",
            predicate_pushdown_mode(self)
        );
        if let Some(fast_path_selected) = fast_path_selected(self) {
            let _ = write!(line, " fast_path_selected={fast_path_selected}");
        }
        if let Some(fast_path_reason) = fast_path_reason(self) {
            let _ = write!(line, " fast_path_reason={fast_path_reason}");
        }

        if let Some(access_strategy) = self.access_strategy.as_ref() {
            line.push_str(" access=");
            write_access_strategy_label(&mut line, access_strategy);
        }
        if let Some(predicate_pushdown) = self.predicate_pushdown.as_ref() {
            let _ = write!(line, " predicate_pushdown={predicate_pushdown}");
        }
        if let Some(residual_predicate) = self.residual_predicate.as_ref() {
            let _ = write!(line, " residual_predicate={residual_predicate:?}");
        }
        if let Some(projection) = self.projection.as_ref() {
            let _ = write!(line, " projection={projection}");
        }
        if let Some(ordering_source) = self.ordering_source {
            let _ = write!(
                line,
                " ordering_source={}",
                ordering_source_label(ordering_source)
            );
        }
        if let Some(limit) = self.limit {
            let _ = write!(line, " limit={limit}");
        }
        if let Some(cursor) = self.cursor {
            let _ = write!(line, " cursor={cursor}");
        }
        if let Some(covering_scan) = self.covering_scan {
            let _ = write!(line, " covering_scan={covering_scan}");
        }
        if let Some(rows_expected) = self.rows_expected {
            let _ = write!(line, " rows_expected={rows_expected}");
        }
        if !self.node_properties.is_empty() {
            let _ = write!(
                line,
                " node_properties={}",
                render_node_properties(&self.node_properties)
            );
        }

        lines.push(line);

        for child in &self.children {
            child.render_text_tree_into(depth.saturating_add(1), node_id_counter, lines);
        }
    }

    fn render_text_tree_verbose_into(
        &self,
        depth: usize,
        node_id_counter: &mut u64,
        lines: &mut Vec<String>,
    ) {
        let node_id = next_node_id(node_id_counter);
        // Emit the node heading line first so child metadata stays visually scoped.
        let node_indent = "  ".repeat(depth);
        let field_indent = "  ".repeat(depth.saturating_add(1));
        lines.push(format!(
            "{}{} execution_mode={}",
            node_indent,
            self.node_type.as_str(),
            execution_mode_label(self.execution_mode)
        ));
        lines.push(format!("{field_indent}node_id={node_id}"));
        lines.push(format!(
            "{}layer={}",
            field_indent,
            self.node_type.layer_label()
        ));
        lines.push(format!(
            "{}execution_mode_detail={}",
            field_indent,
            execution_mode_detail_label(self.execution_mode)
        ));
        lines.push(format!(
            "{}predicate_pushdown_mode={}",
            field_indent,
            predicate_pushdown_mode(self)
        ));
        if let Some(fast_path_selected) = fast_path_selected(self) {
            lines.push(format!(
                "{field_indent}fast_path_selected={fast_path_selected}"
            ));
        }
        if let Some(fast_path_reason) = fast_path_reason(self) {
            lines.push(format!("{field_indent}fast_path_reason={fast_path_reason}"));
        }

        // Emit all optional node-local fields in a deterministic order.
        if let Some(access_strategy) = self.access_strategy.as_ref() {
            lines.push(format!("{field_indent}access_strategy={access_strategy:?}"));
        }
        if let Some(predicate_pushdown) = self.predicate_pushdown.as_ref() {
            lines.push(format!(
                "{field_indent}predicate_pushdown={predicate_pushdown}"
            ));
        }
        if let Some(residual_predicate) = self.residual_predicate.as_ref() {
            lines.push(format!(
                "{field_indent}residual_predicate={residual_predicate:?}"
            ));
        }
        if let Some(projection) = self.projection.as_ref() {
            lines.push(format!("{field_indent}projection={projection}"));
        }
        if let Some(ordering_source) = self.ordering_source {
            lines.push(format!(
                "{}ordering_source={}",
                field_indent,
                ordering_source_label(ordering_source)
            ));
        }
        if let Some(limit) = self.limit {
            lines.push(format!("{field_indent}limit={limit}"));
        }
        if let Some(cursor) = self.cursor {
            lines.push(format!("{field_indent}cursor={cursor}"));
        }
        if let Some(covering_scan) = self.covering_scan {
            lines.push(format!("{field_indent}covering_scan={covering_scan}"));
        }
        if let Some(rows_expected) = self.rows_expected {
            lines.push(format!("{field_indent}rows_expected={rows_expected}"));
        }
        if !self.node_properties.is_empty() {
            lines.push(format!(
                "{}node_properties={}",
                field_indent,
                render_node_properties(&self.node_properties)
            ));
        }

        // Recurse in execution order to preserve stable tree topology.
        for child in &self.children {
            child.render_text_tree_verbose_into(depth.saturating_add(1), node_id_counter, lines);
        }
    }
}

fn render_node_properties(node_properties: &BTreeMap<&'static str, Value>) -> String {
    let mut rendered = String::new();
    let mut first = true;
    for (key, value) in node_properties {
        if first {
            first = false;
        } else {
            rendered.push(',');
        }
        let _ = write!(rendered, "{key}={value:?}");
    }
    rendered
}

const fn next_node_id(node_id_counter: &mut u64) -> u64 {
    let node_id = *node_id_counter;
    *node_id_counter = node_id_counter.saturating_add(1);
    node_id
}
