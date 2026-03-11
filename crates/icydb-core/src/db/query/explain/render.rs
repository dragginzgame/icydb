//! Module: query::explain::render
//! Responsibility: text/json rendering for explain execution descriptors.
//! Does not own: planner or executor decision derivation.
//! Boundary: consumes explain projection types and emits deterministic render output.

use crate::{
    db::query::explain::{
        ExplainExecutionNodeDescriptor,
        access_projection::{access_strategy_label, write_access_json},
        execution::{execution_mode_label, ordering_source_label},
        writer::JsonWriter,
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

    /// Render this execution subtree as canonical JSON.
    #[must_use]
    pub fn render_json_canonical(&self) -> String {
        let mut out = String::new();
        let mut node_id_counter = 0_u64;
        write_execution_node_json(self, &mut node_id_counter, &mut out);
        out
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
            let _ = write!(line, " access={}", access_strategy_label(access_strategy));
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

fn render_node_properties(node_properties: &BTreeMap<String, Value>) -> String {
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

fn write_execution_node_json(
    node: &ExplainExecutionNodeDescriptor,
    node_id_counter: &mut u64,
    out: &mut String,
) {
    let node_id = next_node_id(node_id_counter);
    let mut object = JsonWriter::begin_object(out);

    object.field_u64("node_id", node_id);
    object.field_str("node_type", node.node_type.as_str());
    object.field_str("execution_mode", execution_mode_label(node.execution_mode));
    object.field_str(
        "execution_mode_detail",
        execution_mode_detail_label(node.execution_mode),
    );
    object.field_with("access_strategy", |out| {
        match node.access_strategy.as_ref() {
            Some(access) => write_access_json(access, out),
            None => out.push_str("null"),
        }
    });
    object.field_str("predicate_pushdown_mode", predicate_pushdown_mode(node));
    match node.predicate_pushdown.as_deref() {
        Some(predicate_pushdown) => object.field_str("predicate_pushdown", predicate_pushdown),
        None => object.field_null("predicate_pushdown"),
    }
    match fast_path_selected(node) {
        Some(selected) => object.field_bool("fast_path_selected", selected),
        None => object.field_null("fast_path_selected"),
    }
    match fast_path_reason(node) {
        Some(reason) => object.field_str("fast_path_reason", reason),
        None => object.field_null("fast_path_reason"),
    }
    match node.residual_predicate.as_ref() {
        Some(residual_predicate) => {
            object.field_value_debug("residual_predicate", residual_predicate);
        }
        None => object.field_null("residual_predicate"),
    }
    match node.projection.as_deref() {
        Some(projection) => object.field_str("projection", projection),
        None => object.field_null("projection"),
    }
    match node.ordering_source {
        Some(ordering_source) => {
            object.field_str("ordering_source", ordering_source_label(ordering_source));
        }
        None => object.field_null("ordering_source"),
    }
    match node.limit {
        Some(limit) => object.field_u64("limit", u64::from(limit)),
        None => object.field_null("limit"),
    }
    match node.cursor {
        Some(cursor) => object.field_bool("cursor", cursor),
        None => object.field_null("cursor"),
    }
    match node.covering_scan {
        Some(covering_scan) => object.field_bool("covering_scan", covering_scan),
        None => object.field_null("covering_scan"),
    }
    match node.rows_expected {
        Some(rows_expected) => object.field_u64("rows_expected", rows_expected),
        None => object.field_null("rows_expected"),
    }
    object.field_with("children", |out| {
        out.push('[');
        for (index, child) in node.children.iter().enumerate() {
            if index > 0 {
                out.push(',');
            }
            write_execution_node_json(child, node_id_counter, out);
        }
        out.push(']');
    });
    object.field_debug_map("node_properties", &node.node_properties);

    object.finish();
}

const fn next_node_id(node_id_counter: &mut u64) -> u64 {
    let node_id = *node_id_counter;
    *node_id_counter = node_id_counter.saturating_add(1);
    node_id
}

const fn execution_mode_detail_label(
    mode: crate::db::query::explain::ExplainExecutionMode,
) -> &'static str {
    match mode {
        crate::db::query::explain::ExplainExecutionMode::Streaming => "streaming",
        crate::db::query::explain::ExplainExecutionMode::Materialized => "materialized",
    }
}

fn predicate_pushdown_mode(node: &ExplainExecutionNodeDescriptor) -> &'static str {
    match node.predicate_pushdown.as_deref() {
        None => "none",
        Some("strict_all_or_none") => "full",
        Some(_) => {
            if node.residual_predicate.is_some() {
                "partial"
            } else {
                "full"
            }
        }
    }
}

fn fast_path_selected(node: &ExplainExecutionNodeDescriptor) -> Option<bool> {
    let selected = node.node_properties.get("fast_path_selected")?;
    match selected {
        Value::Text(path) => Some(path.as_str() != "none"),
        _ => None,
    }
}

fn fast_path_reason(node: &ExplainExecutionNodeDescriptor) -> Option<&str> {
    let reason = node.node_properties.get("fast_path_selected_reason")?;
    match reason {
        Value::Text(reason) => Some(reason.as_str()),
        _ => None,
    }
}
