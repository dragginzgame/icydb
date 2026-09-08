//! Borrowed, schema-independent input limits before query preparation.
//! This is not retained-cache accounting or a bound on planner-produced work.

#[cfg(test)]
mod tests;

use crate::{
    db::query::{
        DynamicQuery,
        builder::AggregateExpr,
        expr::{FilterExpr, FilterValue},
        plan::expr::Expr,
    },
    value::Value,
};
use icydb_diagnostic_code::QueryReadAdmissionCode;

pub(in crate::db) const MAX_QUERY_INPUT_DEPTH: usize = 128;
pub(in crate::db) const MAX_QUERY_INPUT_NODES: usize = 4_096;
pub(in crate::db) const MAX_QUERY_INPUT_BYTES: usize = 2 * 1024 * 1024;

/// One content budget shared by every authored component of a request.
pub(in crate::db) struct QueryInputBudget {
    nodes: usize,
    bytes: usize,
}

impl QueryInputBudget {
    pub(in crate::db) const fn new() -> Self {
        Self {
            nodes: MAX_QUERY_INPUT_NODES,
            bytes: MAX_QUERY_INPUT_BYTES,
        }
    }

    // Check before descent; sibling iteration never builds an unbounded frontier.
    pub(in crate::db) fn node(&mut self, depth: usize) -> Result<(), QueryReadAdmissionCode> {
        if depth > MAX_QUERY_INPUT_DEPTH {
            return Err(QueryReadAdmissionCode::InputDepthExceeded);
        }
        self.nodes = self
            .nodes
            .checked_sub(1)
            .ok_or(QueryReadAdmissionCode::InputNodesExceeded)?;
        Ok(())
    }

    pub(in crate::db) fn payload(&mut self, bytes: usize) -> Result<(), QueryReadAdmissionCode> {
        self.bytes = self
            .bytes
            .checked_sub(bytes)
            .ok_or(QueryReadAdmissionCode::InputBytesExceeded)?;
        Ok(())
    }

    pub(in crate::db) fn name(
        &mut self,
        name: &str,
        depth: usize,
    ) -> Result<(), QueryReadAdmissionCode> {
        self.node(depth)?;
        self.payload(name.len())
    }

    fn filter(&mut self, filter: &FilterExpr, depth: usize) -> Result<(), QueryReadAdmissionCode> {
        self.node(depth)?;
        match filter {
            FilterExpr::Constant(_) => Ok(()),
            FilterExpr::Junction { filters, .. } => {
                for filter in filters {
                    self.filter(filter, depth + 1)?;
                }
                Ok(())
            }
            FilterExpr::Not(filter) => self.filter(filter, depth + 1),
            FilterExpr::Compare { field, value, .. }
            | FilterExpr::Collection { field, value, .. } => {
                self.name(field, depth + 1)?;
                self.filter_value(value, depth + 1)
            }
            FilterExpr::CompareFields {
                left_field,
                right_field,
                ..
            } => {
                self.name(left_field, depth + 1)?;
                self.name(right_field, depth + 1)
            }
            FilterExpr::Set { field, values, .. } => {
                self.name(field, depth + 1)?;
                for value in values {
                    self.filter_value(value, depth + 1)?;
                }
                Ok(())
            }
            FilterExpr::State { field, .. } => self.name(field, depth + 1),
        }
    }

    fn filter_value(
        &mut self,
        value: &FilterValue,
        depth: usize,
    ) -> Result<(), QueryReadAdmissionCode> {
        self.node(depth)?;
        match value {
            FilterValue::String(text) => self.payload(text.len()),
            FilterValue::List(values) => {
                for value in values {
                    self.filter_value(value, depth + 1)?;
                }
                Ok(())
            }
            FilterValue::Bool(_) | FilterValue::Null => Ok(()),
        }
    }

    fn aggregate_children(
        &mut self,
        aggregate: &AggregateExpr,
        depth: usize,
    ) -> Result<(), QueryReadAdmissionCode> {
        if let Some(input) = aggregate.input_expr() {
            self.expr(input, depth)?;
        }
        if let Some(filter) = aggregate.filter_expr() {
            self.expr(filter, depth)?;
        }
        Ok(())
    }

    pub(in crate::db) fn expr(
        &mut self,
        expr: &Expr,
        depth: usize,
    ) -> Result<(), QueryReadAdmissionCode> {
        self.node(depth)?;
        match expr {
            Expr::Field(field) => self.payload(field.as_str().len()),
            Expr::FieldPath(path) => {
                self.payload(path.root().as_str().len())?;
                for segment in path.segments() {
                    self.name(segment, depth + 1)?;
                }
                Ok(())
            }
            Expr::Literal(value) => self.value(value, depth + 1),
            Expr::FunctionCall { args, .. } => {
                for arg in args {
                    self.expr(arg, depth + 1)?;
                }
                Ok(())
            }
            Expr::Unary { expr, .. } => self.expr(expr, depth + 1),
            Expr::Binary { left, right, .. } => {
                self.expr(left, depth + 1)?;
                self.expr(right, depth + 1)
            }
            Expr::Case {
                when_then_arms,
                else_expr,
            } => {
                for arm in when_then_arms {
                    self.expr(arm.condition(), depth + 1)?;
                    self.expr(arm.result(), depth + 1)?;
                }
                self.expr(else_expr, depth + 1)
            }
            Expr::Aggregate(aggregate) => self.aggregate_children(aggregate, depth + 1),
            #[cfg(test)]
            Expr::Alias { expr, name } => {
                self.payload(name.as_str().len())?;
                self.expr(expr, depth + 1)
            }
        }
    }

    pub(in crate::db) fn value(
        &mut self,
        value: &Value,
        depth: usize,
    ) -> Result<(), QueryReadAdmissionCode> {
        self.node(depth)?;
        match value {
            Value::Text(text) => self.payload(text.len()),
            Value::Blob(blob) => self.payload(blob.len()),
            Value::IntBig(value) => self.big_payload(value.magnitude_bits()),
            Value::NatBig(value) => self.big_payload(value.magnitude_bits()),
            Value::List(values) => {
                for value in values {
                    self.value(value, depth + 1)?;
                }
                Ok(())
            }
            Value::Map(entries) => {
                for (key, value) in entries {
                    self.value(key, depth + 1)?;
                    self.value(value, depth + 1)?;
                }
                Ok(())
            }
            Value::Enum(value) => {
                if let Some(payload) = value.payload() {
                    self.value(payload, depth + 1)?;
                }
                Ok(())
            }
            Value::Account(_)
            | Value::Bool(_)
            | Value::Date(_)
            | Value::Decimal(_)
            | Value::Duration(_)
            | Value::Float32(_)
            | Value::Float64(_)
            | Value::Int64(_)
            | Value::Int128(_)
            | Value::Nat64(_)
            | Value::Nat128(_)
            | Value::Null
            | Value::Principal(_)
            | Value::Subaccount(_)
            | Value::Timestamp(_)
            | Value::U256(_)
            | Value::Ulid(_)
            | Value::Unit => Ok(()),
        }
    }

    fn big_payload(&mut self, bits: u64) -> Result<(), QueryReadAdmissionCode> {
        self.payload(
            usize::try_from(bits.div_ceil(8))
                .map_err(|_| QueryReadAdmissionCode::InputBytesExceeded)?,
        )
    }
}

pub(in crate::db) fn validate_dynamic_query_input(
    request: &DynamicQuery,
) -> Result<(), QueryReadAdmissionCode> {
    let mut budget = QueryInputBudget::new();
    budget.name(request.entity(), 1)?;
    if let Some(filter) = request.filter_expr() {
        budget.filter(filter, 1)?;
    }
    for order in request.order_terms() {
        budget.expr(order.expression(), 1)?;
    }
    for field in request
        .selected_fields()
        .iter()
        .chain(request.group_fields())
    {
        budget.name(field, 1)?;
    }
    for aggregate in request.aggregates() {
        budget.node(1)?;
        budget.aggregate_children(aggregate, 2)?;
    }
    if let Some(cursor) = request.continuation_cursor() {
        budget.name(cursor, 1)?;
    }
    Ok(())
}
