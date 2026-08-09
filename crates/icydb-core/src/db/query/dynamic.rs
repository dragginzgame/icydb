//! Module: db::query::dynamic
//! Responsibility: entity-name-driven structural read requests and results.
//! Does not own: accepted schema resolution, planning, or execution.
//! Boundary: public dynamic inputs are lowered once against accepted authority.

use crate::db::query::{
    builder::AggregateExpr,
    expr::{FilterExpr, OrderTerm},
};

///
/// DynamicQuery
///
/// Entity-name-driven structural read request.
/// The session resolves fields, ordering, indexes, and projection against the
/// accepted schema; no generated entity descriptor participates.
///

#[derive(Clone, Debug)]
pub struct DynamicQuery {
    entity: String,
    filter: Option<FilterExpr>,
    order: Vec<OrderTerm>,
    fields: Vec<String>,
    #[cfg(test)]
    distinct: bool,
    limit: Option<u32>,
    group_fields: Vec<String>,
    aggregates: Vec<AggregateExpr>,
    grouped_limits: Option<(u32, u32)>,
    cursor: Option<String>,
}

impl DynamicQuery {
    /// Start one dynamic read for an accepted entity name.
    #[must_use]
    pub fn new(entity: impl Into<String>) -> Self {
        Self {
            entity: entity.into(),
            filter: None,
            order: Vec::new(),
            fields: Vec::new(),
            #[cfg(test)]
            distinct: false,
            limit: None,
            group_fields: Vec::new(),
            aggregates: Vec::new(),
            grouped_limits: None,
            cursor: None,
        }
    }

    /// Add one filter expression.
    #[must_use]
    pub fn filter(mut self, filter: impl Into<FilterExpr>) -> Self {
        self.filter = Some(filter.into());
        self
    }

    /// Append one deterministic ordering term.
    #[must_use]
    pub fn order_by(mut self, order: OrderTerm) -> Self {
        self.order.push(order);
        self
    }

    /// Select explicit fields in scalar output order.
    ///
    /// Grouped execution rejects an explicit scalar selection because group
    /// keys and aggregates define its output contract.
    #[must_use]
    pub fn select<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.fields = fields.into_iter().map(Into::into).collect();
        self
    }

    /// Limit the number of returned rows.
    #[must_use]
    pub const fn limit(mut self, limit: u32) -> Self {
        self.limit = Some(limit);
        self
    }

    /// Enable projection DISTINCT for maintained internal execution callers.
    ///
    /// The public dynamic-query grammar deliberately does not expose this
    /// builder; SQL and internal executor contracts remain the DISTINCT
    /// frontends until a separately reviewed public API is designed.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn distinct_for_internal_execution(mut self) -> Self {
        self.distinct = true;
        self
    }

    /// Append one accepted field to the grouped key in declaration order.
    #[must_use]
    pub fn group_by(mut self, field: impl Into<String>) -> Self {
        self.group_fields.push(field.into());
        self
    }

    /// Append one grouped aggregate in declaration order.
    #[must_use]
    pub fn aggregate(mut self, aggregate: AggregateExpr) -> Self {
        self.aggregates.push(aggregate);
        self
    }

    /// Set explicit hard limits for grouped execution.
    ///
    /// Ordinary public reads additionally enforce their built-in admission
    /// ceilings. Zero values are rejected before execution.
    #[must_use]
    pub const fn grouped_limits(mut self, max_groups: u32, max_group_bytes: u32) -> Self {
        self.grouped_limits = Some((max_groups, max_group_bytes));
        self
    }

    /// Continue a grouped page from one opaque cursor returned by IcyDB.
    #[must_use]
    pub fn cursor(mut self, cursor: impl Into<String>) -> Self {
        self.cursor = Some(cursor.into());
        self
    }

    pub(in crate::db) const fn entity(&self) -> &str {
        self.entity.as_str()
    }

    pub(in crate::db) const fn filter_expr(&self) -> Option<&FilterExpr> {
        self.filter.as_ref()
    }

    pub(in crate::db) const fn order_terms(&self) -> &[OrderTerm] {
        self.order.as_slice()
    }

    pub(in crate::db) const fn selected_fields(&self) -> &[String] {
        self.fields.as_slice()
    }

    pub(in crate::db) const fn row_limit(&self) -> Option<u32> {
        self.limit
    }

    #[cfg(test)]
    pub(in crate::db) const fn projection_is_distinct(&self) -> bool {
        self.distinct
    }

    pub(in crate::db) const fn has_grouping(&self) -> bool {
        !self.group_fields.is_empty() || !self.aggregates.is_empty()
    }

    pub(in crate::db) const fn group_fields(&self) -> &[String] {
        self.group_fields.as_slice()
    }

    pub(in crate::db) const fn aggregates(&self) -> &[AggregateExpr] {
        self.aggregates.as_slice()
    }

    pub(in crate::db) const fn grouped_execution_limits(&self) -> Option<(u32, u32)> {
        self.grouped_limits
    }

    pub(in crate::db) fn continuation_cursor(&self) -> Option<&str> {
        self.cursor.as_deref()
    }
}
