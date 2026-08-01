//! Module: db::query::dynamic
//! Responsibility: entity-name-driven structural read requests and results.
//! Does not own: accepted schema resolution, planning, or execution.
//! Boundary: public dynamic inputs are lowered once against accepted authority.

use crate::db::query::expr::{FilterExpr, OrderTerm};

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
    limit: Option<u32>,
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
            limit: None,
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

    /// Select explicit fields in output order.
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
}
