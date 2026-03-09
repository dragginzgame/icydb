//! Module: query::plan::order_contract
//! Responsibility: planner-owned execution ordering contracts and direction normalization.
//! Does not own: runtime order application mechanics or cursor wire token encoding.
//! Boundary: exposes immutable order contracts consumed across planner/executor boundaries.

use crate::db::{
    direction::Direction,
    query::plan::{OrderDirection, OrderSpec},
};

///
/// ExecutionOrdering
///
/// Planner-owned execution ordering selection.
/// Keeps scalar and grouped ordering contracts explicit at one boundary.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) enum ExecutionOrdering {
    PrimaryKey,
    Explicit(OrderSpec),
    Grouped(Option<OrderSpec>),
}

///
/// ExecutionOrderContract
///
/// Immutable planner-projected execution ordering contract.
/// Encodes ordering shape, canonical traversal direction, and cursor support.
///
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct ExecutionOrderContract {
    ordering: ExecutionOrdering,
    direction: Direction,
    supports_cursor: bool,
}

impl ExecutionOrderContract {
    /// Build one execution ordering contract from grouped/order plan shape.
    #[must_use]
    pub(in crate::db) fn from_plan(is_grouped: bool, order: Option<&OrderSpec>) -> Self {
        let direction = primary_scan_direction(order);

        if is_grouped {
            return Self {
                ordering: ExecutionOrdering::Grouped(order.cloned()),
                direction,
                supports_cursor: true,
            };
        }

        match order.cloned() {
            Some(order) => Self {
                ordering: ExecutionOrdering::Explicit(order),
                direction,
                supports_cursor: true,
            },
            None => Self {
                ordering: ExecutionOrdering::PrimaryKey,
                direction,
                supports_cursor: false,
            },
        }
    }

    #[must_use]
    pub(in crate::db) const fn ordering(&self) -> &ExecutionOrdering {
        &self.ordering
    }

    #[must_use]
    pub(in crate::db) const fn direction(&self) -> Direction {
        self.direction
    }

    /// Return canonical primary scan direction for this execution contract.
    #[must_use]
    pub(in crate::db) const fn primary_scan_direction(&self) -> Direction {
        self.direction
    }

    /// Return canonical secondary-index scan direction for this contract.
    #[must_use]
    #[expect(dead_code)]
    pub(in crate::db) fn secondary_scan_direction(&self) -> Direction {
        secondary_scan_direction(self.order_spec())
    }

    #[must_use]
    pub(in crate::db) const fn supports_cursor(&self) -> bool {
        self.supports_cursor
    }

    #[must_use]
    pub(in crate::db) const fn is_grouped(&self) -> bool {
        matches!(&self.ordering, ExecutionOrdering::Grouped(_))
    }

    #[must_use]
    pub(in crate::db) const fn order_spec(&self) -> Option<&OrderSpec> {
        match &self.ordering {
            ExecutionOrdering::PrimaryKey => None,
            ExecutionOrdering::Explicit(order) => Some(order),
            ExecutionOrdering::Grouped(order) => order.as_ref(),
        }
    }
}

fn primary_scan_direction(order: Option<&OrderSpec>) -> Direction {
    let Some(order) = order else {
        return Direction::Asc;
    };
    let Some((_, direction)) = order.fields.first() else {
        return Direction::Asc;
    };

    match direction {
        OrderDirection::Asc => Direction::Asc,
        OrderDirection::Desc => Direction::Desc,
    }
}

fn secondary_scan_direction(order: Option<&OrderSpec>) -> Direction {
    let Some(order) = order else {
        return Direction::Asc;
    };
    let Some((_, direction)) = order.fields.last() else {
        return Direction::Asc;
    };

    match direction {
        OrderDirection::Asc => Direction::Asc,
        OrderDirection::Desc => Direction::Desc,
    }
}
