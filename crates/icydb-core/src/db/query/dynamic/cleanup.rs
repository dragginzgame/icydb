//! Detach recursive children before dropping an owned dynamic request.
//! Cleanup visits the already-owned input, even when admission rejected early.

use crate::db::query::{
    DynamicQuery,
    expr::{FilterExpr, FilterValue},
};

pub(super) fn clear(query: &mut DynamicQuery) {
    if let Some(filter) = query.filter.take() {
        clear_filter(filter);
    }
}

fn clear_filter(filter: FilterExpr) {
    let mut pending = Vec::new();
    let mut current = Some(filter);
    while let Some(filter) = current.take().or_else(|| pending.pop()) {
        match filter {
            FilterExpr::Junction { filters, .. } => pending.extend(filters),
            FilterExpr::Not(filter) => current = Some(*filter),
            FilterExpr::Compare { value, .. } | FilterExpr::Collection { value, .. } => {
                clear_filter_value(value);
            }
            FilterExpr::Set { values, .. } => {
                for value in values {
                    clear_filter_value(value);
                }
            }
            FilterExpr::Constant(_)
            | FilterExpr::CompareFields { .. }
            | FilterExpr::State { .. } => {}
        }
    }
}

fn clear_filter_value(value: FilterValue) {
    let mut pending = Vec::new();
    let mut current = Some(value);
    while let Some(value) = current.take().or_else(|| pending.pop()) {
        if let FilterValue::List(values) = value {
            pending.extend(values);
            current = pending.pop();
        }
    }
}
