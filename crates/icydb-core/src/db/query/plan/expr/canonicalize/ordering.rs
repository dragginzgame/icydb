//! Abortable canonical ordering. Successful order remains label, then Debug;
//! resource rejection never substitutes an ordering result or a key.

#[cfg(test)]
mod tests;

use std::{
    cmp::Ordering,
    fmt::{self, Write},
};

use crate::db::{
    QueryError,
    query::{
        builder::scalar_projection::write_scalar_projection_expr_plan_label, plan::expr::Expr,
        preparation::PreparationWork,
    },
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

struct OrderingText<'a, 'scope> {
    work: &'a PreparationWork<'scope>,
    text: String,
    error: Option<QueryError>,
}

impl<'a, 'scope> OrderingText<'a, 'scope> {
    fn render(
        work: &'a PreparationWork<'scope>,
        render: impl FnOnce(&mut Self) -> fmt::Result,
    ) -> Result<String, QueryError> {
        let mut output = Self {
            work,
            text: String::new(),
            error: None,
        };
        let result = render(&mut output);
        if let Some(error) = output.error {
            return Err(error);
        }
        result.map_err(|_| QueryError::invariant())?;
        Ok(output.text)
    }
}

impl Write for OrderingText<'_, '_> {
    fn write_str(&mut self, text: &str) -> fmt::Result {
        if self.error.is_some() {
            return Err(fmt::Error);
        }
        let admission = self
            .work
            .charge(Resource::PredicateExpressionSteps, text.len() as u64)
            .and_then(|()| self.work.reserve_string(&mut self.text, text.len()));
        if let Err(error) = admission {
            self.error = Some(error);
            return Err(fmt::Error);
        }
        self.text.push_str(text);
        Ok(())
    }
}

// Keys live only for this sort. Render each label once, and each typed Debug
// key only if a label ties; no operand is cloned or retained by another owner.
struct OrderingKey<'a> {
    expr: &'a Expr,
    label: String,
    debug: Option<String>,
}

impl<'a> OrderingKey<'a> {
    fn new(expr: &'a Expr, work: &PreparationWork<'_>) -> Result<Self, QueryError> {
        Ok(Self {
            expr,
            label: OrderingText::render(work, |out| {
                write_scalar_projection_expr_plan_label(expr, out)
            })?,
            debug: None,
        })
    }

    fn prepare_debug(&mut self, work: &PreparationWork<'_>) -> Result<(), QueryError> {
        if self.debug.is_none() {
            let expr = self.expr;
            self.debug = Some(OrderingText::render(work, |out| write!(out, "{expr:?}"))?);
        }
        Ok(())
    }
}

fn compare_text(
    left: &str,
    right: &str,
    work: &PreparationWork<'_>,
) -> Result<Ordering, QueryError> {
    // Bound byte comparison before entering the standard string comparator.
    // The shorter byte length is a conservative bound, not a node count.
    work.charge(
        Resource::PredicateExpressionSteps,
        left.len().min(right.len()) as u64,
    )?;
    Ok(left.cmp(right))
}

fn compare(
    keys: &mut [OrderingKey<'_>],
    left: usize,
    right: usize,
    work: &PreparationWork<'_>,
) -> Result<Ordering, QueryError> {
    work.charge(Resource::SortComparisons, 1)?;
    let order = compare_text(&keys[left].label, &keys[right].label, work)?;
    if order != Ordering::Equal {
        return Ok(order);
    }
    keys[left].prepare_debug(work)?;
    keys[right].prepare_debug(work)?;
    match (&keys[left].debug, &keys[right].debug) {
        (Some(left), Some(right)) => compare_text(left, right, work),
        _ => Err(QueryError::invariant()),
    }
}

/// Stable bottom-up merge of indices permits comparator failure without
/// cloning operands or exposing a partially reordered expression. The final
/// permutation runs only after every comparison succeeds.
pub(super) fn sort_bool_children(
    children: &mut [Expr],
    work: &PreparationWork<'_>,
) -> Result<(), QueryError> {
    let len = children.len();
    if len < 2 {
        return Ok(());
    }
    let mut keys = Vec::new();
    work.reserve_vec(&mut keys, len)?;
    for child in children.iter() {
        keys.push(OrderingKey::new(child, work)?);
    }
    let mut order = Vec::new();
    let mut scratch = Vec::new();
    work.reserve_vec(&mut order, len)?;
    work.reserve_vec(&mut scratch, len)?;
    order.extend(0..len);
    scratch.resize(len, 0);
    let mut width = 1;
    while width < len {
        for start in (0..len).step_by(width.saturating_mul(2)) {
            let middle = start.saturating_add(width).min(len);
            let end = middle.saturating_add(width).min(len);
            let (mut left, mut right) = (start, middle);
            for destination in &mut scratch[start..end] {
                work.charge(Resource::PredicateExpressionSteps, 1)?;
                let take_left = right == end
                    || (left < middle
                        && compare(&mut keys, order[left], order[right], work)?.is_le());
                if take_left {
                    *destination = order[left];
                    left += 1;
                } else {
                    *destination = order[right];
                    right += 1;
                }
            }
        }
        std::mem::swap(&mut order, &mut scratch);
        width = width.saturating_mul(2);
    }
    drop(keys);
    work.charge(Resource::PredicateExpressionSteps, len as u64)?;
    // Invert source-at-destination into destination-of-source, then discharge
    // each cycle in place. Equal keys kept their input order during the merge.
    for (destination, &source) in order.iter().enumerate() {
        scratch[source] = destination;
    }
    for index in 0..len {
        while scratch[index] != index {
            let destination = scratch[index];
            children.swap(index, destination);
            scratch.swap(index, destination);
        }
    }
    Ok(())
}
