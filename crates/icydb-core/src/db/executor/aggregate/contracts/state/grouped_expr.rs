//! Module: executor::aggregate::contracts::state::grouped_expr
//! Responsibility: compatibility re-export for grouped aggregate expression programs.
//! Does not own: expression compilation or expression evaluation.
//! Boundary: aggregate reducer code imports the compiled expression type here,
//! while the implementation lives under `query::plan::expr`.

use crate::{
    db::{executor::pipeline::runtime::RowView, query::plan::expr::CompiledExprSlotReader},
    value::Value,
};
use std::borrow::Cow;

impl CompiledExprSlotReader for RowView {
    fn compiled_slot_value(&self, slot: usize) -> Option<Cow<'_, Value>> {
        self.slot_value(slot)
    }
}

pub(in crate::db::executor::aggregate) use crate::db::query::plan::expr::GroupedCompiledExpr;
