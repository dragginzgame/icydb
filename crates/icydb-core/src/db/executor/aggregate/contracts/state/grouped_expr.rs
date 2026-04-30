//! Module: executor::aggregate::contracts::state::grouped_expr
//! Responsibility: compatibility re-export for grouped aggregate expression programs.
//! Does not own: expression compilation or expression evaluation.
//! Boundary: aggregate reducer code imports the compiled expression type here,
//! while the implementation lives under `query::plan::expr`.

use crate::{
    db::{executor::pipeline::runtime::RowView, query::plan::expr::CompiledExprValueReader},
    value::Value,
};
use std::borrow::Cow;

impl CompiledExprValueReader for RowView {
    fn read_slot(&self, slot: usize) -> Option<Cow<'_, Value>> {
        self.slot_value_ref(slot).map(Cow::Borrowed)
    }

    fn read_group_key(&self, _offset: usize) -> Option<Cow<'_, Value>> {
        None
    }

    fn read_aggregate(&self, _index: usize) -> Option<Cow<'_, Value>> {
        None
    }
}

pub(in crate::db::executor::aggregate) use crate::db::query::plan::expr::CompiledExpr;
