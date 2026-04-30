//! Module: executor::aggregate::contracts::state::grouped_expr
//! Responsibility: compatibility re-export for grouped aggregate expression programs.
//! Does not own: expression compilation or expression evaluation.
//! Boundary: aggregate reducer code imports the compiled expression type here,
//! while the implementation lives under `query::plan::expr`.

use crate::{
    db::{executor::pipeline::runtime::RowView, query::plan::expr::CompiledExprValueReader},
    value::Value,
};

impl CompiledExprValueReader for RowView {
    fn read_slot(&self, slot: usize) -> Option<&Value> {
        self.slot_value_ref(slot)
    }

    fn read_group_key(&self, _offset: usize) -> Option<&Value> {
        None
    }

    fn read_aggregate(&self, _index: usize) -> Option<&Value> {
        None
    }
}

pub(in crate::db::executor::aggregate) use crate::db::query::plan::expr::CompiledExpr;
