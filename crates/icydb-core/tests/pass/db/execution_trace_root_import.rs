use icydb_core::db::{ExecutionAccessPathVariant, ExecutionOptimization, ExecutionTrace};

fn accept_trace(
    _trace: Option<ExecutionTrace>,
    _variant: ExecutionAccessPathVariant,
    _optimization: Option<ExecutionOptimization>,
) {
}

fn main() {}
