use crate::db::{
    query::{builder::AggregateExpr, plan::expr::Expr},
    sql::lowering::{
        SqlLoweringError,
        aggregate::{
            semantics::AggregateTerminalSemanticKey, terminal::SqlGlobalAggregateTerminal,
        },
        analyze_lowered_expr,
    },
};

///
/// GlobalAggregateTerminalCollectionMode
///
/// Local selector for the two supported collection contracts over aggregate
/// expressions. Direct collection returns the remap index for `Aggregate`
/// projection items; nested collection only guarantees that every aggregate
/// leaf has been inserted into the terminal list.
///
enum GlobalAggregateTerminalCollectionMode {
    Direct,
    Nested,
}

// Global post-aggregate projection expressions may compose aggregate leaves
// with literals/functions/arithmetic, but they may not reopen direct field
// access outside aggregate inputs.
pub(in crate::db::sql::lowering) fn expr_references_global_direct_fields(expr: &Expr) -> bool {
    analyze_lowered_expr(expr, None).references_direct_fields()
}

pub(in crate::db::sql::lowering::aggregate) fn intern_having_global_aggregate_terminal_index(
    terminals: &mut Vec<SqlGlobalAggregateTerminal>,
    aggregate_expr: &AggregateExpr,
) -> Result<usize, SqlLoweringError> {
    intern_global_aggregate_terminal(terminals, aggregate_expr)
}

// Collect every aggregate leaf referenced by one global post-aggregate output
// expression while deduplicating onto the canonical executable terminal list.
// Direct aggregate terminals report the first-seen terminal remap so the
// terminal-remap contract stays stable for direct aggregate outputs.
pub(super) fn collect_global_aggregate_terminals_from_analysis(
    aggregate_refs: &[AggregateExpr],
    direct_output: bool,
    terminals: &mut Vec<SqlGlobalAggregateTerminal>,
) -> Result<Option<usize>, SqlLoweringError> {
    let mode = if direct_output {
        GlobalAggregateTerminalCollectionMode::Direct
    } else {
        GlobalAggregateTerminalCollectionMode::Nested
    };

    collect_global_aggregate_terminals_with_mode(aggregate_refs, terminals, mode)
}

fn collect_global_aggregate_terminals_with_mode(
    aggregate_refs: &[AggregateExpr],
    terminals: &mut Vec<SqlGlobalAggregateTerminal>,
    mode: GlobalAggregateTerminalCollectionMode,
) -> Result<Option<usize>, SqlLoweringError> {
    let mut direct_terminal_index = None;
    for aggregate_expr in aggregate_refs {
        let unique_index = intern_global_aggregate_terminal(terminals, aggregate_expr)?;
        if direct_terminal_index.is_none()
            && matches!(mode, GlobalAggregateTerminalCollectionMode::Direct)
        {
            direct_terminal_index = Some(unique_index);
        }
    }

    Ok(direct_terminal_index)
}

fn intern_global_aggregate_terminal(
    terminals: &mut Vec<SqlGlobalAggregateTerminal>,
    aggregate_expr: &AggregateExpr,
) -> Result<usize, SqlLoweringError> {
    let terminal = SqlGlobalAggregateTerminal::from_aggregate_expr(aggregate_expr)?;
    let semantic_key = AggregateTerminalSemanticKey::from_terminal(&terminal);

    Ok(terminals
        .iter()
        .position(|current| AggregateTerminalSemanticKey::from_terminal(current) == semantic_key)
        .unwrap_or_else(|| {
            let index = terminals.len();
            terminals.push(terminal);
            index
        }))
}
