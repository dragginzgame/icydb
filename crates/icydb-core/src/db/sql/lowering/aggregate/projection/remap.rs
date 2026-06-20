use crate::db::{
    query::builder::AggregateExpr,
    sql::lowering::{
        SqlLoweringError,
        aggregate::{
            semantics::AggregateTerminalSemanticKey, terminal::LoweredSqlGlobalAggregateTerminal,
        },
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

pub(super) fn intern_global_aggregate_terminal_index(
    terminals: &mut Vec<LoweredSqlGlobalAggregateTerminal>,
    semantic_keys: &mut Vec<AggregateTerminalSemanticKey>,
    aggregate_expr: &AggregateExpr,
) -> Result<usize, SqlLoweringError> {
    intern_global_aggregate_terminal(terminals, semantic_keys, aggregate_expr)
}

// Collect every aggregate leaf referenced by one global post-aggregate output
// expression while deduplicating onto the canonical executable terminal list.
// Direct aggregate terminals report the first-seen terminal remap so the
// terminal-remap contract stays stable for direct aggregate outputs.
pub(super) fn collect_global_aggregate_terminals_from_analysis(
    aggregate_refs: &[AggregateExpr],
    direct_output: bool,
    terminals: &mut Vec<LoweredSqlGlobalAggregateTerminal>,
    semantic_keys: &mut Vec<AggregateTerminalSemanticKey>,
) -> Result<Option<usize>, SqlLoweringError> {
    let mode = if direct_output {
        GlobalAggregateTerminalCollectionMode::Direct
    } else {
        GlobalAggregateTerminalCollectionMode::Nested
    };

    collect_global_aggregate_terminals_with_mode(aggregate_refs, terminals, semantic_keys, mode)
}

fn collect_global_aggregate_terminals_with_mode(
    aggregate_refs: &[AggregateExpr],
    terminals: &mut Vec<LoweredSqlGlobalAggregateTerminal>,
    semantic_keys: &mut Vec<AggregateTerminalSemanticKey>,
    mode: GlobalAggregateTerminalCollectionMode,
) -> Result<Option<usize>, SqlLoweringError> {
    let mut direct_terminal_index = None;
    for aggregate_expr in aggregate_refs {
        let unique_index =
            intern_global_aggregate_terminal(terminals, semantic_keys, aggregate_expr)?;
        if direct_terminal_index.is_none()
            && matches!(mode, GlobalAggregateTerminalCollectionMode::Direct)
        {
            direct_terminal_index = Some(unique_index);
        }
    }

    Ok(direct_terminal_index)
}

fn intern_global_aggregate_terminal(
    terminals: &mut Vec<LoweredSqlGlobalAggregateTerminal>,
    semantic_keys: &mut Vec<AggregateTerminalSemanticKey>,
    aggregate_expr: &AggregateExpr,
) -> Result<usize, SqlLoweringError> {
    debug_assert_eq!(
        terminals.len(),
        semantic_keys.len(),
        "global aggregate terminal semantic keys must stay aligned with retained terminals",
    );

    let terminal = LoweredSqlGlobalAggregateTerminal::from_aggregate_expr(aggregate_expr)?;
    let semantic_key = terminal.semantic_key().clone();

    Ok(semantic_keys
        .iter()
        .position(|current| current == &semantic_key)
        .unwrap_or_else(|| {
            let index = terminals.len();
            terminals.push(terminal);
            semantic_keys.push(semantic_key);
            index
        }))
}
