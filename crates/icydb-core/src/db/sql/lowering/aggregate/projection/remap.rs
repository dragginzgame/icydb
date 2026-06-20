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

///
/// GlobalAggregateTerminalInterner
///
/// Owns the parallel executable-terminal and semantic-key vectors so global
/// aggregate lowering cannot accidentally update one without the other.
///
pub(super) struct GlobalAggregateTerminalInterner {
    terminals: Vec<LoweredSqlGlobalAggregateTerminal>,
    semantic_keys: Vec<AggregateTerminalSemanticKey>,
}

impl GlobalAggregateTerminalInterner {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            terminals: Vec::with_capacity(capacity),
            semantic_keys: Vec::with_capacity(capacity),
        }
    }

    pub(super) fn into_terminals(self) -> Vec<LoweredSqlGlobalAggregateTerminal> {
        debug_assert_eq!(
            self.terminals.len(),
            self.semantic_keys.len(),
            "global aggregate terminal semantic keys must stay aligned with retained terminals",
        );

        self.terminals
    }

    pub(super) fn intern(
        &mut self,
        aggregate_expr: &AggregateExpr,
    ) -> Result<usize, SqlLoweringError> {
        self.assert_aligned();

        let semantic_key = AggregateTerminalSemanticKey::from_aggregate_expr(aggregate_expr);
        if let Some(index) = self
            .semantic_keys
            .iter()
            .position(|current| current == &semantic_key)
        {
            return Ok(index);
        }

        let terminal = LoweredSqlGlobalAggregateTerminal::from_aggregate_expr_with_semantic_key(
            aggregate_expr,
            semantic_key.clone(),
        )?;
        let index = self.terminals.len();
        self.terminals.push(terminal);
        self.semantic_keys.push(semantic_key);

        Ok(index)
    }

    // Collect every aggregate leaf referenced by one global post-aggregate
    // output expression while deduplicating onto the canonical executable
    // terminal list. Direct aggregate terminals report the first-seen terminal
    // remap so the terminal-remap contract stays stable for direct outputs.
    pub(super) fn collect_from_analysis(
        &mut self,
        aggregate_refs: &[AggregateExpr],
        direct_output: bool,
    ) -> Result<Option<usize>, SqlLoweringError> {
        let mode = if direct_output {
            GlobalAggregateTerminalCollectionMode::Direct
        } else {
            GlobalAggregateTerminalCollectionMode::Nested
        };

        self.collect_with_mode(aggregate_refs, mode)
    }

    fn collect_with_mode(
        &mut self,
        aggregate_refs: &[AggregateExpr],
        mode: GlobalAggregateTerminalCollectionMode,
    ) -> Result<Option<usize>, SqlLoweringError> {
        let mut direct_terminal_index = None;
        for aggregate_expr in aggregate_refs {
            let unique_index = self.intern(aggregate_expr)?;
            if direct_terminal_index.is_none()
                && matches!(mode, GlobalAggregateTerminalCollectionMode::Direct)
            {
                direct_terminal_index = Some(unique_index);
            }
        }

        Ok(direct_terminal_index)
    }

    fn assert_aligned(&self) {
        debug_assert_eq!(
            self.terminals.len(),
            self.semantic_keys.len(),
            "global aggregate terminal semantic keys must stay aligned with retained terminals",
        );
    }
}
