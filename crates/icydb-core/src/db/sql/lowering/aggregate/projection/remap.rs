use crate::db::{
    query::{builder::AggregateExpr, plan::AggregateSemanticKey},
    sql::lowering::{SqlLoweringError, aggregate::terminal::LoweredSqlGlobalAggregateTerminal},
};
use std::collections::HashMap;
use std::{
    collections::hash_map::DefaultHasher,
    hash::{Hash, Hasher},
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
    semantic_keys: Vec<AggregateSemanticKey>,
    indices_by_semantic_fingerprint: HashMap<u64, Vec<usize>>,
}

impl GlobalAggregateTerminalInterner {
    pub(super) fn with_capacity(capacity: usize) -> Self {
        Self {
            terminals: Vec::with_capacity(capacity),
            semantic_keys: Vec::with_capacity(capacity),
            indices_by_semantic_fingerprint: HashMap::with_capacity(capacity),
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

        let semantic_key = AggregateSemanticKey::from_aggregate_expr(aggregate_expr);
        let fingerprint = aggregate_semantic_key_fingerprint(&semantic_key);
        let indices = self
            .indices_by_semantic_fingerprint
            .entry(fingerprint)
            .or_default();
        if let Some(index) = indices
            .iter()
            .copied()
            .find(|index| self.semantic_keys.get(*index) == Some(&semantic_key))
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
        indices.push(index);

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
        debug_assert_eq!(
            self.terminals.len(),
            self.indices_by_semantic_fingerprint
                .values()
                .map(Vec::len)
                .sum::<usize>(),
            "global aggregate terminal semantic-key index must retain one slot per terminal",
        );
    }
}

fn aggregate_semantic_key_fingerprint(key: &AggregateSemanticKey) -> u64 {
    let mut hasher = DefaultHasher::new();
    format!("{key:?}").hash(&mut hasher);
    hasher.finish()
}
