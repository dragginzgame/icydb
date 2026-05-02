//! Module: db::session::sql::compile::artifacts
//! Responsibility: compile-stage DTOs and attribution accumulation.
//! Does not own: semantic lowering, compiled-command cache lookup, or execution.
//! Boundary: shared payloads passed between SQL compile and cache shells.

use crate::db::{session::sql::CompiledSqlCommand, sql::parser::SqlParsePhaseAttribution};

///
/// SqlCompilePhaseAttribution
///
/// SqlCompilePhaseAttribution keeps the SQL-front-end compile miss path split
/// into the concrete stages that still exist after the shared lower-cache
/// collapse.
///

#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub(in crate::db) struct SqlCompilePhaseAttribution {
    pub cache_key: u64,
    pub cache_lookup: u64,
    pub parse: u64,
    pub parse_tokenize: u64,
    pub parse_select: u64,
    pub parse_expr: u64,
    pub parse_predicate: u64,
    pub aggregate_lane_check: u64,
    pub prepare: u64,
    pub lower: u64,
    pub bind: u64,
    pub cache_insert: u64,
}

///
/// SqlCompileArtifacts
///
/// SqlCompileArtifacts is the cache-independent result of compiling one parsed
/// SQL statement for one authority.
/// It keeps the semantic command and stage-local instruction counters together.
///

#[derive(Debug)]
pub(in crate::db) struct SqlCompileArtifacts {
    pub command: CompiledSqlCommand,
    pub shape: SqlQueryShape,
    pub aggregate_lane_check: u64,
    pub prepare: u64,
    pub lower: u64,
    pub bind: u64,
}

///
/// SqlQueryShape
///
/// SqlQueryShape is the compile-owned semantic descriptor for one SQL command.
/// It records stable command facts once at the compile boundary so later
/// phases do not need to rediscover semantic classification from syntax.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct SqlQueryShape {
    pub is_aggregate: bool,
    pub returns_rows: bool,
    pub is_mutation: bool,
}

impl SqlQueryShape {
    #[must_use]
    pub(in crate::db::session::sql) const fn read_rows(is_aggregate: bool) -> Self {
        Self {
            is_aggregate,
            returns_rows: true,
            is_mutation: false,
        }
    }

    #[must_use]
    pub(in crate::db::session::sql) const fn metadata() -> Self {
        Self {
            is_aggregate: false,
            returns_rows: false,
            is_mutation: false,
        }
    }

    #[must_use]
    pub(in crate::db::session::sql) const fn mutation(returns_rows: bool) -> Self {
        Self {
            is_aggregate: false,
            returns_rows,
            is_mutation: true,
        }
    }
}

///
/// SqlCompileAttributionBuilder
///
/// SqlCompileAttributionBuilder accumulates one compile miss path in pipeline
/// order before emitting the diagnostics payload.
/// It keeps cache, parser, compile-core, and cache-insert counters aligned.
///

#[derive(Clone, Copy, Debug, Default)]
pub(in crate::db::session::sql) struct SqlCompileAttributionBuilder {
    phase: SqlCompilePhaseAttribution,
}

impl SqlCompileAttributionBuilder {
    // Record the cache-key stage after the outer compile shell builds the
    // syntax/entity/surface key used by the session-local compiled cache.
    pub(in crate::db::session::sql) const fn record_cache_key(&mut self, local_instructions: u64) {
        self.phase.cache_key = local_instructions;
    }

    // Record the compiled-command cache lookup stage before parse work starts.
    pub(in crate::db::session::sql) const fn record_cache_lookup(
        &mut self,
        local_instructions: u64,
    ) {
        self.phase.cache_lookup = local_instructions;
    }

    // Record parser-owned sub-buckets while preserving the public diagnostics
    // contract that parse subphases add back up to the measured parse total.
    pub(in crate::db::session::sql) const fn record_parse(
        &mut self,
        local_instructions: u64,
        attribution: SqlParsePhaseAttribution,
    ) {
        let statement_shell = local_instructions
            .saturating_sub(attribution.tokenize)
            .saturating_sub(attribution.expr)
            .saturating_sub(attribution.predicate);

        self.phase.parse = local_instructions;
        self.phase.parse_tokenize = attribution.tokenize;
        // Public compile diagnostics promise an exhaustive parse split. Keep
        // the statement-shell bucket as the residual owner for parser overhead
        // that is outside tokenization, expression roots, and predicate roots.
        self.phase.parse_select = statement_shell;
        self.phase.parse_expr = attribution.expr;
        self.phase.parse_predicate = attribution.predicate;
    }

    // Merge the cache-independent compile artifact counters into the outer
    // miss-path attribution after surface validation and semantic compilation.
    pub(in crate::db::session::sql) const fn record_core_compile(
        &mut self,
        attribution: SqlCompilePhaseAttribution,
    ) {
        self.phase.aggregate_lane_check = attribution.aggregate_lane_check;
        self.phase.prepare = attribution.prepare;
        self.phase.lower = attribution.lower;
        self.phase.bind = attribution.bind;
    }

    // Record cache insertion as the final compile miss-path stage.
    pub(in crate::db::session::sql) const fn record_cache_insert(
        &mut self,
        local_instructions: u64,
    ) {
        self.phase.cache_insert = local_instructions;
    }

    #[must_use]
    pub(in crate::db::session::sql) const fn finish(self) -> SqlCompilePhaseAttribution {
        self.phase
    }
}

impl SqlCompileArtifacts {
    // Build one compile artifact and assert that the compile-owned semantic
    // shape still agrees with the command payload it describes.
    pub(in crate::db::session::sql) fn new(
        command: CompiledSqlCommand,
        shape: SqlQueryShape,
        aggregate_lane_check: u64,
        prepare: u64,
        lower: u64,
        bind: u64,
    ) -> Self {
        debug_assert_eq!(
            shape.is_aggregate,
            matches!(command, CompiledSqlCommand::GlobalAggregate { .. }),
            "compile aggregate shape must match the compiled command variant"
        );
        debug_assert_eq!(
            shape.is_mutation,
            matches!(
                command,
                CompiledSqlCommand::Delete { .. }
                    | CompiledSqlCommand::Insert(_)
                    | CompiledSqlCommand::Update(_)
            ),
            "compile mutation shape must match the compiled command variant"
        );
        debug_assert_eq!(
            shape.returns_rows,
            Self::command_returns_rows(&command),
            "compile row-returning shape must match the compiled command variant"
        );

        Self {
            command,
            shape,
            aggregate_lane_check,
            prepare,
            lower,
            bind,
        }
    }

    // Keep row-returning validation local to artifact construction. Runtime
    // consumers read `shape.returns_rows`; this debug-only mirror exists only
    // to catch compile-time descriptor drift.
    const fn command_returns_rows(command: &CompiledSqlCommand) -> bool {
        match command {
            CompiledSqlCommand::Select { .. } | CompiledSqlCommand::GlobalAggregate { .. } => true,
            CompiledSqlCommand::Delete { returning, .. } => returning.is_some(),
            CompiledSqlCommand::Insert(statement) => statement.returning.is_some(),
            CompiledSqlCommand::Update(statement) => statement.returning.is_some(),
            CompiledSqlCommand::Explain(_)
            | CompiledSqlCommand::DescribeEntity
            | CompiledSqlCommand::ShowIndexesEntity
            | CompiledSqlCommand::ShowColumnsEntity
            | CompiledSqlCommand::ShowEntities => false,
        }
    }

    // Convert the core compile artifact into the phase-attribution shape used
    // by SQL diagnostics. Cache and parse counters stay zero here because the
    // cache wrapper owns those outer phases.
    #[must_use]
    pub(in crate::db::session::sql) const fn phase_attribution(
        &self,
    ) -> SqlCompilePhaseAttribution {
        SqlCompilePhaseAttribution {
            cache_key: 0,
            cache_lookup: 0,
            parse: 0,
            parse_tokenize: 0,
            parse_select: 0,
            parse_expr: 0,
            parse_predicate: 0,
            aggregate_lane_check: self.aggregate_lane_check,
            prepare: self.prepare,
            lower: self.lower,
            bind: self.bind,
            cache_insert: 0,
        }
    }
}
