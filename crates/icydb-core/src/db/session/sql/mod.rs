//! Module: db::session::sql
//! Responsibility: session-owned SQL execution, explain, projection, and
//! surface-classification helpers above lowered SQL commands.
//! Does not own: SQL parsing or structural executor runtime behavior.
//! Boundary: keeps session visibility, authority selection, and SQL surface routing in one subsystem.

mod cache;
mod compiled;
mod execute;
mod projection;

#[cfg(feature = "diagnostics")]
use candid::CandidType;
#[cfg(feature = "diagnostics")]
use serde::Deserialize;
use std::sync::Arc;

#[cfg(feature = "diagnostics")]
use crate::db::DataStore;
#[cfg(feature = "diagnostics")]
use crate::db::executor::{
    GroupedCountAttribution as ExecutorGroupedCountAttribution, ScalarAggregateTerminalAttribution,
    current_pure_covering_decode_local_instructions,
    current_pure_covering_row_assembly_local_instructions,
};
#[cfg(test)]
use crate::db::sql::parser::parse_sql;
#[cfg(feature = "diagnostics")]
use crate::db::{GroupedCountAttribution, GroupedExecutionAttribution};
use crate::{
    db::{
        DbSession, GroupedRow, MissingRowPolicy, PersistedRow, QueryError,
        commit::CommitSchemaFingerprint,
        executor::{EntityAuthority, SharedPreparedExecutionPlan},
        query::intent::StructuralQuery,
        session::sql::projection::{
            projection_fixed_scales_from_projection_spec, projection_labels_from_projection_spec,
        },
        sql::lowering::{
            PreparedSqlStatement, bind_lowered_sql_delete_query_structural,
            bind_lowered_sql_select_query_structural,
            compile_sql_global_aggregate_command_core_from_prepared,
            extract_prepared_sql_insert_statement, extract_prepared_sql_update_statement,
            lower_prepared_sql_delete_statement, lower_prepared_sql_select_statement,
            lower_sql_command_from_prepared_statement, prepare_sql_statement,
        },
        sql::parser::{SqlParsePhaseAttribution, SqlStatement, parse_sql_with_attribution},
    },
    model::entity::EntityModel,
    traits::{CanisterKind, EntityValue},
    value::OutputValue,
};

pub(in crate::db::session::sql) use crate::db::diagnostics::measure_local_instruction_delta as measure_sql_stage;
pub(in crate::db::session::sql) use cache::SqlCompiledCommandSurface;
pub(in crate::db) use cache::{SqlCacheAttribution, SqlCompiledCommandCacheKey};
pub(in crate::db) use compiled::{CompiledSqlCommand, SqlProjectionContract};

#[cfg(all(test, not(feature = "diagnostics")))]
pub(crate) use crate::db::session::sql::projection::with_sql_projection_materialization_metrics;
#[cfg(feature = "diagnostics")]
pub use crate::db::session::sql::projection::{
    SqlProjectionMaterializationMetrics, with_sql_projection_materialization_metrics,
};

/// Unified SQL statement payload returned by shared SQL lane execution.
#[derive(Debug)]
pub enum SqlStatementResult {
    Count {
        row_count: u32,
    },
    Projection {
        columns: Vec<String>,
        fixed_scales: Vec<Option<u32>>,
        rows: Vec<Vec<OutputValue>>,
        row_count: u32,
    },
    ProjectionText {
        columns: Vec<String>,
        rows: Vec<Vec<String>>,
        row_count: u32,
    },
    Grouped {
        columns: Vec<String>,
        fixed_scales: Vec<Option<u32>>,
        rows: Vec<GroupedRow>,
        row_count: u32,
        next_cursor: Option<String>,
    },
    Explain(String),
    Describe(crate::db::EntitySchemaDescription),
    ShowIndexes(Vec<String>),
    ShowColumns(Vec<crate::db::EntityFieldDescription>),
    ShowEntities(Vec<String>),
}

// SqlCompileAttribution
//
// Candid diagnostics payload for SQL front-end compile counters.
// The short field names are scoped by the `compile` parent field on
// `SqlQueryExecutionAttribution`.
#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlCompileAttribution {
    pub cache_key_local_instructions: u64,
    pub cache_lookup_local_instructions: u64,
    pub parse_local_instructions: u64,
    pub parse_tokenize_local_instructions: u64,
    pub parse_select_local_instructions: u64,
    pub parse_expr_local_instructions: u64,
    pub parse_predicate_local_instructions: u64,
    pub aggregate_lane_check_local_instructions: u64,
    pub prepare_local_instructions: u64,
    pub lower_local_instructions: u64,
    pub bind_local_instructions: u64,
    pub cache_insert_local_instructions: u64,
}

// SqlExecutionAttribution
//
// Candid diagnostics payload for the reduced SQL execute phase.
// Planner, store, executor invocation, executor runtime, and response
// finalization counters stay together under the `execution` parent field.
#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlExecutionAttribution {
    pub planner_local_instructions: u64,
    pub store_local_instructions: u64,
    pub executor_invocation_local_instructions: u64,
    pub executor_local_instructions: u64,
    pub response_finalization_local_instructions: u64,
}

// SqlScalarAggregateAttribution
//
// Candid diagnostics payload for scalar aggregate terminal execution.
// The field names drop the old `scalar_aggregate_` prefix because the parent
// field now owns that context.
#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlScalarAggregateAttribution {
    pub base_row_local_instructions: u64,
    pub reducer_fold_local_instructions: u64,
    pub expression_evaluations: u64,
    pub filter_evaluations: u64,
    pub rows_ingested: u64,
    pub terminal_count: u64,
    pub unique_input_expr_count: u64,
    pub unique_filter_expr_count: u64,
    pub sink_mode: Option<String>,
}

#[cfg(feature = "diagnostics")]
impl SqlScalarAggregateAttribution {
    fn from_executor(terminal: ScalarAggregateTerminalAttribution) -> Option<Self> {
        // Treat the nested payload as absent only when the executor reported
        // no scalar aggregate work at all. This keeps COUNT fast paths compact
        // while preserving any future counter that becomes nonzero.
        let has_scalar_aggregate_work = terminal.base_row_local_instructions != 0
            || terminal.reducer_fold_local_instructions != 0
            || terminal.expression_evaluations != 0
            || terminal.filter_evaluations != 0
            || terminal.rows_ingested != 0
            || terminal.terminal_count != 0
            || terminal.unique_input_expr_count != 0
            || terminal.unique_filter_expr_count != 0
            || terminal.sink_mode.label().is_some();
        if !has_scalar_aggregate_work {
            return None;
        }

        Some(Self {
            base_row_local_instructions: terminal.base_row_local_instructions,
            reducer_fold_local_instructions: terminal.reducer_fold_local_instructions,
            expression_evaluations: terminal.expression_evaluations,
            filter_evaluations: terminal.filter_evaluations,
            rows_ingested: terminal.rows_ingested,
            terminal_count: terminal.terminal_count,
            unique_input_expr_count: terminal.unique_input_expr_count,
            unique_filter_expr_count: terminal.unique_filter_expr_count,
            sink_mode: terminal.sink_mode.label().map(str::to_string),
        })
    }
}

// SqlPureCoveringAttribution
//
// Candid diagnostics payload for pure covering projection counters.
// The value is optional on the top-level SQL attribution because most query
// shapes do not enter this projection path.
#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlPureCoveringAttribution {
    pub decode_local_instructions: u64,
    pub row_assembly_local_instructions: u64,
}

// SqlQueryCacheAttribution
//
// Candid diagnostics payload for SQL compiled-command and shared query-plan
// cache counters observed during one SQL query call.
#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Copy, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlQueryCacheAttribution {
    pub sql_compiled_command_hits: u64,
    pub sql_compiled_command_misses: u64,
    pub shared_query_plan_hits: u64,
    pub shared_query_plan_misses: u64,
}

// SqlQueryExecutionAttribution
//
// SqlQueryExecutionAttribution records the top-level reduced SQL query cost
// split at the new compile/execute seam.
// Every field is an additive counter where zero means no observed work or no
// observed event for that bucket. Path-specific counters are present only for
// the execution path that produced them.

#[cfg(feature = "diagnostics")]
#[derive(CandidType, Clone, Debug, Default, Deserialize, Eq, PartialEq)]
pub struct SqlQueryExecutionAttribution {
    pub compile_local_instructions: u64,
    pub compile: SqlCompileAttribution,
    pub plan_lookup_local_instructions: u64,
    pub execution: SqlExecutionAttribution,
    pub grouped: Option<GroupedExecutionAttribution>,
    pub scalar_aggregate: Option<SqlScalarAggregateAttribution>,
    pub pure_covering: Option<SqlPureCoveringAttribution>,
    pub store_get_calls: u64,
    pub response_decode_local_instructions: u64,
    pub execute_local_instructions: u64,
    pub total_local_instructions: u64,
    pub cache: SqlQueryCacheAttribution,
}

// SqlExecutePhaseAttribution keeps the execute side split into select-plan
// work, physical store/index access, and narrower runtime execution so shell
// tooling can show all three.
#[cfg(feature = "diagnostics")]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct SqlExecutePhaseAttribution {
    pub planner_local_instructions: u64,
    pub store_local_instructions: u64,
    pub executor_invocation_local_instructions: u64,
    pub executor_local_instructions: u64,
    pub response_finalization_local_instructions: u64,
    pub grouped_stream_local_instructions: u64,
    pub grouped_fold_local_instructions: u64,
    pub grouped_finalize_local_instructions: u64,
    pub grouped_count: ExecutorGroupedCountAttribution,
    pub scalar_aggregate_terminal: ScalarAggregateTerminalAttribution,
}

///
/// SqlCompilePhaseAttribution
///
/// SqlCompilePhaseAttribution keeps the SQL-front-end compile miss path split
/// into the concrete stages that still exist after the shared lower-cache
/// collapse.
/// This lets perf audits distinguish cache lookup, parsing, prepared-statement
/// normalization, lowered-command construction, structural binding, and cache
/// insertion cost instead of treating compile as one opaque bucket.
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
/// SQL statement for one authority. It keeps the semantic command and the
/// stage-local instruction counters together so cache wrappers do not unpack
/// anonymous tuples or duplicate compile-pipeline accounting.
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
    const fn read_rows(is_aggregate: bool) -> Self {
        Self {
            is_aggregate,
            returns_rows: true,
            is_mutation: false,
        }
    }

    #[must_use]
    const fn metadata() -> Self {
        Self {
            is_aggregate: false,
            returns_rows: false,
            is_mutation: false,
        }
    }

    #[must_use]
    const fn mutation(returns_rows: bool) -> Self {
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
/// It exists so cache, parser, compile-core, and cache-insert counters cannot
/// drift through repeated manual struct literals.
///

#[derive(Clone, Copy, Debug, Default)]
struct SqlCompileAttributionBuilder {
    phase: SqlCompilePhaseAttribution,
}

impl SqlCompileAttributionBuilder {
    // Record the cache-key stage after the outer compile shell builds the
    // syntax/entity/surface key used by the session-local compiled cache.
    const fn record_cache_key(&mut self, local_instructions: u64) {
        self.phase.cache_key = local_instructions;
    }

    // Record the compiled-command cache lookup stage before parse work starts.
    const fn record_cache_lookup(&mut self, local_instructions: u64) {
        self.phase.cache_lookup = local_instructions;
    }

    // Record parser-owned sub-buckets while preserving the public diagnostics
    // contract that parse subphases add back up to the measured parse total.
    const fn record_parse(
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
    const fn record_core_compile(&mut self, attribution: SqlCompilePhaseAttribution) {
        self.phase.aggregate_lane_check = attribution.aggregate_lane_check;
        self.phase.prepare = attribution.prepare;
        self.phase.lower = attribution.lower;
        self.phase.bind = attribution.bind;
    }

    // Record cache insertion as the final compile miss-path stage.
    const fn record_cache_insert(&mut self, local_instructions: u64) {
        self.phase.cache_insert = local_instructions;
    }

    #[must_use]
    const fn finish(self) -> SqlCompilePhaseAttribution {
        self.phase
    }
}

impl SqlCompileArtifacts {
    // Build one compile artifact and assert that the compile-owned semantic
    // shape still agrees with the command payload it describes. These checks
    // are debug-only so release execution keeps the shape field as a cheap
    // data-flow fact rather than a recomputation hook.
    fn new(
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
    const fn phase_attribution(&self) -> SqlCompilePhaseAttribution {
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

#[cfg(feature = "diagnostics")]
impl SqlExecutePhaseAttribution {
    #[must_use]
    pub(in crate::db) const fn from_execute_total_and_store_total(
        execute_local_instructions: u64,
        store_local_instructions: u64,
    ) -> Self {
        Self {
            planner_local_instructions: 0,
            store_local_instructions,
            executor_invocation_local_instructions: execute_local_instructions,
            executor_local_instructions: execute_local_instructions
                .saturating_sub(store_local_instructions),
            response_finalization_local_instructions: 0,
            grouped_stream_local_instructions: 0,
            grouped_fold_local_instructions: 0,
            grouped_finalize_local_instructions: 0,
            grouped_count: ExecutorGroupedCountAttribution::none(),
            scalar_aggregate_terminal: ScalarAggregateTerminalAttribution::none(),
        }
    }
}

// Keep parsing as a module-owned helper instead of hanging a pure parser off
// `DbSession` as a fake session method.
#[cfg(test)]
pub(in crate::db) fn parse_sql_statement(sql: &str) -> Result<SqlStatement, QueryError> {
    parse_sql(sql).map_err(QueryError::from_sql_parse_error)
}

// Measure one SQL compile stage and immediately surface the stage result. The
// helper keeps attribution capture uniform while avoiding repeated
// `(cost, result); result?` boilerplate across the compile pipeline.
fn measured<T>(stage: impl FnOnce() -> Result<T, QueryError>) -> Result<(u64, T), QueryError> {
    let (local_instructions, result) = measure_sql_stage(stage);
    let value = result?;

    Ok((local_instructions, value))
}

impl<C: CanisterKind> DbSession<C> {
    // Compile one parsed SQL statement into the generic-free session-owned
    // semantic command artifact for one resolved authority.
    fn compile_sql_statement_core(
        statement: &SqlStatement,
        authority: EntityAuthority,
        compiled_cache_key: SqlCompiledCommandCacheKey,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let model = authority.model();

        match statement {
            SqlStatement::Select(_) => Self::compile_select(statement, model, compiled_cache_key),
            SqlStatement::Delete(_) => Self::compile_delete(statement, model),
            SqlStatement::Insert(_) => Self::compile_insert(statement, model),
            SqlStatement::Update(_) => Self::compile_update(statement, model),
            SqlStatement::Explain(_) => Self::compile_explain(statement, model),
            SqlStatement::Describe(_) => Self::compile_describe(statement, model),
            SqlStatement::ShowIndexes(_) => Self::compile_show_indexes(statement, model),
            SqlStatement::ShowColumns(_) => Self::compile_show_columns(statement, model),
            SqlStatement::ShowEntities(_) => Ok(Self::compile_show_entities()),
        }
    }

    // Prepare one statement against a resolved entity model while preserving
    // the prepare-stage counter as a first-class compile artifact field.
    fn prepare_statement_for_model(
        statement: &SqlStatement,
        model: &'static EntityModel,
    ) -> Result<(u64, PreparedSqlStatement), QueryError> {
        measured(|| {
            prepare_sql_statement(statement, model.name())
                .map_err(QueryError::from_sql_lowering_error)
        })
    }

    // Compile SELECT by owning only lane detection. Each lane keeps its own
    // lowering/binding behavior so aggregate and scalar SELECTs do not share a
    // branch with different semantic assumptions.
    fn compile_select(
        statement: &SqlStatement,
        model: &'static EntityModel,
        compiled_cache_key: SqlCompiledCommandCacheKey,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, prepared) =
            Self::prepare_statement_for_model(statement, model)?;
        let (aggregate_lane_check_local_instructions, requires_aggregate_lane) =
            measured(|| Ok(prepared.statement().is_global_aggregate_lane_shape()))?;

        if requires_aggregate_lane {
            Self::compile_select_global_aggregate(
                prepared,
                model,
                aggregate_lane_check_local_instructions,
                prepare_local_instructions,
            )
        } else {
            Self::compile_select_non_aggregate(
                prepared,
                model,
                compiled_cache_key,
                aggregate_lane_check_local_instructions,
                prepare_local_instructions,
            )
        }
    }

    // Compile one prepared SELECT that belongs on the global aggregate lane.
    // This path intentionally stays separate from scalar SELECT binding so
    // aggregate-specific lowering and future aggregate detection changes have
    // one narrow owner.
    fn compile_select_global_aggregate(
        prepared: PreparedSqlStatement,
        model: &'static EntityModel,
        aggregate_lane_check_local_instructions: u64,
        prepare_local_instructions: u64,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (lower_local_instructions, command) = measured(|| {
            compile_sql_global_aggregate_command_core_from_prepared(
                prepared,
                model,
                MissingRowPolicy::Ignore,
            )
            .map_err(QueryError::from_sql_lowering_error)
        })?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::GlobalAggregate {
                command: Box::new(command),
            },
            SqlQueryShape::read_rows(true),
            aggregate_lane_check_local_instructions,
            prepare_local_instructions,
            lower_local_instructions,
            0,
        ))
    }

    // Compile one prepared SELECT that remains on the ordinary scalar query
    // lane. Projection/query binding stays here instead of sharing branches
    // with the aggregate path.
    fn compile_select_non_aggregate(
        prepared: PreparedSqlStatement,
        model: &'static EntityModel,
        compiled_cache_key: SqlCompiledCommandCacheKey,
        aggregate_lane_check_local_instructions: u64,
        prepare_local_instructions: u64,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (lower_local_instructions, select) = measured(|| {
            lower_prepared_sql_select_statement(prepared, model)
                .map_err(QueryError::from_sql_lowering_error)
        })?;
        let (bind_local_instructions, query) = measured(|| {
            bind_lowered_sql_select_query_structural(model, select, MissingRowPolicy::Ignore)
                .map_err(QueryError::from_sql_lowering_error)
        })?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::Select {
                query: Arc::new(query),
                compiled_cache_key,
            },
            SqlQueryShape::read_rows(false),
            aggregate_lane_check_local_instructions,
            prepare_local_instructions,
            lower_local_instructions,
            bind_local_instructions,
        ))
    }

    // Compile DELETE through the same prepare/lower/bind phases as ordinary
    // SELECTs while preserving DELETE-specific RETURNING extraction.
    fn compile_delete(
        statement: &SqlStatement,
        model: &'static EntityModel,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, prepared) =
            Self::prepare_statement_for_model(statement, model)?;
        let (lower_local_instructions, delete) = measured(|| {
            lower_prepared_sql_delete_statement(prepared)
                .map_err(QueryError::from_sql_lowering_error)
        })?;
        let returning = delete.returning().cloned();
        let query = delete.into_base_query();
        let (bind_local_instructions, query) = measured(|| {
            Ok(bind_lowered_sql_delete_query_structural(
                model,
                query,
                MissingRowPolicy::Ignore,
            ))
        })?;

        let shape = SqlQueryShape::mutation(returning.is_some());

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::Delete {
                query: Arc::new(query),
                returning,
            },
            shape,
            0,
            prepare_local_instructions,
            lower_local_instructions,
            bind_local_instructions,
        ))
    }

    // Compile INSERT after the shared prepare phase. Prepared statement
    // extraction intentionally remains outside the lower/bind counters because
    // the historical INSERT path has no separate lower or bind stage.
    fn compile_insert(
        statement: &SqlStatement,
        model: &'static EntityModel,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, prepared) =
            Self::prepare_statement_for_model(statement, model)?;
        let statement = extract_prepared_sql_insert_statement(prepared)
            .map_err(QueryError::from_sql_lowering_error)?;

        let shape = SqlQueryShape::mutation(statement.returning.is_some());

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::Insert(statement),
            shape,
            0,
            prepare_local_instructions,
            0,
            0,
        ))
    }

    // Compile UPDATE after the shared prepare phase. Like INSERT, UPDATE owns
    // only prepared-statement extraction here to preserve existing attribution.
    fn compile_update(
        statement: &SqlStatement,
        model: &'static EntityModel,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, prepared) =
            Self::prepare_statement_for_model(statement, model)?;
        let statement = extract_prepared_sql_update_statement(prepared)
            .map_err(QueryError::from_sql_lowering_error)?;

        let shape = SqlQueryShape::mutation(statement.returning.is_some());

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::Update(statement),
            shape,
            0,
            prepare_local_instructions,
            0,
            0,
        ))
    }

    // Compile EXPLAIN by lowering its prepared target but deliberately not
    // binding it into an executable query, matching the explain-only contract.
    fn compile_explain(
        statement: &SqlStatement,
        model: &'static EntityModel,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, prepared) =
            Self::prepare_statement_for_model(statement, model)?;
        let (lower_local_instructions, lowered) = measured(|| {
            lower_sql_command_from_prepared_statement(prepared, model)
                .map_err(QueryError::from_sql_lowering_error)
        })?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::Explain(Box::new(lowered)),
            SqlQueryShape::metadata(),
            0,
            prepare_local_instructions,
            lower_local_instructions,
            0,
        ))
    }

    // Compile DESCRIBE by validating the prepared surface and returning the
    // fixed introspection command without a lower or bind stage.
    fn compile_describe(
        statement: &SqlStatement,
        model: &'static EntityModel,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, _prepared) =
            Self::prepare_statement_for_model(statement, model)?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::DescribeEntity,
            SqlQueryShape::metadata(),
            0,
            prepare_local_instructions,
            0,
            0,
        ))
    }

    // Compile SHOW INDEXES by validating the prepared surface and returning
    // the fixed introspection command.
    fn compile_show_indexes(
        statement: &SqlStatement,
        model: &'static EntityModel,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, _prepared) =
            Self::prepare_statement_for_model(statement, model)?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::ShowIndexesEntity,
            SqlQueryShape::metadata(),
            0,
            prepare_local_instructions,
            0,
            0,
        ))
    }

    // Compile SHOW COLUMNS by validating the prepared surface and returning
    // the fixed introspection command.
    fn compile_show_columns(
        statement: &SqlStatement,
        model: &'static EntityModel,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        let (prepare_local_instructions, _prepared) =
            Self::prepare_statement_for_model(statement, model)?;

        Ok(SqlCompileArtifacts::new(
            CompiledSqlCommand::ShowColumnsEntity,
            SqlQueryShape::metadata(),
            0,
            prepare_local_instructions,
            0,
            0,
        ))
    }

    // Compile SHOW ENTITIES without entity-bound preparation because the
    // command is catalog-wide and historically reports no compile sub-stages.
    fn compile_show_entities() -> SqlCompileArtifacts {
        SqlCompileArtifacts::new(
            CompiledSqlCommand::ShowEntities,
            SqlQueryShape::metadata(),
            0,
            0,
            0,
            0,
        )
    }

    // Own the complete parsed-statement compile boundary: surface validation
    // happens here before the cache-independent semantic compiler runs, so no
    // caller can accidentally compile a query through the update lane or the
    // inverse.
    fn compile_sql_statement_entry(
        statement: &SqlStatement,
        surface: SqlCompiledCommandSurface,
        authority: EntityAuthority,
        compiled_cache_key: SqlCompiledCommandCacheKey,
    ) -> Result<SqlCompileArtifacts, QueryError> {
        Self::ensure_sql_statement_supported_for_surface(statement, surface)?;

        Self::compile_sql_statement_core(statement, authority, compiled_cache_key)
    }

    // Wrap the complete compile entrypoint with the attribution shape used by
    // callers. The core artifact remains the single authority for command
    // output and stage-local compile counters.
    fn compile_sql_statement_measured(
        statement: &SqlStatement,
        surface: SqlCompiledCommandSurface,
        authority: EntityAuthority,
        compiled_cache_key: SqlCompiledCommandCacheKey,
    ) -> Result<(SqlCompileArtifacts, SqlCompilePhaseAttribution), QueryError> {
        let artifacts =
            Self::compile_sql_statement_entry(statement, surface, authority, compiled_cache_key)?;
        debug_assert!(
            !artifacts.shape.is_aggregate || artifacts.bind == 0,
            "aggregate SQL artifacts must not report scalar bind work"
        );
        debug_assert!(
            !artifacts.shape.is_mutation || artifacts.aggregate_lane_check == 0,
            "mutation SQL artifacts must not report SELECT lane checks"
        );
        let attribution = artifacts.phase_attribution();

        Ok((artifacts, attribution))
    }

    // Resolve one SQL SELECT entirely through the shared lower query-plan
    // cache and derive only the outward SQL projection contract locally.
    fn sql_select_prepared_plan(
        &self,
        query: &StructuralQuery,
        authority: EntityAuthority,
        cache_schema_fingerprint: CommitSchemaFingerprint,
    ) -> Result<
        (
            SharedPreparedExecutionPlan,
            SqlProjectionContract,
            SqlCacheAttribution,
        ),
        QueryError,
    > {
        let (prepared_plan, cache_attribution) = self.cached_shared_query_plan_for_authority(
            authority,
            cache_schema_fingerprint,
            query,
        )?;
        let projection_spec = prepared_plan
            .logical_plan()
            .projection_spec(authority.model());
        let projection = SqlProjectionContract::new(
            projection_labels_from_projection_spec(&projection_spec),
            projection_fixed_scales_from_projection_spec(&projection_spec),
        );

        Ok((
            prepared_plan,
            projection,
            SqlCacheAttribution::from_shared_query_plan_cache(cache_attribution),
        ))
    }

    // Keep query/update surface gating owned by one helper so the SQL
    // compiled-command lane does not duplicate the same statement-family split
    // just to change the outward error wording.
    fn ensure_sql_statement_supported_for_surface(
        statement: &SqlStatement,
        surface: SqlCompiledCommandSurface,
    ) -> Result<(), QueryError> {
        match (surface, statement) {
            (
                SqlCompiledCommandSurface::Query,
                SqlStatement::Select(_)
                | SqlStatement::Explain(_)
                | SqlStatement::Describe(_)
                | SqlStatement::ShowIndexes(_)
                | SqlStatement::ShowColumns(_)
                | SqlStatement::ShowEntities(_),
            )
            | (
                SqlCompiledCommandSurface::Update,
                SqlStatement::Insert(_) | SqlStatement::Update(_) | SqlStatement::Delete(_),
            ) => Ok(()),
            (SqlCompiledCommandSurface::Query, SqlStatement::Insert(_)) => {
                Err(QueryError::unsupported_query(
                    "execute_sql_query rejects INSERT; use execute_sql_update::<E>()",
                ))
            }
            (SqlCompiledCommandSurface::Query, SqlStatement::Update(_)) => {
                Err(QueryError::unsupported_query(
                    "execute_sql_query rejects UPDATE; use execute_sql_update::<E>()",
                ))
            }
            (SqlCompiledCommandSurface::Query, SqlStatement::Delete(_)) => {
                Err(QueryError::unsupported_query(
                    "execute_sql_query rejects DELETE; use execute_sql_update::<E>()",
                ))
            }
            (SqlCompiledCommandSurface::Update, SqlStatement::Select(_)) => {
                Err(QueryError::unsupported_query(
                    "execute_sql_update rejects SELECT; use execute_sql_query::<E>()",
                ))
            }
            (SqlCompiledCommandSurface::Update, SqlStatement::Explain(_)) => {
                Err(QueryError::unsupported_query(
                    "execute_sql_update rejects EXPLAIN; use execute_sql_query::<E>()",
                ))
            }
            (SqlCompiledCommandSurface::Update, SqlStatement::Describe(_)) => {
                Err(QueryError::unsupported_query(
                    "execute_sql_update rejects DESCRIBE; use execute_sql_query::<E>()",
                ))
            }
            (SqlCompiledCommandSurface::Update, SqlStatement::ShowIndexes(_)) => {
                Err(QueryError::unsupported_query(
                    "execute_sql_update rejects SHOW INDEXES; use execute_sql_query::<E>()",
                ))
            }
            (SqlCompiledCommandSurface::Update, SqlStatement::ShowColumns(_)) => {
                Err(QueryError::unsupported_query(
                    "execute_sql_update rejects SHOW COLUMNS; use execute_sql_query::<E>()",
                ))
            }
            (SqlCompiledCommandSurface::Update, SqlStatement::ShowEntities(_)) => {
                Err(QueryError::unsupported_query(
                    "execute_sql_update rejects SHOW ENTITIES; use execute_sql_query::<E>()",
                ))
            }
        }
    }

    /// Execute one single-entity reduced SQL query or introspection statement.
    ///
    /// This surface stays hard-bound to `E`, rejects state-changing SQL, and
    /// returns SQL-shaped statement output instead of typed entities.
    pub fn execute_sql_query<E>(&self, sql: &str) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let compiled = self.compile_sql_query::<E>(sql)?;

        self.execute_compiled_sql_owned::<E>(compiled)
    }

    /// Execute one reduced SQL query while reporting the compile/execute split
    /// at the top-level SQL seam.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn execute_sql_query_with_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<(SqlStatementResult, SqlQueryExecutionAttribution), QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        // Phase 1: measure the compile side of the new seam, including parse,
        // surface validation, and semantic command construction.
        let (compile_local_instructions, compiled) =
            measure_sql_stage(|| self.compile_sql_query_with_cache_attribution::<E>(sql));
        let (compiled, compile_cache_attribution, compile_phase_attribution) = compiled?;

        // Phase 2: measure the execute side separately so repeat-run cache
        // experiments can prove which side actually moved.
        let store_get_calls_before = DataStore::current_get_call_count();
        let pure_covering_decode_before = current_pure_covering_decode_local_instructions();
        let pure_covering_row_assembly_before =
            current_pure_covering_row_assembly_local_instructions();
        let (result, execute_cache_attribution, execute_phase_attribution) =
            self.execute_compiled_sql_with_phase_attribution::<E>(&compiled)?;
        let store_get_calls =
            DataStore::current_get_call_count().saturating_sub(store_get_calls_before);
        let pure_covering_decode_local_instructions =
            current_pure_covering_decode_local_instructions()
                .saturating_sub(pure_covering_decode_before);
        let pure_covering_row_assembly_local_instructions =
            current_pure_covering_row_assembly_local_instructions()
                .saturating_sub(pure_covering_row_assembly_before);
        let execute_local_instructions = execute_phase_attribution
            .planner_local_instructions
            .saturating_add(execute_phase_attribution.store_local_instructions)
            .saturating_add(execute_phase_attribution.executor_local_instructions)
            .saturating_add(execute_phase_attribution.response_finalization_local_instructions);
        let cache_attribution = compile_cache_attribution.merge(execute_cache_attribution);
        let total_local_instructions =
            compile_local_instructions.saturating_add(execute_local_instructions);
        let grouped = matches!(&result, SqlStatementResult::Grouped { .. }).then_some(
            GroupedExecutionAttribution {
                stream_local_instructions: execute_phase_attribution
                    .grouped_stream_local_instructions,
                fold_local_instructions: execute_phase_attribution.grouped_fold_local_instructions,
                finalize_local_instructions: execute_phase_attribution
                    .grouped_finalize_local_instructions,
                count: GroupedCountAttribution::from_executor(
                    execute_phase_attribution.grouped_count,
                ),
            },
        );
        let pure_covering = (pure_covering_decode_local_instructions > 0
            || pure_covering_row_assembly_local_instructions > 0)
            .then_some(SqlPureCoveringAttribution {
                decode_local_instructions: pure_covering_decode_local_instructions,
                row_assembly_local_instructions: pure_covering_row_assembly_local_instructions,
            });

        Ok((
            result,
            SqlQueryExecutionAttribution {
                compile_local_instructions,
                compile: SqlCompileAttribution {
                    cache_key_local_instructions: compile_phase_attribution.cache_key,
                    cache_lookup_local_instructions: compile_phase_attribution.cache_lookup,
                    parse_local_instructions: compile_phase_attribution.parse,
                    parse_tokenize_local_instructions: compile_phase_attribution.parse_tokenize,
                    parse_select_local_instructions: compile_phase_attribution.parse_select,
                    parse_expr_local_instructions: compile_phase_attribution.parse_expr,
                    parse_predicate_local_instructions: compile_phase_attribution.parse_predicate,
                    aggregate_lane_check_local_instructions: compile_phase_attribution
                        .aggregate_lane_check,
                    prepare_local_instructions: compile_phase_attribution.prepare,
                    lower_local_instructions: compile_phase_attribution.lower,
                    bind_local_instructions: compile_phase_attribution.bind,
                    cache_insert_local_instructions: compile_phase_attribution.cache_insert,
                },
                plan_lookup_local_instructions: execute_phase_attribution
                    .planner_local_instructions,
                execution: SqlExecutionAttribution {
                    planner_local_instructions: execute_phase_attribution
                        .planner_local_instructions,
                    store_local_instructions: execute_phase_attribution.store_local_instructions,
                    executor_invocation_local_instructions: execute_phase_attribution
                        .executor_invocation_local_instructions,
                    executor_local_instructions: execute_phase_attribution
                        .executor_local_instructions,
                    response_finalization_local_instructions: execute_phase_attribution
                        .response_finalization_local_instructions,
                },
                grouped,
                scalar_aggregate: SqlScalarAggregateAttribution::from_executor(
                    execute_phase_attribution.scalar_aggregate_terminal,
                ),
                pure_covering,
                store_get_calls,
                response_decode_local_instructions: 0,
                execute_local_instructions,
                total_local_instructions,
                cache: SqlQueryCacheAttribution {
                    sql_compiled_command_hits: cache_attribution.sql_compiled_command_cache_hits,
                    sql_compiled_command_misses: cache_attribution
                        .sql_compiled_command_cache_misses,
                    shared_query_plan_hits: cache_attribution.shared_query_plan_cache_hits,
                    shared_query_plan_misses: cache_attribution.shared_query_plan_cache_misses,
                },
            },
        ))
    }

    /// Execute one single-entity reduced SQL mutation statement.
    ///
    /// This surface stays hard-bound to `E`, rejects read-only SQL, and
    /// returns SQL-shaped mutation output such as counts or `RETURNING` rows.
    pub fn execute_sql_update<E>(&self, sql: &str) -> Result<SqlStatementResult, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let compiled = self.compile_sql_update::<E>(sql)?;

        self.execute_compiled_sql_owned::<E>(compiled)
    }

    // Compile one SQL query-surface string into the session-owned generic-free
    // semantic command artifact before execution.
    pub(in crate::db) fn compile_sql_query<E>(
        &self,
        sql: &str,
    ) -> Result<CompiledSqlCommand, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.compile_sql_query_with_cache_attribution::<E>(sql)
            .map(|(compiled, _, _)| compiled)
    }

    fn compile_sql_query_with_cache_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<
        (
            CompiledSqlCommand,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.compile_sql_surface_with_cache_attribution::<E>(sql, SqlCompiledCommandSurface::Query)
    }

    // Compile one SQL update-surface string into the session-owned generic-free
    // semantic command artifact before execution.
    pub(in crate::db) fn compile_sql_update<E>(
        &self,
        sql: &str,
    ) -> Result<CompiledSqlCommand, QueryError>
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.compile_sql_update_with_cache_attribution::<E>(sql)
            .map(|(compiled, _, _)| compiled)
    }

    fn compile_sql_update_with_cache_attribution<E>(
        &self,
        sql: &str,
    ) -> Result<
        (
            CompiledSqlCommand,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        self.compile_sql_surface_with_cache_attribution::<E>(sql, SqlCompiledCommandSurface::Update)
    }

    // Reuse one internal compile shell for both outward SQL surfaces so query
    // and update no longer duplicate cache-key construction and surface
    // validation plumbing before they reach the real compile/cache owner.
    fn compile_sql_surface_with_cache_attribution<E>(
        &self,
        sql: &str,
        surface: SqlCompiledCommandSurface,
    ) -> Result<
        (
            CompiledSqlCommand,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (cache_key_local_instructions, cache_key) = measured(|| {
            Ok::<_, QueryError>(SqlCompiledCommandCacheKey::for_entity::<E>(surface, sql))
        })?;
        let mut attribution = SqlCompileAttributionBuilder::default();
        attribution.record_cache_key(cache_key_local_instructions);

        self.compile_sql_statement_with_cache::<E>(cache_key, attribution, sql, surface)
    }

    // Reuse one previously compiled SQL artifact when the session-local cache
    // can prove the surface, entity contract, and raw SQL text all match.
    fn compile_sql_statement_with_cache<E>(
        &self,
        cache_key: SqlCompiledCommandCacheKey,
        mut attribution: SqlCompileAttributionBuilder,
        sql: &str,
        surface: SqlCompiledCommandSurface,
    ) -> Result<
        (
            CompiledSqlCommand,
            SqlCacheAttribution,
            SqlCompilePhaseAttribution,
        ),
        QueryError,
    >
    where
        E: PersistedRow<Canister = C> + EntityValue,
    {
        let (cache_lookup_local_instructions, cached) = measured(|| {
            let cached =
                self.with_sql_compiled_command_cache(|cache| cache.get(&cache_key).cloned());
            Ok::<_, QueryError>(cached)
        })?;
        attribution.record_cache_lookup(cache_lookup_local_instructions);
        if let Some(compiled) = cached {
            return Ok((
                compiled,
                SqlCacheAttribution::sql_compiled_command_cache_hit(),
                attribution.finish(),
            ));
        }

        let (parse_local_instructions, (parsed, parse_attribution)) =
            measured(|| parse_sql_with_attribution(sql).map_err(QueryError::from_sql_parse_error))?;
        attribution.record_parse(parse_local_instructions, parse_attribution);
        let authority = EntityAuthority::for_type::<E>();
        let (artifacts, compile_attribution) =
            Self::compile_sql_statement_measured(&parsed, surface, authority, cache_key.clone())?;
        attribution.record_core_compile(compile_attribution);
        let compiled = artifacts.command;

        let (cache_insert_local_instructions, ()) = measured(|| {
            self.with_sql_compiled_command_cache(|cache| {
                cache.insert(cache_key, compiled.clone());
            });
            Ok::<_, QueryError>(())
        })?;
        attribution.record_cache_insert(cache_insert_local_instructions);

        Ok((
            compiled,
            SqlCacheAttribution::sql_compiled_command_cache_miss(),
            attribution.finish(),
        ))
    }
}
