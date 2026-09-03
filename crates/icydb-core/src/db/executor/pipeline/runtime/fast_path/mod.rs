//! Module: executor::pipeline::runtime::fast_path
//! Responsibility: fast-path decision and fallback key-stream resolution policy.
//! Does not own: page materialization.
//! Boundary: internal helper boundary for `executor::pipeline::runtime`.

mod strategy;

use crate::{
    db::{
        executor::{
            AccessStreamExecutionPolicy, ExecutionRoutePlan,
            pipeline::{contracts::ResolvedExecutionKeyStream, runtime::ExecutionAttemptKernel},
        },
        index::{
            IndexCompilePolicy, compile_index_program, compile_index_program_for_targets,
            predicate::{IndexPredicateExecution, IndexPredicateProgram},
        },
    },
    error::InternalError,
};

use crate::db::executor::pipeline::runtime::fast_path::strategy::FastPathResolutionStrategy;

///
/// ResolvedIndexPredicateProgram
///
///
/// ResolvedIndexPredicateProgram keeps fast-path index predicate preparation
/// behind one explicit execution-input-owned boundary.
/// It carries either one borrowed precompiled program or one owned on-demand
/// compile so later resolution code only asks whether a program is available.
///

enum ResolvedIndexPredicateProgram<'a> {
    None,
    Borrowed(&'a IndexPredicateProgram),
    Owned(IndexPredicateProgram),
}

impl<'a> ResolvedIndexPredicateProgram<'a> {
    // Borrow the resolved program without exposing whether it was borrowed or
    // compiled on demand.
    const fn execution(&'a self) -> Option<IndexPredicateExecution<'a>> {
        Some(match self {
            Self::None => return None,
            Self::Borrowed(program) => program,
            Self::Owned(program) => program,
        })
    }
}

///
/// FastPathResolutionContext
///
///
/// FastPathResolutionContext freezes the execution state shared by fast-path
/// hit handling and canonical fallback stream resolution.
/// It keeps route-owned fallback hints and index-predicate execution in one
/// place so stream resolution no longer rethreads them through sibling helpers.
///

struct FastPathResolutionContext<'a, 'b> {
    kernel: &'a ExecutionAttemptKernel<'a>,
    route_plan: &'b ExecutionRoutePlan,
    index_predicate_execution: Option<IndexPredicateExecution<'a>>,
}

impl<'a, 'b> FastPathResolutionContext<'a, 'b> {
    // Build one owner-local resolution context for a single fast-path or
    // fallback key-stream decision.
    const fn new(
        kernel: &'a ExecutionAttemptKernel<'a>,
        route_plan: &'b ExecutionRoutePlan,
        index_predicate_execution: Option<IndexPredicateExecution<'a>>,
    ) -> Self {
        Self {
            kernel,
            route_plan,
            index_predicate_execution,
        }
    }

    // Resolve one canonical key stream from either a fast-path hit or the
    // fallback stream owned by this execution context.
    fn resolve_from_decision(
        &self,
        fast_path_decision: Option<crate::db::executor::pipeline::contracts::FastPathKeyResult>,
    ) -> Result<ResolvedExecutionKeyStream, InternalError> {
        match fast_path_decision {
            Some(fast) => Ok(ResolvedExecutionKeyStream::new(
                fast.ordered_key_stream,
                fast.rows_scanned,
            )),
            None => self.resolve_fallback_execution_key_stream(),
        }
    }

    // Resolve canonical fallback access stream when no fast path produced rows.
    fn resolve_fallback_execution_key_stream(
        &self,
    ) -> Result<ResolvedExecutionKeyStream, InternalError> {
        let fallback_fetch_hint = self
            .route_plan
            .fallback_physical_fetch_hint(self.kernel.inputs.stream_bindings().direction());
        let execution_policy = AccessStreamExecutionPolicy::new(
            fallback_fetch_hint,
            self.route_plan.index_leaf_order_policy(),
        );
        let key_stream = self
            .kernel
            .inputs
            .runtime()
            .resolve_fallback_execution_key_stream(
                self.kernel.inputs.executable_access().clone(),
                *self.kernel.inputs.stream_bindings(),
                execution_policy,
                self.index_predicate_execution,
            )?;

        Ok(ResolvedExecutionKeyStream::new(key_stream, None))
    }
}

impl ExecutionAttemptKernel<'_> {
    // Resolve the index predicate program for one execution attempt, reusing
    // the prepared mode when available and compiling on demand only when needed.
    fn resolve_index_predicate_program(
        &self,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> ResolvedIndexPredicateProgram<'_> {
        match predicate_compile_mode {
            IndexCompilePolicy::ConservativeSubset => self
                .inputs
                .execution_preparation()
                .conservative_mode()
                .map_or_else(
                    || self.compile_index_predicate_program(predicate_compile_mode),
                    ResolvedIndexPredicateProgram::Borrowed,
                ),
            IndexCompilePolicy::StrictAllOrNone => self
                .inputs
                .execution_preparation()
                .strict_mode()
                .map_or_else(
                    || self.compile_index_predicate_program(predicate_compile_mode),
                    ResolvedIndexPredicateProgram::Borrowed,
                ),
        }
    }

    // Compile one index predicate program only when execution preparation did
    // not already freeze the requested compile mode.
    fn compile_index_predicate_program(
        &self,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> ResolvedIndexPredicateProgram<'_> {
        let Some(compiled_predicate) = self.inputs.execution_preparation().compiled_predicate()
        else {
            return ResolvedIndexPredicateProgram::None;
        };

        let compiled_index_predicate =
            if let Some(compile_targets) = self.inputs.execution_preparation().compile_targets() {
                compile_index_program_for_targets(
                    compiled_predicate.executable(),
                    compile_targets,
                    predicate_compile_mode,
                )
            } else {
                let Some(slot_map) = self.inputs.execution_preparation().slot_map() else {
                    return ResolvedIndexPredicateProgram::None;
                };

                compile_index_program(
                    compiled_predicate.executable(),
                    slot_map,
                    predicate_compile_mode,
                )
            };

        compiled_index_predicate.map_or(
            ResolvedIndexPredicateProgram::None,
            ResolvedIndexPredicateProgram::Owned,
        )
    }

    /// Resolve one canonical execution key stream in fast-path precedence order.
    ///
    /// This is the single shared load key-stream resolver boundary.
    pub(in crate::db::executor) fn resolve_execution_key_stream_without_distinct(
        &self,
        route_plan: &ExecutionRoutePlan,
        predicate_compile_mode: IndexCompilePolicy,
    ) -> Result<ResolvedExecutionKeyStream, InternalError> {
        // Phase 0: reuse precompiled runtime index predicates when the
        // execution-preparation boundary already owns the requested mode, and
        // only fall back to one on-demand compile when it does not.
        let index_predicate_program = self.resolve_index_predicate_program(predicate_compile_mode);
        let index_predicate_execution = index_predicate_program.execution();

        // Phase 1: select fast-path resolution strategy once from route shape.
        let fast_path_strategy = FastPathResolutionStrategy::for_route(route_plan);
        let fast_path_decision = fast_path_strategy.resolve_fast_path_decision(
            self.inputs,
            route_plan,
            index_predicate_execution,
        )?;

        // Phase 2: materialize from fast-path hit or canonical fallback stream.
        let resolution =
            FastPathResolutionContext::new(self, route_plan, index_predicate_execution);

        resolution.resolve_from_decision(fast_path_decision)
    }
}
