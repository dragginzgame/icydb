//! Module: db::session::sql::execute::diagnostics
//! Responsibility: diagnostics-only SQL execution attribution helpers.
//! Does not own: SQL command routing or executor runtime semantics.
//! Boundary: keeps measurement scaffolding out of the root SQL execution shell.

#[cfg(feature = "diagnostics")]
use crate::db::physical_access::with_physical_access_attribution;
#[cfg(feature = "diagnostics")]
use crate::db::session::sql::measure_sql_stage;
use crate::{
    db::{
        DbSession, QueryError, executor::StructuralGroupedProjectionResult,
        session::sql::SqlStatementResult,
    },
    traits::CanisterKind,
};

#[cfg(feature = "diagnostics")]
pub(super) fn measure_execute_phase_with_physical_access<T, E>(
    run: impl FnOnce() -> Result<T, E>,
) -> ((u64, u64), Result<T, E>) {
    let (store_local_instructions, (execute_local_instructions, result)) =
        with_physical_access_attribution(|| measure_sql_stage(run));

    (
        (execute_local_instructions, store_local_instructions),
        result,
    )
}

///
/// GroupedSqlDiagnosticsCollector
///
/// GroupedSqlDiagnosticsCollector carries the diagnostics-only response
/// finalization counter through the shared grouped SQL execution core.
/// Normal execution passes no collector, so the response path remains the
/// direct statement-result finalizer used outside diagnostics builds.
///

pub(super) struct GroupedSqlDiagnosticsCollector<'a> {
    #[cfg(feature = "diagnostics")]
    response_finalization_local_instructions: &'a mut u64,
    #[cfg(not(feature = "diagnostics"))]
    _marker: std::marker::PhantomData<&'a mut u64>,
}

impl GroupedSqlDiagnosticsCollector<'_> {
    // Build one diagnostics collector over the caller-owned response counter.
    #[cfg(feature = "diagnostics")]
    pub(super) const fn new(
        response_finalization_local_instructions: &mut u64,
    ) -> GroupedSqlDiagnosticsCollector<'_> {
        GroupedSqlDiagnosticsCollector {
            response_finalization_local_instructions,
        }
    }

    // Finalize a grouped SQL result while recording diagnostics-only response
    // attribution when diagnostics are enabled.
    pub(super) fn finalize_grouped_sql_statement<C: CanisterKind>(
        self,
        columns: Vec<String>,
        fixed_scales: Vec<Option<u32>>,
        result: StructuralGroupedProjectionResult,
    ) -> Result<SqlStatementResult, QueryError> {
        #[cfg(feature = "diagnostics")]
        {
            let (response_finalization_local_instructions, statement_result) =
                measure_sql_stage(|| {
                    DbSession::<C>::grouped_sql_statement_result_from_result(
                        columns,
                        fixed_scales,
                        result,
                    )
                });
            *self.response_finalization_local_instructions =
                response_finalization_local_instructions;

            statement_result
        }

        #[cfg(not(feature = "diagnostics"))]
        {
            let _ = self;
            DbSession::<C>::grouped_sql_statement_result_from_result(columns, fixed_scales, result)
        }
    }
}
