use crate::{
    db::executor::{
        ExecutablePlan, ExecutionKernel,
        aggregate::{AggregateKind, AggregateOutput, AggregateSpec},
        load::LoadExecutor,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
};

impl<E> LoadExecutor<E>
where
    E: EntityKind + EntityValue,
{
    pub(in crate::db) fn aggregate_count(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<u32, InternalError> {
        match ExecutionKernel::execute_aggregate_spec(
            self,
            plan,
            AggregateSpec::for_terminal(AggregateKind::Count),
        )? {
            AggregateOutput::Count(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate COUNT result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_exists(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<bool, InternalError> {
        match ExecutionKernel::execute_aggregate_spec(
            self,
            plan,
            AggregateSpec::for_terminal(AggregateKind::Exists),
        )? {
            AggregateOutput::Exists(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate EXISTS result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_min(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match ExecutionKernel::execute_aggregate_spec(
            self,
            plan,
            AggregateSpec::for_terminal(AggregateKind::Min),
        )? {
            AggregateOutput::Min(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate MIN result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_max(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match ExecutionKernel::execute_aggregate_spec(
            self,
            plan,
            AggregateSpec::for_terminal(AggregateKind::Max),
        )? {
            AggregateOutput::Max(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate MAX result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_min_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<Option<Id<E>>, InternalError> {
        let target_field = target_field.into();
        match ExecutionKernel::execute_aggregate_spec(
            self,
            plan,
            AggregateSpec::for_target_field(AggregateKind::Min, target_field),
        )? {
            AggregateOutput::Min(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate MIN(field) result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_max_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<Option<Id<E>>, InternalError> {
        let target_field = target_field.into();
        match ExecutionKernel::execute_aggregate_spec(
            self,
            plan,
            AggregateSpec::for_target_field(AggregateKind::Max, target_field),
        )? {
            AggregateOutput::Max(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate MAX(field) result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_nth_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
        nth: usize,
    ) -> Result<Option<Id<E>>, InternalError> {
        let target_field = target_field.into();

        self.execute_nth_field_aggregate(plan, target_field.as_str(), nth)
    }

    pub(in crate::db) fn aggregate_median_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<Option<Id<E>>, InternalError> {
        let target_field = target_field.into();

        self.execute_median_field_aggregate(plan, target_field.as_str())
    }

    #[expect(clippy::type_complexity)]
    pub(in crate::db) fn aggregate_min_max_by(
        &self,
        plan: ExecutablePlan<E>,
        target_field: impl Into<String>,
    ) -> Result<Option<(Id<E>, Id<E>)>, InternalError> {
        let target_field = target_field.into();

        self.execute_min_max_field_aggregate(plan, target_field.as_str())
    }

    pub(in crate::db) fn aggregate_first(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match ExecutionKernel::execute_aggregate_spec(
            self,
            plan,
            AggregateSpec::for_terminal(AggregateKind::First),
        )? {
            AggregateOutput::First(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate FIRST result kind mismatch",
            )),
        }
    }

    pub(in crate::db) fn aggregate_last(
        &self,
        plan: ExecutablePlan<E>,
    ) -> Result<Option<Id<E>>, InternalError> {
        match ExecutionKernel::execute_aggregate_spec(
            self,
            plan,
            AggregateSpec::for_terminal(AggregateKind::Last),
        )? {
            AggregateOutput::Last(value) => Ok(value),
            _ => Err(InternalError::query_executor_invariant(
                "aggregate LAST result kind mismatch",
            )),
        }
    }
}
