//! Module: query::fluent::load::terminals::aggregate
//! Responsibility: fluent aggregate terminal public methods.
//! Does not own: descriptor driver wiring, output projection helpers, or executor routing.
//! Boundary: delegates guard, slot, execute, attribution, and EXPLAIN handoffs to `support`.

#[cfg(feature = "diagnostics")]
use crate::db::FluentTerminalExecutionAttribution;
use crate::db::query::read_intent::ReadIntentKind;
use crate::{
    db::{
        PersistedRow,
        query::{
            builder::{
                AvgBySlotTerminal, AvgDistinctBySlotTerminal, CountDistinctBySlotTerminal,
                CountRowsTerminal, MaxIdBySlotTerminal, MaxIdTerminal, MedianIdBySlotTerminal,
                MinIdBySlotTerminal, MinIdTerminal, MinMaxIdBySlotTerminal, NthIdBySlotTerminal,
                SumBySlotTerminal, SumDistinctBySlotTerminal,
            },
            explain::{ExplainAggregateTerminalPlan, ExplainExecutionNodeDescriptor},
            fluent::load::FluentLoadQuery,
            intent::{IntentError, QueryError},
        },
    },
    traits::EntityValue,
    types::{Decimal, Id},
};

use super::driver::MinMaxByIds;

impl<E> FluentLoadQuery<'_, E>
where
    E: PersistedRow,
{
    /// Execute and return the number of matching rows.
    pub fn count(&self) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        self.execute_terminal(CountRowsTerminal::new())
    }

    /// Execute and return the exact number of matching rows.
    ///
    /// Unlike `count()`, this semantic aggregate rejects a prior raw
    /// `limit(...)` so exact counts cannot accidentally mean "count the first
    /// N rows."
    pub fn count_exact(&self) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        self.execute_exact_aggregate_terminal(
            IntentError::raw_limit_before_count_exact_terminal(),
            CountRowsTerminal::new(),
        )
    }

    /// Explain exact count routing without executing the terminal.
    pub fn explain_count_exact(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_checked_exact_aggregate_terminal(
            IntentError::raw_limit_before_count_exact_terminal(),
            &CountRowsTerminal::new(),
        )
    }

    /// Execute and return the number of matching rows with terminal attribution.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn count_with_attribution(
        &self,
    ) -> Result<(u32, FluentTerminalExecutionAttribution), QueryError>
    where
        E: EntityValue,
    {
        self.execute_count_terminal_with_attribution(ReadIntentKind::BoundedRowWindow)
    }

    /// Execute and return the exact number of matching rows with terminal attribution.
    #[cfg(feature = "diagnostics")]
    #[doc(hidden)]
    pub fn count_exact_with_attribution(
        &self,
    ) -> Result<(u32, FluentTerminalExecutionAttribution), QueryError>
    where
        E: EntityValue,
    {
        self.ensure_exact_aggregate_intent_owns_limit(
            IntentError::raw_limit_before_count_exact_terminal(),
        )?;

        self.execute_count_terminal_with_attribution(ReadIntentKind::ExactAggregate)
    }

    /// Execute and return the smallest matching identifier, if any.
    pub fn min(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_terminal(MinIdTerminal::new())
    }

    /// Execute and return the exact smallest matching identifier.
    ///
    /// Unlike `min()`, this semantic aggregate rejects a prior raw
    /// `limit(...)` so exact minimum selection cannot accidentally mean
    /// "minimum over the first N rows."
    pub fn min_id_exact(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_exact_aggregate_terminal(
            IntentError::raw_limit_before_min_exact_terminal(),
            MinIdTerminal::new(),
        )
    }

    /// Explain scalar `min()` routing without executing the terminal.
    pub fn explain_min(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_terminal(&MinIdTerminal::new())
    }

    /// Explain exact `min_id_exact()` routing without executing the terminal.
    pub fn explain_min_id_exact(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_checked_exact_aggregate_terminal(
            IntentError::raw_limit_before_min_exact_terminal(),
            &MinIdTerminal::new(),
        )
    }

    /// Execute and return the id of the row with the smallest value for `field`.
    ///
    /// Ties are deterministic: equal field values resolve by primary key ascending.
    pub fn min_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, MinIdBySlotTerminal::new)
    }

    /// Execute and return the id of the row with the exact minimum `field` value.
    ///
    /// Ties are deterministic: equal field values resolve by primary key ascending.
    /// A prior row-window cap is rejected because the terminal owns the exact
    /// aggregate intent.
    pub fn min_exact_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_exact_aggregate_by_slot_terminal(
            field,
            IntentError::raw_limit_before_min_exact_terminal(),
            MinIdBySlotTerminal::new,
        )
    }

    /// Explain exact `min_exact_by(field)` routing without executing the terminal.
    pub fn explain_min_exact_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_exact_aggregate_by_slot_terminal(
            field,
            IntentError::raw_limit_before_min_exact_terminal(),
            MinIdBySlotTerminal::new,
        )
    }

    /// Execute and return the largest matching identifier, if any.
    pub fn max(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_terminal(MaxIdTerminal::new())
    }

    /// Execute and return the exact largest matching identifier.
    ///
    /// Unlike `max()`, this semantic aggregate rejects a prior row-window cap
    /// so exact maximum selection cannot accidentally mean
    /// "maximum over the first N rows."
    pub fn max_id_exact(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_exact_aggregate_terminal(
            IntentError::raw_limit_before_max_exact_terminal(),
            MaxIdTerminal::new(),
        )
    }

    /// Explain scalar `max()` routing without executing the terminal.
    pub fn explain_max(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_terminal(&MaxIdTerminal::new())
    }

    /// Explain exact `max_id_exact()` routing without executing the terminal.
    pub fn explain_max_id_exact(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_checked_exact_aggregate_terminal(
            IntentError::raw_limit_before_max_exact_terminal(),
            &MaxIdTerminal::new(),
        )
    }

    /// Execute and return the id of the row with the largest value for `field`.
    ///
    /// Ties are deterministic: equal field values resolve by primary key ascending.
    pub fn max_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, MaxIdBySlotTerminal::new)
    }

    /// Execute and return the id of the row with the exact maximum `field` value.
    ///
    /// Ties are deterministic: equal field values resolve by primary key ascending.
    /// A prior row-window cap is rejected because the terminal owns the exact
    /// aggregate intent.
    pub fn max_exact_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_exact_aggregate_by_slot_terminal(
            field,
            IntentError::raw_limit_before_max_exact_terminal(),
            MaxIdBySlotTerminal::new,
        )
    }

    /// Explain exact `max_exact_by(field)` routing without executing the terminal.
    pub fn explain_max_exact_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_exact_aggregate_by_slot_terminal(
            field,
            IntentError::raw_limit_before_max_exact_terminal(),
            MaxIdBySlotTerminal::new,
        )
    }

    /// Execute and return the id at zero-based ordinal `nth` when rows are
    /// ordered by `field` ascending, with primary-key ascending tie-breaks.
    pub fn nth_by(&self, field: impl AsRef<str>, nth: usize) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, |target_slot| {
            NthIdBySlotTerminal::new(target_slot, nth)
        })
    }

    /// Execute and return the sum of `field` over matching rows.
    pub fn sum_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, SumBySlotTerminal::new)
    }

    /// Execute and return the exact sum of `field` over matching rows.
    ///
    /// Unlike `sum_by(...)`, this semantic aggregate rejects a prior raw
    /// `limit(...)` so exact sums cannot accidentally mean "sum the first N
    /// rows."
    pub fn sum_exact(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_exact_aggregate_by_slot_terminal(
            field,
            IntentError::raw_limit_before_sum_exact_terminal(),
            SumBySlotTerminal::new,
        )
    }

    /// Explain exact sum routing without executing the terminal.
    pub fn explain_sum_exact(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_exact_aggregate_by_slot_terminal(
            field,
            IntentError::raw_limit_before_sum_exact_terminal(),
            SumBySlotTerminal::new,
        )
    }

    /// Explain scalar `sum_by(field)` routing without executing the terminal.
    pub fn explain_sum_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_slot_terminal(field, SumBySlotTerminal::new)
    }

    /// Execute and return the sum of distinct `field` values.
    pub fn sum_distinct_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, SumDistinctBySlotTerminal::new)
    }

    /// Explain scalar `sum(distinct field)` routing without executing the terminal.
    pub fn explain_sum_distinct_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_slot_terminal(field, SumDistinctBySlotTerminal::new)
    }

    /// Execute and return the average of `field` over matching rows.
    pub fn avg_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, AvgBySlotTerminal::new)
    }

    /// Execute and return the exact average of `field` over matching rows.
    ///
    /// Unlike `avg_by(...)`, this semantic aggregate rejects a prior raw
    /// `limit(...)` so exact averages cannot accidentally mean "average the
    /// first N rows."
    pub fn avg_exact(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_exact_aggregate_by_slot_terminal(
            field,
            IntentError::raw_limit_before_avg_exact_terminal(),
            AvgBySlotTerminal::new,
        )
    }

    /// Explain scalar `avg_by(field)` routing without executing the terminal.
    pub fn explain_avg_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_slot_terminal(field, AvgBySlotTerminal::new)
    }

    /// Explain exact `avg_exact(field)` routing without executing the terminal.
    pub fn explain_avg_exact(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_exact_aggregate_by_slot_terminal(
            field,
            IntentError::raw_limit_before_avg_exact_terminal(),
            AvgBySlotTerminal::new,
        )
    }

    /// Execute and return the average of distinct `field` values.
    pub fn avg_distinct_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, AvgDistinctBySlotTerminal::new)
    }

    /// Explain scalar `avg(distinct field)` routing without executing the terminal.
    pub fn explain_avg_distinct_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.explain_slot_terminal(field, AvgDistinctBySlotTerminal::new)
    }

    /// Execute and return the median id by `field` using deterministic ordering
    /// `(field asc, primary key asc)`.
    ///
    /// Even-length windows select the lower median.
    pub fn median_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, MedianIdBySlotTerminal::new)
    }

    /// Execute and return the number of distinct values for `field` over the
    /// effective result window.
    pub fn count_distinct_by(&self, field: impl AsRef<str>) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, CountDistinctBySlotTerminal::new)
    }

    /// Explain `count_distinct_by(field)` routing without executing the terminal.
    pub fn explain_count_distinct_by(
        &self,
        field: impl AsRef<str>,
    ) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.explain_slot_terminal(field, CountDistinctBySlotTerminal::new)
    }

    /// Execute and return both `(min_by(field), max_by(field))` in one terminal.
    ///
    /// Tie handling is deterministic for both extrema: primary key ascending.
    pub fn min_max_by(&self, field: impl AsRef<str>) -> Result<MinMaxByIds<E>, QueryError>
    where
        E: EntityValue,
    {
        self.execute_slot_terminal(field, MinMaxIdBySlotTerminal::new)
    }
}
