use crate::{
    db::{
        query::fluent::load::FluentLoadQuery,
        query::{
            api::ResponseCardinalityExt,
            builder::aggregate::{exists, first, last, max, min},
            explain::{ExplainAggregateTerminalPlan, ExplainExecutionNodeDescriptor},
            intent::QueryError,
        },
        response::EntityResponse,
    },
    traits::{EntityKind, EntityValue},
    types::{Decimal, Id},
    value::Value,
};

type MinMaxByIds<E> = Option<(Id<E>, Id<E>)>;

impl<E> FluentLoadQuery<'_, E>
where
    E: EntityKind,
{
    // ------------------------------------------------------------------
    // Execution (single semantic boundary)
    // ------------------------------------------------------------------

    /// Execute this query using the session's policy settings.
    pub fn execute(&self) -> Result<EntityResponse<E>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session.execute_query(self.query())
    }

    // ------------------------------------------------------------------
    // Execution terminals — semantic only
    // ------------------------------------------------------------------

    /// Execute and return whether the result set is empty.
    pub fn is_empty(&self) -> Result<bool, QueryError>
    where
        E: EntityValue,
    {
        Ok(!self.exists()?)
    }

    /// Execute and return whether at least one matching row exists.
    pub fn exists(&self) -> Result<bool, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| load.aggregate_exists(plan))
    }

    /// Explain scalar `exists()` routing without executing the terminal.
    pub fn explain_exists(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        crate::db::DbSession::<E::Canister>::explain_load_query_terminal_with(
            self.query(),
            exists(),
        )
    }

    /// Explain scalar load execution shape without executing the query.
    pub fn explain_execution(&self) -> Result<ExplainExecutionNodeDescriptor, QueryError>
    where
        E: EntityValue,
    {
        self.query().explain_execution()
    }

    /// Execute and return the number of matching rows.
    pub fn count(&self) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| load.aggregate_count(plan))
    }

    /// Execute and return the smallest matching identifier, if any.
    pub fn min(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| load.aggregate_min(plan))
    }

    /// Explain scalar `min()` routing without executing the terminal.
    pub fn explain_min(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        crate::db::DbSession::<E::Canister>::explain_load_query_terminal_with(self.query(), min())
    }

    /// Execute and return the id of the row with the smallest value for `field`.
    ///
    /// Ties are deterministic: equal field values resolve by primary key ascending.
    pub fn min_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.aggregate_min_by_slot(plan, target_slot)
                })
        })
    }

    /// Execute and return the largest matching identifier, if any.
    pub fn max(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| load.aggregate_max(plan))
    }

    /// Explain scalar `max()` routing without executing the terminal.
    pub fn explain_max(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        crate::db::DbSession::<E::Canister>::explain_load_query_terminal_with(self.query(), max())
    }

    /// Execute and return the id of the row with the largest value for `field`.
    ///
    /// Ties are deterministic: equal field values resolve by primary key ascending.
    pub fn max_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.aggregate_max_by_slot(plan, target_slot)
                })
        })
    }

    /// Execute and return the id at zero-based ordinal `nth` when rows are
    /// ordered by `field` ascending, with primary-key ascending tie-breaks.
    pub fn nth_by(&self, field: impl AsRef<str>, nth: usize) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.aggregate_nth_by_slot(plan, target_slot, nth)
                })
        })
    }

    /// Execute and return the sum of `field` over matching rows.
    pub fn sum_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.aggregate_sum_by_slot(plan, target_slot)
                })
        })
    }

    /// Execute and return the sum of distinct `field` values.
    pub fn sum_distinct_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.aggregate_sum_distinct_by_slot(plan, target_slot)
                })
        })
    }

    /// Execute and return the average of `field` over matching rows.
    pub fn avg_by(&self, field: impl AsRef<str>) -> Result<Option<Decimal>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.aggregate_avg_by_slot(plan, target_slot)
                })
        })
    }

    /// Execute and return the median id by `field` using deterministic ordering
    /// `(field asc, primary key asc)`.
    ///
    /// Even-length windows select the lower median.
    pub fn median_by(&self, field: impl AsRef<str>) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.aggregate_median_by_slot(plan, target_slot)
                })
        })
    }

    /// Execute and return the number of distinct values for `field` over the
    /// effective result window.
    pub fn count_distinct_by(&self, field: impl AsRef<str>) -> Result<u32, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.aggregate_count_distinct_by_slot(plan, target_slot)
                })
        })
    }

    /// Execute and return both `(min_by(field), max_by(field))` in one terminal.
    ///
    /// Tie handling is deterministic for both extrema: primary key ascending.
    pub fn min_max_by(&self, field: impl AsRef<str>) -> Result<MinMaxByIds<E>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.aggregate_min_max_by_slot(plan, target_slot)
                })
        })
    }

    /// Execute and return projected field values for the effective result window.
    pub fn values_by(&self, field: impl AsRef<str>) -> Result<Vec<Value>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.values_by_slot(plan, target_slot)
                })
        })
    }

    /// Execute and return the first `k` rows from the effective response window.
    pub fn take(&self, take_count: u32) -> Result<EntityResponse<E>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| load.take(plan, take_count))
    }

    /// Execute and return the top `k` rows by `field` under deterministic
    /// ordering `(field desc, primary_key asc)` over the effective response
    /// window.
    ///
    /// This terminal applies its own ordering and does not preserve query
    /// `order_by(...)` row order in the returned rows. For `k = 1`, this
    /// matches `max_by(field)` selection semantics.
    pub fn top_k_by(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<EntityResponse<E>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.top_k_by_slot(plan, target_slot, take_count)
                })
        })
    }

    /// Execute and return the bottom `k` rows by `field` under deterministic
    /// ordering `(field asc, primary_key asc)` over the effective response
    /// window.
    ///
    /// This terminal applies its own ordering and does not preserve query
    /// `order_by(...)` row order in the returned rows. For `k = 1`, this
    /// matches `min_by(field)` selection semantics.
    pub fn bottom_k_by(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<EntityResponse<E>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.bottom_k_by_slot(plan, target_slot, take_count)
                })
        })
    }

    /// Execute and return projected values for the top `k` rows by `field`
    /// under deterministic ordering `(field desc, primary_key asc)` over the
    /// effective response window.
    ///
    /// Ranking is applied before projection and does not preserve query
    /// `order_by(...)` row order in the returned values. For `k = 1`, this
    /// matches `max_by(field)` projected to one value.
    pub fn top_k_by_values(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<Value>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.top_k_by_values_slot(plan, target_slot, take_count)
                })
        })
    }

    /// Execute and return projected values for the bottom `k` rows by `field`
    /// under deterministic ordering `(field asc, primary_key asc)` over the
    /// effective response window.
    ///
    /// Ranking is applied before projection and does not preserve query
    /// `order_by(...)` row order in the returned values. For `k = 1`, this
    /// matches `min_by(field)` projected to one value.
    pub fn bottom_k_by_values(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<Value>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.bottom_k_by_values_slot(plan, target_slot, take_count)
                })
        })
    }

    /// Execute and return projected id/value pairs for the top `k` rows by
    /// `field` under deterministic ordering `(field desc, primary_key asc)`
    /// over the effective response window.
    ///
    /// Ranking is applied before projection and does not preserve query
    /// `order_by(...)` row order in the returned values. For `k = 1`, this
    /// matches `max_by(field)` projected to one `(id, value)` pair.
    pub fn top_k_by_with_ids(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.top_k_by_with_ids_slot(plan, target_slot, take_count)
                })
        })
    }

    /// Execute and return projected id/value pairs for the bottom `k` rows by
    /// `field` under deterministic ordering `(field asc, primary_key asc)`
    /// over the effective response window.
    ///
    /// Ranking is applied before projection and does not preserve query
    /// `order_by(...)` row order in the returned values. For `k = 1`, this
    /// matches `min_by(field)` projected to one `(id, value)` pair.
    pub fn bottom_k_by_with_ids(
        &self,
        field: impl AsRef<str>,
        take_count: u32,
    ) -> Result<Vec<(Id<E>, Value)>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.bottom_k_by_with_ids_slot(plan, target_slot, take_count)
                })
        })
    }

    /// Execute and return distinct projected field values for the effective
    /// result window, preserving first-observed value order.
    pub fn distinct_values_by(&self, field: impl AsRef<str>) -> Result<Vec<Value>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.distinct_values_by_slot(plan, target_slot)
                })
        })
    }

    /// Execute and return projected field values paired with row ids for the
    /// effective result window.
    pub fn values_by_with_ids(
        &self,
        field: impl AsRef<str>,
    ) -> Result<Vec<(Id<E>, Value)>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.values_by_with_ids_slot(plan, target_slot)
                })
        })
    }

    /// Execute and return the first projected field value in effective response
    /// order, if any.
    pub fn first_value_by(&self, field: impl AsRef<str>) -> Result<Option<Value>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.first_value_by_slot(plan, target_slot)
                })
        })
    }

    /// Execute and return the last projected field value in effective response
    /// order, if any.
    pub fn last_value_by(&self, field: impl AsRef<str>) -> Result<Option<Value>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        Self::with_slot(field, |target_slot| {
            self.session
                .execute_load_query_with(self.query(), move |load, plan| {
                    load.last_value_by_slot(plan, target_slot)
                })
        })
    }

    /// Execute and return the first matching identifier in response order, if any.
    pub fn first(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| load.aggregate_first(plan))
    }

    /// Explain scalar `first()` routing without executing the terminal.
    pub fn explain_first(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        crate::db::DbSession::<E::Canister>::explain_load_query_terminal_with(self.query(), first())
    }

    /// Execute and return the last matching identifier in response order, if any.
    pub fn last(&self) -> Result<Option<Id<E>>, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        self.session
            .execute_load_query_with(self.query(), |load, plan| load.aggregate_last(plan))
    }

    /// Explain scalar `last()` routing without executing the terminal.
    pub fn explain_last(&self) -> Result<ExplainAggregateTerminalPlan, QueryError>
    where
        E: EntityValue,
    {
        self.ensure_non_paged_mode_ready()?;

        crate::db::DbSession::<E::Canister>::explain_load_query_terminal_with(self.query(), last())
    }

    /// Execute and require exactly one matching row.
    pub fn require_one(&self) -> Result<(), QueryError>
    where
        E: EntityValue,
    {
        self.execute()?.require_one()?;
        Ok(())
    }

    /// Execute and require at least one matching row.
    pub fn require_some(&self) -> Result<(), QueryError>
    where
        E: EntityValue,
    {
        self.execute()?.require_some()?;
        Ok(())
    }
}
