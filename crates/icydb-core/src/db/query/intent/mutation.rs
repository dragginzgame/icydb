//! Module: query::intent::mutation
//! Responsibility: query-intent mutation helpers for scalar/grouped/load/delete intent state.
//! Does not own: final planner validation or executor route/runtime semantics.
//! Boundary: applies fluent/query API mutations to internal intent state contracts.

use crate::db::{
    predicate::Predicate,
    query::{
        intent::{
            IntentError, KeyAccess, KeyAccessKind, KeyAccessState, order::push_order,
            state::QueryIntent,
        },
        plan::{
            FieldSlot, GroupAggregateSpec, GroupHavingClause, GroupHavingSpec,
            GroupedExecutionConfig, OrderDirection, OrderSpec,
        },
    },
};

impl<K> QueryIntent<K> {
    /// Append one filter predicate to scalar intent, implicitly AND-ing chains.
    pub(in crate::db::query::intent) fn append_predicate(&mut self, predicate: Predicate) {
        let scalar = self.scalar_mut();
        scalar.predicate = match scalar.predicate.take() {
            Some(existing) => Some(Predicate::And(vec![existing, predicate])),
            None => Some(predicate),
        };
    }

    /// Append one ascending ORDER BY key to scalar intent.
    pub(in crate::db::query::intent) fn push_order_ascending(&mut self, field: &str) {
        self.push_order_field(field, OrderDirection::Asc);
    }

    /// Append one descending ORDER BY key to scalar intent.
    pub(in crate::db::query::intent) fn push_order_descending(&mut self, field: &str) {
        self.push_order_field(field, OrderDirection::Desc);
    }

    /// Override scalar ORDER BY with one validated order specification.
    pub(in crate::db::query::intent) fn set_order_spec(&mut self, order: OrderSpec) {
        self.scalar_mut().order = Some(order);
    }

    /// Enable DISTINCT semantics in scalar intent state.
    pub(in crate::db::query::intent) const fn set_distinct(&mut self) {
        self.scalar_mut().distinct = true;
    }

    /// Set key access to one single-key lookup.
    pub(in crate::db::query::intent) fn set_by_id(&mut self, id: K) {
        self.set_key_access(KeyAccessKind::Single, KeyAccess::Single(id));
    }

    /// Set key access to one many-key lookup set.
    pub(in crate::db::query::intent) fn set_by_ids<I>(&mut self, ids: I)
    where
        I: IntoIterator<Item = K>,
    {
        self.set_key_access(
            KeyAccessKind::Many,
            KeyAccess::Many(ids.into_iter().collect()),
        );
    }

    /// Set key access to the singleton key path.
    pub(in crate::db::query::intent) fn set_only(&mut self, id: K) {
        self.set_key_access(KeyAccessKind::Only, KeyAccess::Single(id));
    }

    /// Record one grouped key slot while preserving grouped-delete policy semantics.
    pub(in crate::db::query::intent) fn push_group_field_slot(&mut self, field_slot: FieldSlot) {
        if matches!(self, Self::Delete(_)) {
            self.mark_delete_grouping_requested();
            return;
        }

        let group = &mut self.ensure_grouped_mut().group;
        if !group
            .group_fields
            .iter()
            .any(|existing| existing.index() == field_slot.index())
        {
            group.group_fields.push(field_slot);
        }
    }

    /// Record one grouped aggregate terminal while preserving delete policy flags.
    pub(in crate::db::query::intent) fn push_group_aggregate(
        &mut self,
        aggregate: GroupAggregateSpec,
    ) {
        if matches!(self, Self::Delete(_)) {
            self.mark_delete_grouping_requested();
            return;
        }

        self.ensure_grouped_mut().group.aggregates.push(aggregate);
    }

    /// Override grouped hard limits while preserving delete-grouping policy flags.
    pub(in crate::db::query::intent) fn set_grouped_limits(
        &mut self,
        max_groups: u64,
        max_group_bytes: u64,
    ) {
        if matches!(self, Self::Delete(_)) {
            self.mark_delete_grouping_requested();
            return;
        }

        self.ensure_grouped_mut().group.execution =
            GroupedExecutionConfig::with_hard_limits(max_groups, max_group_bytes);
    }

    /// Record one HAVING clause when grouped shape is present.
    ///
    /// Delete mode never materializes grouped shape, so grouped-delete policy is
    /// tracked through delete flags instead of storing grouped clause state.
    pub(in crate::db::query::intent) fn push_having_clause(
        &mut self,
        clause: GroupHavingClause,
    ) -> Result<(), IntentError> {
        if matches!(self, Self::Delete(_)) {
            if self.is_grouped() {
                self.mark_delete_grouping_requested();
                return Ok(());
            }

            return Err(IntentError::HavingRequiresGroupBy);
        }

        let Some(grouped) = self.grouped_mut() else {
            return Err(IntentError::HavingRequiresGroupBy);
        };

        let having = grouped.having.get_or_insert(GroupHavingSpec {
            clauses: Vec::new(),
        });
        having.clauses.push(clause);

        Ok(())
    }

    // Record key-access origin and detect conflicting key-only builder usage.
    fn set_key_access(&mut self, kind: KeyAccessKind, access: KeyAccess<K>) {
        let scalar = self.scalar_mut();
        if let Some(existing) = &scalar.key_access
            && existing.kind != kind
        {
            scalar.key_access_conflict = true;
        }

        scalar.key_access = Some(KeyAccessState { kind, access });
    }

    // Append one ORDER BY field while preserving any previously-declared order.
    fn push_order_field(&mut self, field: &str, direction: OrderDirection) {
        let scalar = self.scalar_mut();
        scalar.order = Some(push_order(scalar.order.take(), field, direction));
    }
}
