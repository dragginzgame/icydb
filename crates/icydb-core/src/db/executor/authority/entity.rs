use crate::{
    db::{
        access::{AccessPlanError, validate_access_structure_model},
        cursor::{CursorPlanError, PlannedCursor},
        data::StorageKey,
        executor::terminal::RowLayout,
        index::IndexKey,
        query::plan::{
            AccessPlannedQuery, ContinuationContract, CoveringReadExecutionPlan,
            covering_read_execution_plan_from_fields,
        },
        relation::model_has_strong_relation_targets,
        schema::SchemaInfo,
    },
    error::InternalError,
    model::{entity::EntityModel, field::FieldModel, index::IndexModel},
    traits::{EntityKind, Path},
    types::EntityTag,
    value::Value,
};

///
/// EntityAuthority
///
/// EntityAuthority is the canonical structural entity-identity bundle used by
/// executor runtime preparation once typed API boundaries have resolved the
/// concrete entity type.
/// It keeps model, entity-tag, and store path authority aligned while deriving
/// the entity path from the model itself so execution-core code does not pass
/// duplicated metadata independently.
///

#[derive(Clone, Copy, Debug)]
pub struct EntityAuthority {
    model: &'static EntityModel,
    row_layout: RowLayout,
    primary_key_name: &'static str,
    entity_tag: EntityTag,
    store_path: &'static str,
}

impl EntityAuthority {
    /// Build authority from explicit runtime metadata.
    #[must_use]
    pub const fn new(
        model: &'static EntityModel,
        entity_tag: EntityTag,
        store_path: &'static str,
    ) -> Self {
        Self {
            model,
            row_layout: RowLayout::from_model(model),
            primary_key_name: model.primary_key.name,
            entity_tag,
            store_path,
        }
    }

    /// Build authority from one resolved entity type.
    #[must_use]
    pub const fn for_type<E: EntityKind>() -> Self {
        Self::new(E::MODEL, E::ENTITY_TAG, E::Store::PATH)
    }

    /// Borrow the entity model authority.
    #[must_use]
    pub const fn model(&self) -> &'static EntityModel {
        self.model
    }

    /// Borrow the cached schema authority for this entity.
    #[must_use]
    pub(in crate::db) fn schema_info(&self) -> &'static SchemaInfo {
        SchemaInfo::cached_for_entity_model(self.model)
    }

    /// Borrow the authoritative generated field table for this entity.
    #[must_use]
    pub(in crate::db) const fn fields(&self) -> &'static [FieldModel] {
        self.model.fields()
    }

    /// Borrow the frozen structural row-decode layout for this entity.
    #[must_use]
    pub(in crate::db::executor) const fn row_layout(&self) -> RowLayout {
        self.row_layout
    }

    /// Borrow the frozen structural primary-key field name for this entity.
    #[must_use]
    pub const fn primary_key_name(&self) -> &'static str {
        self.primary_key_name
    }

    /// Borrow structural entity-tag authority.
    #[must_use]
    pub const fn entity_tag(&self) -> EntityTag {
        self.entity_tag
    }

    /// Borrow structural entity-path authority.
    #[must_use]
    pub const fn entity_path(&self) -> &'static str {
        self.model.path()
    }

    /// Borrow structural store-path authority.
    #[must_use]
    pub const fn store_path(&self) -> &'static str {
        self.store_path
    }

    /// Finalize planner-owned static execution shape through canonical entity authority.
    pub(in crate::db::executor) fn finalize_static_planning_shape(
        self,
        plan: &mut AccessPlannedQuery,
    ) {
        plan.finalize_static_planning_shape_for_model(self.model)
            .expect("executable plan core requires planner-frozen static execution shape");
    }

    /// Finalize planner-owned route profiling through canonical entity authority.
    pub(in crate::db::executor) fn finalize_planner_route_profile(
        self,
        plan: &mut AccessPlannedQuery,
    ) {
        plan.finalize_planner_route_profile_for_model(self.model);
    }

    /// Validate one access-planned query against authority-owned structural contracts.
    pub(in crate::db::executor) fn validate_executor_plan(
        self,
        plan: &AccessPlannedQuery,
    ) -> Result<(), InternalError> {
        validate_access_structure_model(self.schema_info(), self.model, &plan.access)
            .map_err(AccessPlanError::into_internal_error)
    }

    /// Validate and decode one scalar continuation cursor through authority-owned contracts.
    pub(in crate::db::executor) fn prepare_scalar_cursor(
        self,
        contract: &ContinuationContract,
        bytes: Option<&[u8]>,
    ) -> Result<PlannedCursor, CursorPlanError> {
        contract.prepare_scalar_cursor(self.entity_path(), self.entity_tag, self.model, bytes)
    }

    /// Revalidate one scalar continuation cursor through authority-owned contracts.
    pub(in crate::db::executor) fn revalidate_scalar_cursor(
        self,
        contract: &ContinuationContract,
        cursor: PlannedCursor,
    ) -> Result<PlannedCursor, CursorPlanError> {
        contract.revalidate_scalar_cursor(self.entity_tag, self.model, cursor)
    }

    /// Derive one covering-read execution contract through authority-owned schema metadata.
    #[must_use]
    pub(in crate::db::executor) fn covering_read_execution_plan(
        self,
        plan: &AccessPlannedQuery,
        strict_predicate_compatible: bool,
    ) -> Option<CoveringReadExecutionPlan> {
        covering_read_execution_plan_from_fields(
            self.fields(),
            plan,
            self.primary_key_name,
            strict_predicate_compatible,
        )
    }

    /// Return whether this entity declares strong relation targets.
    #[must_use]
    pub(in crate::db::executor) fn has_strong_relation_targets(self) -> bool {
        model_has_strong_relation_targets(self.model)
    }

    /// Build one structural index key from already-materialized row slots.
    pub(in crate::db::executor) fn index_key_from_slot_reader(
        self,
        storage_key: StorageKey,
        index: &IndexModel,
        read_slot: &mut dyn FnMut(usize) -> Option<Value>,
    ) -> Result<Option<IndexKey>, InternalError> {
        IndexKey::new_from_slot_reader(self.entity_tag, storage_key, self.model, index, read_slot)
    }
}
