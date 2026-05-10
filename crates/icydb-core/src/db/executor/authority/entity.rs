#[cfg(test)]
use crate::model::field::FieldModel;
use crate::{
    db::{
        access::{SemanticIndexRangeSpec, validate_access_runtime_invariants_with_schema},
        cursor::{CursorPlanError, PlannedCursor},
        data::StorageKey,
        executor::{planning::route::AggregateRouteShape, terminal::RowLayout},
        index::IndexKey,
        query::plan::{
            AccessPlannedQuery, AggregateKind, CoveringReadExecutionPlan, CoveringReadPlan,
            PlannedContinuationContract, covering_hybrid_projection_plan_with_schema_info,
            covering_read_execution_plan_with_schema_info,
        },
        schema::{
            AcceptedGeneratedCompatibleRowShape, AcceptedRowDecodeContract,
            AcceptedRowLayoutRuntimeDescriptor, AcceptedSchemaSnapshot, SchemaInfo,
        },
    },
    error::InternalError,
    metrics::sink::{
        PreparedShapeFinalizationOutcome, record_prepared_shape_finalization_for_path,
    },
    model::entity::EntityModel,
    traits::{EntityKind, Path},
    types::EntityTag,
    value::Value,
};
use std::sync::Arc;

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

#[derive(Clone, Debug)]
pub struct EntityAuthority {
    model: &'static EntityModel,
    row_layout: Option<RowLayout>,
    primary_key_name: &'static str,
    entity_tag: EntityTag,
    store_path: &'static str,
    accepted_schema_info: Option<Arc<SchemaInfo>>,
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
            row_layout: None,
            primary_key_name: model.primary_key.name,
            entity_tag,
            store_path,
            accepted_schema_info: None,
        }
    }

    /// Build raw generated authority from one resolved entity type for tests.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn for_generated_type_for_test<E: EntityKind>() -> Self {
        Self::new(E::MODEL, E::ENTITY_TAG, E::Store::PATH)
    }

    /// Build typed executor authority from an accepted schema snapshot.
    ///
    /// The generated model remains the compatibility proof input until
    /// accepted snapshots own every index/layout fact directly, but callers get
    /// back only authority with accepted row decode and schema info attached.
    pub(in crate::db) fn from_accepted_schema_for_type<E>(
        accepted_schema: &AcceptedSchemaSnapshot,
    ) -> Result<Self, InternalError>
    where
        E: EntityKind,
    {
        let authority = Self::new(E::MODEL, E::ENTITY_TAG, E::Store::PATH);
        let (accepted_row_layout, row_shape) =
            AcceptedRowLayoutRuntimeDescriptor::from_generated_compatible_schema(
                accepted_schema,
                authority.model(),
            )?;
        let row_decode_contract = accepted_row_layout.row_decode_contract();
        let schema_info =
            SchemaInfo::from_accepted_snapshot_for_model(authority.model(), accepted_schema);

        Ok(
            authority.with_accepted_row_decode_contract(
                row_shape,
                row_decode_contract,
                schema_info,
            ),
        )
    }

    /// Return authority with row decode frozen from accepted schema field contracts.
    ///
    /// The generated-compatible proof remains an explicit input so callers
    /// cannot attach accepted decode contracts to layouts that the current
    /// generated write/materialization bridge cannot still handle.
    #[must_use]
    pub(in crate::db) fn with_accepted_row_decode_contract(
        self,
        row_shape: AcceptedGeneratedCompatibleRowShape,
        accepted_decode_contract: AcceptedRowDecodeContract,
        accepted_schema_info: SchemaInfo,
    ) -> Self {
        let row_layout = RowLayout::from_generated_compatible_accepted_decode_contract(
            self.model.path(),
            row_shape,
            accepted_decode_contract,
        );

        Self {
            row_layout: Some(row_layout),
            accepted_schema_info: Some(Arc::new(accepted_schema_info)),
            ..self
        }
    }

    /// Return authority with generated row decode attached for test-only
    /// prepared plan construction.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn with_generated_row_layout_for_test(self) -> Self {
        Self {
            row_layout: Some(RowLayout::from_generated_model_for_test(self.model)),
            ..self
        }
    }

    /// Return authority with cursor schema facts supplied by test-only
    /// generated plan construction.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) fn with_cursor_schema_info_for_test(self, schema_info: SchemaInfo) -> Self {
        let authority = self.with_generated_row_layout_for_test();

        Self {
            accepted_schema_info: Some(Arc::new(schema_info)),
            ..authority
        }
    }

    /// Borrow the entity model authority.
    #[must_use]
    pub const fn model(&self) -> &'static EntityModel {
        self.model
    }

    /// Borrow the authoritative generated field table for this entity.
    #[must_use]
    #[cfg(test)]
    pub(in crate::db) const fn fields(&self) -> &'static [FieldModel] {
        self.model.fields()
    }

    /// Borrow the frozen structural row-decode layout for this entity.
    #[must_use]
    pub(in crate::db::executor) fn row_layout(&self) -> RowLayout {
        self.row_layout_ref().clone()
    }

    /// Borrow the frozen structural row-decode layout for metadata-only callers.
    #[must_use]
    pub(in crate::db::executor) const fn row_layout_ref(&self) -> &RowLayout {
        self.row_layout.as_ref().expect(
            "entity authority row layout must be selected from accepted schema or explicit test layout",
        )
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
        &self,
        plan: &mut AccessPlannedQuery,
    ) {
        // Cached/session planning may already have frozen static execution
        // metadata with accepted schema authority. Do not overwrite that
        // schema-selected slot contract while lowering the executor core.
        if plan.has_static_planning_shape() {
            record_prepared_shape_finalization_for_path(
                self.entity_path(),
                PreparedShapeFinalizationOutcome::AlreadyFinalized,
            );
            return;
        }

        let schema_info = self
            .accepted_schema_info
            .as_ref()
            .expect("executor static shape finalization requires accepted schema info");
        plan.finalize_static_planning_shape_for_model_with_schema(self.model, schema_info)
            .expect("executable plan core requires accepted-schema static execution shape");
    }

    /// Finalize planner-owned route profiling through canonical entity authority.
    pub(in crate::db::executor) fn finalize_planner_route_profile(
        &self,
        plan: &mut AccessPlannedQuery,
    ) {
        plan.finalize_planner_route_profile_for_model(self.model);
    }

    /// Validate one access-planned query against authority-owned structural contracts.
    pub(in crate::db::executor) fn validate_executor_plan(
        &self,
        plan: &AccessPlannedQuery,
    ) -> Result<(), InternalError> {
        if !plan.has_static_planning_shape() {
            return Err(InternalError::query_executor_invariant(format!(
                "executor plan validation requires planner-frozen static shape for '{}'",
                self.entity_path()
            )));
        }

        let schema_info = self.accepted_schema_info.as_ref().ok_or_else(|| {
            InternalError::query_executor_invariant(
                "executor plan validation requires accepted schema info",
            )
        })?;

        validate_access_runtime_invariants_with_schema(schema_info.as_ref(), &plan.access)
            .map_err(crate::db::access::AccessPlanError::into_internal_error)
    }

    /// Validate and decode one scalar continuation cursor through authority-owned contracts.
    pub(in crate::db::executor) fn prepare_scalar_cursor(
        &self,
        contract: &PlannedContinuationContract,
        bytes: Option<&[u8]>,
    ) -> Result<PlannedCursor, CursorPlanError> {
        let schema_info = self.cursor_schema_info()?;

        contract.prepare_scalar_cursor(
            self.entity_path(),
            self.entity_tag,
            self.model,
            schema_info,
            bytes,
        )
    }

    /// Revalidate one scalar continuation cursor through authority-owned contracts.
    pub(in crate::db::executor) fn revalidate_scalar_cursor(
        &self,
        contract: &PlannedContinuationContract,
        cursor: PlannedCursor,
    ) -> Result<PlannedCursor, CursorPlanError> {
        let schema_info = self.cursor_schema_info()?;

        contract.revalidate_scalar_cursor(self.entity_tag, self.model, schema_info, cursor)
    }

    fn cursor_schema_info(&self) -> Result<&SchemaInfo, CursorPlanError> {
        self.accepted_schema_info
            .as_ref()
            .ok_or_else(|| {
                CursorPlanError::continuation_cursor_invariant(
                    "scalar cursor validation requires accepted schema info",
                )
            })
            .map(AsRef::as_ref)
    }

    /// Resolve one aggregate route shape through authority-owned schema metadata.
    pub(in crate::db) fn aggregate_route_shape<'a>(
        &self,
        kind: AggregateKind,
        target_field: Option<&'a str>,
    ) -> Result<AggregateRouteShape<'a>, InternalError> {
        let schema_info = self.accepted_schema_info.as_ref().ok_or_else(|| {
            InternalError::query_executor_invariant(
                "aggregate route shape derivation requires accepted schema info",
            )
        })?;

        Ok(AggregateRouteShape::new_from_schema_info(
            kind,
            target_field,
            schema_info,
        ))
    }

    /// Derive one covering-read execution contract through authority-owned schema metadata.
    #[must_use]
    pub(in crate::db::executor) fn covering_read_execution_plan(
        &self,
        plan: &AccessPlannedQuery,
        strict_predicate_compatible: bool,
    ) -> Option<CoveringReadExecutionPlan> {
        let schema_info = self.accepted_schema_info.as_ref()?;

        covering_read_execution_plan_with_schema_info(
            schema_info,
            plan,
            self.primary_key_name,
            strict_predicate_compatible,
        )
    }

    /// Derive one hybrid covering projection contract through authority-owned schema metadata.
    #[must_use]
    pub(in crate::db::executor) fn covering_hybrid_projection_plan(
        &self,
        plan: &AccessPlannedQuery,
    ) -> Option<CoveringReadPlan> {
        let schema_info = self.accepted_schema_info.as_ref()?;

        covering_hybrid_projection_plan_with_schema_info(schema_info, plan, self.primary_key_name)
    }

    /// Build one structural index key from already-materialized row slots
    /// without cloning field values back out of the row cache first.
    pub(in crate::db::executor) fn index_range_anchor_key_from_slot_ref_reader<'a>(
        &self,
        storage_key: StorageKey,
        index_range: &SemanticIndexRangeSpec,
        read_slot: &mut dyn FnMut(usize) -> Option<&'a Value>,
    ) -> Result<Option<IndexKey>, InternalError> {
        let schema_info = self.index_key_schema_info()?;
        let index = index_range.index();

        if index.has_expression_key_items() {
            return IndexKey::new_from_slot_ref_reader_with_access_contract(
                self.entity_tag,
                storage_key,
                schema_info,
                index,
                read_slot,
            );
        }

        let accepted_index = schema_info
            .field_path_indexes()
            .iter()
            .find(|accepted| accepted.name() == index.name())
            .ok_or_else(|| {
                InternalError::query_executor_invariant(
                    "field-path index cursor anchor derivation requires accepted index contract",
                )
            })?;

        IndexKey::new_from_slot_ref_reader_with_accepted_field_path_index(
            self.entity_tag,
            storage_key,
            accepted_index,
            read_slot,
        )
    }

    fn index_key_schema_info(&self) -> Result<&SchemaInfo, InternalError> {
        self.accepted_schema_info
            .as_ref()
            .ok_or_else(|| {
                InternalError::query_executor_invariant(
                    "index cursor anchor derivation requires accepted schema info",
                )
            })
            .map(AsRef::as_ref)
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::{
        db::{
            executor::EntityAuthority,
            predicate::MissingRowPolicy,
            query::plan::{
                AccessPlannedQuery,
                expr::{FieldId as ExprFieldId, ProjectionSelection},
            },
            schema::{
                AcceptedSchemaSnapshot, FieldId, PersistedFieldKind, PersistedFieldSnapshot,
                PersistedSchemaSnapshot, SchemaFieldDefault, SchemaFieldSlot, SchemaInfo,
                SchemaRowLayout, SchemaVersion,
            },
        },
        metrics::{metrics_report, metrics_reset_all},
        model::{
            entity::EntityModel,
            field::{FieldKind, FieldModel, FieldStorageDecode, LeafCodec},
            index::IndexModel,
        },
        testing::entity_model_from_static,
        types::EntityTag,
    };

    const AUTHORITY_SCHEMA_SLOT_TEST_STORE_PATH: &str = "authority_schema_slot_test_store";

    static FIELDS: [FieldModel; 2] = [
        FieldModel::generated("id", FieldKind::Ulid),
        FieldModel::generated("profile", FieldKind::Structured { queryable: true }),
    ];
    static INDEXES: [&IndexModel; 0] = [];
    static MODEL: EntityModel = entity_model_from_static(
        "executor::authority::tests::Entity",
        "Entity",
        &FIELDS[0],
        0,
        &FIELDS,
        &INDEXES,
    );

    // Build one accepted schema with a deliberately divergent row-layout slot
    // for `profile` so this test catches generated-fallback re-finalization.
    fn accepted_schema_with_profile_slot(slot: SchemaFieldSlot) -> SchemaInfo {
        let snapshot = AcceptedSchemaSnapshot::new(PersistedSchemaSnapshot::new(
            SchemaVersion::initial(),
            "executor::authority::tests::Entity".to_string(),
            "Entity".to_string(),
            FieldId::new(1),
            SchemaRowLayout::new(
                SchemaVersion::initial(),
                vec![
                    (FieldId::new(1), SchemaFieldSlot::new(0)),
                    (FieldId::new(2), slot),
                ],
            ),
            vec![
                PersistedFieldSnapshot::new(
                    FieldId::new(1),
                    "id".to_string(),
                    SchemaFieldSlot::new(0),
                    PersistedFieldKind::Ulid,
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::ByKind,
                    LeafCodec::StructuralFallback,
                ),
                PersistedFieldSnapshot::new(
                    FieldId::new(2),
                    "profile".to_string(),
                    SchemaFieldSlot::new(1),
                    PersistedFieldKind::Structured { queryable: true },
                    Vec::new(),
                    false,
                    SchemaFieldDefault::None,
                    FieldStorageDecode::Value,
                    LeafCodec::StructuralFallback,
                ),
            ],
        ));

        SchemaInfo::from_accepted_snapshot_for_model(&MODEL, &snapshot)
    }

    #[test]
    fn authority_finalization_preserves_schema_finalized_static_shape() {
        metrics_reset_all();
        let authority = EntityAuthority::new(
            &MODEL,
            EntityTag::new(0x1460_0013),
            AUTHORITY_SCHEMA_SLOT_TEST_STORE_PATH,
        );
        let schema = accepted_schema_with_profile_slot(SchemaFieldSlot::new(7));
        let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
        plan.projection_selection = ProjectionSelection::Fields(vec![ExprFieldId::new("profile")]);

        plan.finalize_static_planning_shape_for_model_with_schema(&MODEL, &schema)
            .expect("schema-finalized static shape should build");
        assert_eq!(plan.frozen_direct_projection_slots(), Some([7].as_slice()));

        authority.finalize_static_planning_shape(&mut plan);

        assert_eq!(plan.frozen_direct_projection_slots(), Some([7].as_slice()));

        let report = metrics_report(None);
        let counters = report
            .counters()
            .expect("authority finalization should record metrics");
        assert_eq!(counters.ops().prepared_shape_already_finalized(), 1);
        assert_eq!(counters.ops().prepared_shape_generated_fallback(), 0);
    }

    #[test]
    fn authority_finalization_uses_authority_schema_when_shape_is_missing() {
        metrics_reset_all();
        let authority = EntityAuthority::new(
            &MODEL,
            EntityTag::new(0x1460_0014),
            AUTHORITY_SCHEMA_SLOT_TEST_STORE_PATH,
        )
        .with_cursor_schema_info_for_test(accepted_schema_with_profile_slot(SchemaFieldSlot::new(
            7,
        )));
        let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
        plan.projection_selection = ProjectionSelection::Fields(vec![ExprFieldId::new("profile")]);

        authority.finalize_static_planning_shape(&mut plan);

        assert_eq!(plan.frozen_direct_projection_slots(), Some([7].as_slice()));
    }
}
