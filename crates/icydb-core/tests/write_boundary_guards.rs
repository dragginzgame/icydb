use std::{fs, path::PathBuf};

fn read_source(relative_path: &str) -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push(relative_path);

    fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()))
}

fn rust_sources_under(relative_path: &str) -> Vec<PathBuf> {
    let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    root.push(relative_path);

    let mut sources = Vec::new();
    let mut pending = vec![root];
    while let Some(path) = pending.pop() {
        let entries = fs::read_dir(&path)
            .unwrap_or_else(|err| panic!("failed to list {}: {err}", path.display()));
        for entry in entries {
            let path = entry
                .unwrap_or_else(|err| panic!("failed to read directory entry: {err}"))
                .path();
            if path.is_dir() {
                pending.push(path);
            } else if path.extension().is_some_and(|extension| extension == "rs") {
                sources.push(path);
            }
        }
    }

    sources.sort();
    sources
}

fn compact_source(source: &str) -> String {
    source
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect()
}

#[test]
fn data_store_insert_stays_canonical_row_only() {
    let source = read_source("src/db/data/store.rs");

    assert!(
        source.contains("pub(in crate::db) fn insert(&mut self, key: RawDataKey, row: CanonicalRow) -> Option<RawRow>"),
        "DataStore::insert must remain CanonicalRow-only at the production write boundary",
    );
    assert!(
        !source.contains("pub fn insert(&mut self, key: RawDataKey, row: RawRow)"),
        "DataStore::insert must not accept RawRow in production code",
    );
}

#[test]
fn prepared_row_write_payloads_stay_canonical() {
    let prepared_op = read_source("src/db/commit/prepared_op.rs");
    let typed_save = read_source("src/db/executor/mutation/save/typed.rs");
    let structural_save = read_source("src/db/executor/mutation/save/structural.rs");

    assert!(
        prepared_op.contains("pub(crate) data_value: Option<CanonicalRow>,"),
        "prepared row commit ops must carry CanonicalRow after-images",
    );
    assert!(
        !prepared_op.contains("pub(crate) data_value: Option<RawRow>,"),
        "prepared row commit ops must not regress to RawRow after-images",
    );
    assert!(
        typed_save.contains("canonical_row_from_entity_with_accepted_contract(")
            && !typed_save.contains("CanonicalRow::from_entity(entity)?")
            && !typed_save.contains("CanonicalRow::from_generated_entity_for_test(entity)?"),
        "typed save after-image construction must use accepted-contract row emission",
    );
    assert!(
        structural_save
            .contains("fn build_structural_update_after_image_row_with_accepted_contract(")
            && structural_save.contains("accepted_row_decode_contract: AcceptedRowDecodeContract,")
            && structural_save.contains(") -> Result<CanonicalRow, InternalError>"),
        "structural update after-image builder must stay accepted-contract aware and return CanonicalRow",
    );
    assert!(
        structural_save
            .contains("fn build_normalized_structural_after_image_row_with_accepted_contract(")
            && structural_save.contains("canonical_row_from_entity_with_accepted_contract(")
            && !structural_save.contains("MutationInput::from_entity("),
        "normalized structural save after-image builder must use accepted-contract row emission",
    );
    assert!(
        structural_save
            .contains("materialize_entity_from_serialized_structural_patch_with_accepted_contract")
            && !structural_save.contains("materialize_entity_from_serialized_structural_patch_for_generated_model_for_test::<"),
        "structural insert/replace materialization must use accepted-contract decode authority",
    );
}

#[test]
fn accepted_storage_row_contracts_do_not_retain_generated_field_bridge() {
    let structural_row = read_source("src/db/data/structural_row.rs");
    let structural_row_compact = compact_source(&structural_row);
    let row_reader = read_source("src/db/data/persisted_row/reader/core.rs");
    let row_reader_compact = compact_source(&row_reader);
    let persisted_patch = read_source("src/db/data/persisted_row/patch.rs");
    let persisted_patch_compact = compact_source(&persisted_patch);
    let primary_key_reader = read_source("src/db/data/persisted_row/reader/primary_key.rs");
    let primary_key_reader_compact = compact_source(&primary_key_reader);
    let reverse_index = read_source("src/db/relation/reverse_index.rs");
    let reverse_index_compact = compact_source(&reverse_index);
    let save_validation = read_source("src/db/executor/mutation/save_validation.rs");
    let save_validation_compact = compact_source(&save_validation);
    let row_decode = read_source("src/db/executor/terminal/row_decode/mod.rs");

    assert!(
        structural_row.contains("pub(in crate::db) fn from_accepted_schema_snapshot(")
            && structural_row.contains("Self::from_accepted_decode_contract(")
            && structural_row.contains("fn from_generated_model_for_test(")
            && structural_row
                .contains("fn from_generated_model_with_accepted_decode_contract_for_test(")
            && !structural_row.contains("fn from_model(")
            && !structural_row.contains("fn from_model_with_accepted_decode_contract(")
            && !structural_row.contains("fn from_model_with_accepted_schema_snapshot(")
            && !structural_row.contains(
                "Ok(Self::from_generated_model_with_accepted_decode_contract_for_test(\n            model,\n            descriptor.row_decode_contract(),\n        ))",
        ),
        "storage row readers must use accepted-only row contracts after the generated-compatibility proof",
    );
    assert!(
        row_decode
            .contains("pub(in crate::db) fn from_generated_compatible_accepted_decode_contract(")
            && row_decode.contains("fn from_generated_model_for_test(")
            && row_decode.contains("StructuralRowContract::from_accepted_decode_contract(")
            && !row_decode
                .contains("StructuralRowContract::from_generated_model_with_accepted_decode_contract_for_test("),
        "accepted executor row layouts must not retain the generated field bridge after compatibility proof",
    );
    assert!(
        persisted_patch.contains("StructuralRowContract::from_accepted_decode_contract(")
            && persisted_patch
                .contains("Self::validate_payload_slot(&contract, generated_fields, slot)?")
            && !persisted_patch
                .contains("StructuralRowContract::from_generated_model_with_accepted_decode_contract_for_test("),
        "accepted structural patch materialization must validate payload slots through accepted row contracts without retaining the generated field bridge",
    );
    assert!(
        structural_row_compact.contains("required_accepted_field_decode_contract(&self,slot:usize")
            && structural_row_compact
                .contains(".required_field_for_slot(self.entity_path(),slot)?")
            && !structural_row_compact.contains("fnaccepted_field_decode_contract(")
            && structural_row_compact.contains(
                "ifself.accepted_decode_contract.is_some(){letfield=self.required_accepted_field_decode_contract(slot)?;returnOk(field.leaf_codec());}",
            )
            && structural_row_compact.contains(
                "ifself.accepted_decode_contract.is_some(){letfield=self.required_accepted_field_decode_contract(slot)?;returnOk(field.field_name());}",
            ),
        "accepted structural row field-name and leaf-codec lookups must fail closed instead of falling back to generated field metadata",
    );
    assert!(
        row_reader_compact.contains("fnrequired_accepted_field_decode_contract(&self,slot:usize")
            && row_reader_compact.contains(
                "self.contract.required_accepted_field_decode_contract(slot)",
            )
            && row_reader_compact.contains(
                "letaccepted_field=self.required_accepted_field_decode_contract(slot)?;ifletSome(value)=self.required_accepted_value_storage_scalar(slot,accepted_field)?",
            )
            && !row_reader_compact.contains("pub(incrate::db)fnaccepted_field_decode_contract")
            && !row_reader_compact.contains(
                "ifletSome(accepted_field)=self.contract.accepted_field_decode_contract(slot)",
            ),
        "accepted structural row reader scalar fast paths must fail closed instead of treating missing accepted field metadata as a non-fast-path value",
    );
    assert!(
        primary_key_reader_compact.contains("ifcontract.has_accepted_decode_contract(){")
            && primary_key_reader_compact
                .contains("contract.required_accepted_field_decode_contract(primary_key_slot)?")
            && !primary_key_reader_compact
                .contains(".accepted_field_decode_contract(primary_key_slot).ok_or_else(")
            && !primary_key_reader_compact
                .contains("ifletSome(primary_key_field)=contract.accepted_field_decode_contract(primary_key_slot)"),
        "accepted primary-key validation must fail closed instead of falling back to generated field metadata",
    );
    assert!(
        persisted_patch_compact.contains("contract.required_accepted_field_decode_contract(slot)?")
            && persisted_patch_compact
                .contains("contract.required_accepted_field_decode_contract(slot.index())?"),
        "accepted structural patch payload handling must use the shared required accepted-field contract",
    );
    assert!(
        reverse_index_compact
            .contains("source_row_contract.required_accepted_field_decode_contract(slot)?")
            && save_validation_compact
                .contains("contract.required_accepted_field_decode_contract(slot)?")
            && save_validation_compact
                .contains(".required_accepted_field_decode_contract(field_index)?"),
        "relation and save validation accepted field scans must use the shared required accepted-field contract",
    );
}

#[test]
fn generated_persisted_row_bridge_helpers_are_named_test_only() {
    let persisted_row_mod = read_source("src/db/data/persisted_row/mod.rs");
    let persisted_patch = read_source("src/db/data/persisted_row/patch.rs");
    let persisted_patch_compact = compact_source(&persisted_patch);
    let data_row = read_source("src/db/data/row.rs");
    let data_row_compact = compact_source(&data_row);
    let row_reader = read_source("src/db/data/persisted_row/reader/core.rs");
    let row_reader_compact = compact_source(&row_reader);
    let patch_writer = read_source("src/db/data/persisted_row/writer.rs");
    let patch_writer_compact = compact_source(&patch_writer);

    assert!(
        persisted_row_mod.contains("#[cfg(test)]\nmod writer;")
            && persisted_row_mod.contains(
                "materialize_entity_from_serialized_structural_patch_for_generated_model_for_test",
            )
            && persisted_row_mod.contains(
                "serialize_entity_slots_as_complete_serialized_patch_for_generated_model_for_test",
            ),
        "generated persisted-row writer and patch bridges must stay behind test-only exports",
    );
    assert!(
        persisted_patch.contains("fn new_for_generated_model_for_test(")
            && persisted_patch.contains(
                "fn materialize_entity_from_serialized_structural_patch_for_generated_model_for_test",
            )
            && persisted_patch.contains(
                "fn canonical_row_from_complete_serialized_structural_patch_for_generated_model_for_test",
            )
            && persisted_patch.contains("fn canonical_row_from_entity_for_generated_model_for_test")
            && persisted_patch.contains(
                "fn serialize_entity_slots_as_complete_serialized_patch_for_generated_model_for_test",
            )
            && persisted_patch.contains(
                "fn materialize_entity_from_serialized_structural_patch_with_accepted_contract",
            )
            && persisted_patch.contains("fn canonical_row_from_entity_with_accepted_contract")
            && !persisted_patch.contains("SerializedPatchPayloads::new(")
            && !persisted_patch_compact
                .contains("fnmaterialize_entity_from_serialized_structural_patch<E>")
            && !persisted_patch_compact
                .contains("fncanonical_row_from_complete_serialized_structural_patch(")
            && !persisted_patch_compact.contains("fncanonical_row_from_entity<E>")
            && !persisted_patch_compact
                .contains("fnserialize_entity_slots_as_complete_serialized_patch<E>"),
        "generated serialized patch materialization and row-emission helpers must not keep neutral production-looking names",
    );
    assert!(
        data_row.contains("fn from_generated_entity_for_test")
            && data_row.contains(
                "fn from_complete_serialized_structural_patch_for_generated_model_for_test",
            )
            && data_row.contains("fn try_decode_with_generated_model_for_test")
            && !data_row_compact.contains("fnfrom_entity<E>")
            && !data_row_compact.contains("fnfrom_complete_serialized_structural_patch(")
            && !data_row_compact.contains("fntry_decode<E"),
        "test-only raw row helpers must make generated-model decode authority explicit",
    );
    assert!(
        row_reader.contains("fn from_raw_row_with_generated_model_for_test(")
            && row_reader.contains("fn from_raw_row_with_unvalidated_generated_model_for_test(",)
            && row_reader.contains("fn from_raw_row_with_validated_contract(")
            && !row_reader_compact.contains("pub(incrate::db)fnfrom_raw_row(")
            && !row_reader_compact.contains("fnfrom_raw_row_with_model("),
        "generated structural slot-reader construction must stay named as a generated test bridge",
    );
    assert!(
        patch_writer.contains("fn for_generated_model_for_test(")
            && !patch_writer_compact.contains("fnfor_model("),
        "generated complete serialized patch writers must not expose neutral for_model construction",
    );
}

#[test]
fn commit_and_delete_relation_row_contracts_use_accepted_snapshots() {
    let structural_row = read_source("src/db/data/structural_row.rs");
    let commit_prepare = read_source("src/db/commit/prepare.rs");
    let commit_prepare_compact = compact_source(&commit_prepare);
    let relation_validate = read_source("src/db/relation/validate.rs");

    assert!(
        structural_row.contains("pub(in crate::db) fn from_accepted_schema_snapshot(")
            && !structural_row.contains("fn from_model_with_accepted_schema_snapshot("),
        "structural row contracts must expose accepted-snapshot construction without retaining the generated-compatible snapshot constructor",
    );
    assert!(
        commit_prepare_compact.contains(
            "StructuralRowContract::from_accepted_schema_snapshot(authority.entity_path,&accepted,)",
        ) && commit_prepare_compact
            .contains("SchemaInfo::from_accepted_snapshot_for_model(authority.model,&accepted)",)
            && relation_validate.contains(
                "StructuralRowContract::from_accepted_schema_snapshot(S::PATH, &accepted)"
            )
            && !commit_prepare.contains(
                "StructuralRowContract::from_generated_model_for_test_with_accepted_schema_snapshot"
            )
            && !relation_validate.contains(
                "StructuralRowContract::from_generated_model_for_test_with_accepted_schema_snapshot"
            ),
        "commit preflight and delete relation validation must build accepted-only row contracts after schema acceptance",
    );
}

#[test]
fn accepted_row_decode_contract_runtime_lookups_fail_closed() {
    let schema_runtime = read_source("src/db/schema/runtime.rs");
    let schema_runtime_compact = compact_source(&schema_runtime);
    let relation_save_validate = read_source("src/db/relation/save_validate.rs");
    let relation_save_validate_compact = compact_source(&relation_save_validate);
    let save_validation = read_source("src/db/executor/mutation/save_validation.rs");
    let save_validation_compact = compact_source(&save_validation);

    assert!(
        schema_runtime_compact.contains("pub(incrate::db)fnrequired_field_for_slot(")
            && schema_runtime_compact.contains(
                "InternalError::persisted_row_slot_lookup_out_of_bounds(entity_path,slot)",
            ),
        "accepted row-decode contracts must expose a required slot lookup for accepted runtime authority",
    );
    assert!(
        save_validation_compact
            .contains("accepted_contract.required_field_for_slot(E::PATH,slot)?")
            && save_validation_compact
                .contains("accepted_contract.required_field_for_slot(E::PATH,primary_key_slot)?")
            && save_validation_compact
                .contains("accepted_contract.required_field_for_slot(E::PATH,field_index)?")
            && relation_save_validate_compact
                .contains("accepted_row_decode_contract.required_field_for_slot(E::PATH,slot)?")
            && !save_validation_compact.contains(".field_for_slot(primary_key_slot).ok_or_else(")
            && !save_validation_compact.contains(".field_for_slot(field_index).ok_or_else("),
        "accepted typed-save validation must use required accepted row-decode field lookup",
    );
}

#[test]
fn accepted_schema_info_index_membership_uses_persisted_index_contracts() {
    let schema_info = read_source("src/db/schema/info.rs");
    let schema_info_compact = compact_source(&schema_info);
    let schema_snapshot = read_source("src/db/schema/snapshot.rs");

    assert!(
        schema_snapshot
            .contains("pub(in crate::db) const fn indexes(&self) -> &[PersistedIndexSnapshot]")
            && schema_snapshot.contains("pub(in crate::db) enum PersistedIndexKeySnapshot")
            && schema_snapshot.contains("pub(in crate::db) struct PersistedIndexFieldPathSnapshot"),
        "accepted schema snapshots must expose persisted index contracts before SchemaInfo can use accepted index authority",
    );
    assert!(
        schema_info.contains("fn accepted_field_is_indexed(")
            && schema_info.contains("snapshot.indexes().iter().any(|index|")
            && schema_info.contains("indexed: accepted_field_is_indexed(snapshot, field.id()),")
            && schema_info.contains("fn generated_field_is_indexed(")
            && schema_info.contains(
                "field.indexed = generated_field_is_indexed(model, field_name.as_str());"
            )
            && !schema_info.contains("indexed: generated_field_is_indexed(model, field.name()),"),
        "accepted SchemaInfo construction must source index membership from persisted index contracts, leaving generated index membership only on generated schema views",
    );
    assert!(
        schema_info.contains("pub(in crate::db) struct SchemaIndexInfo")
            && schema_info.contains("pub(in crate::db) struct SchemaIndexFieldPathInfo")
            && schema_info.contains(
                "pub(in crate::db) const fn field_path_indexes(&self) -> &[SchemaIndexInfo]",
            )
            && schema_info.contains("fn schema_index_info_from_accepted_index(")
            && schema_info_compact.contains("indexes:snapshot.indexes().iter().map(|index|schema_index_info_from_accepted_index(index,snapshot)).collect(),")
            && schema_info.contains("fn schema_index_info_from_generated_index("),
        "accepted SchemaInfo must expose field-path index metadata from persisted contracts while keeping generated projection isolated to generated schema views",
    );
}

#[test]
fn runtime_visible_indexes_are_accepted_schema_filtered() {
    let access_planner = read_source("src/db/query/plan/access_planner.rs");
    let order_contract = read_source("src/db/query/plan/order_contract.rs");
    let planner_mod = read_source("src/db/query/plan/planner/mod.rs");
    let order_select = read_source("src/db/query/plan/planner/order_select.rs");
    let plan_mod = read_source("src/db/query/plan/mod.rs");
    let plan_mod_compact = compact_source(&plan_mod);
    let executor_explain = read_source("src/db/executor/explain/mod.rs");
    let session_cache = read_source("src/db/session/query/cache.rs");
    let session_cache_compact = compact_source(&session_cache);
    let session_mod = read_source("src/db/session/mod.rs");

    assert!(
        plan_mod.contains("pub(in crate::db) fn accepted_schema_visible(")
            && plan_mod.contains("pub(in crate::db) struct AcceptedPlannerFieldPathIndex")
            && plan_mod.contains("semantic_access_contract: SemanticIndexAccessContract")
            && !plan_mod.contains("generated_index_bridge")
            && plan_mod.contains("accepted_field_path_indexes: Vec<AcceptedPlannerFieldPathIndex>")
            && plan_mod.contains("accepted_schema_info: Option<SchemaInfo>")
            && plan_mod.contains("pub(in crate::db) const fn accepted_schema_info(")
            && plan_mod.contains("accepted_schema_info: Some(schema_info.clone())")
            && plan_mod.contains("AcceptedPlannerFieldPathIndex::from_schema_index")
            && plan_mod.contains("if index.has_expression_key_items() {")
            && plan_mod_compact.contains(
                "schema_info.field_path_indexes().iter().any(|accepted|accepted.name()==index.name())",
            )
            && plan_mod.contains("VisibleIndexAuthority::AcceptedSchema")
            && plan_mod.contains("accepted_field_path_index_count"),
        "VisibleIndexes must carry accepted field-path planner contracts before runtime planning can stop using generated IndexModel authority",
    );
    assert!(
        access_planner.contains("visible_indexes: &VisibleIndexes<'_>,")
            && access_planner.contains("visible_indexes.accepted_field_path_indexes()")
            && access_planner
                .contains("visible_indexes.accepted_field_path_index_count().is_some()")
            && access_planner.contains("plan_access_selection_with_order_and_accepted_indexes("),
        "runtime access planning must use accepted visible-index contracts when accepted authority is present without breaking generated/model-only planning",
    );
    assert!(
        planner_mod.contains("fn plan_access_selection_with_order_and_accepted_indexes(")
            && planner_mod.contains("enum OrderFallbackIndexAuthority")
            && planner_mod.contains("OrderFallbackIndexAuthority::GeneratedModelOnly")
            && planner_mod.contains("OrderFallbackIndexAuthority::AcceptedFieldPathIndexes")
            && planner_mod
                .contains("accepted_field_path_indexes: &[AcceptedPlannerFieldPathIndex],",)
            && planner_mod.contains("order_fallback_selection(")
            && planner_mod.contains("index_range_from_order_with_accepted_indexes(")
            && order_select.contains("let accepted_order_terms = accepted.order_terms();")
            && order_select.contains("accepted.semantic_access_contract()")
            && order_select.contains("whole_index_ordered_range_scan_from_contract(")
            && order_select.contains("deterministic_secondary_index_order_terms_satisfied(",)
            && order_select.contains("grouped_index_order_terms_satisfied(")
            && order_select.contains("index_key_item_order_terms(index_contract.key_items())")
            && order_select
                .contains("let index_contract = SemanticIndexAccessContract::from_index(*index);")
            && order_select.contains("if !index_contract.has_expression_key_items() {")
            && !order_select.contains("deterministic_secondary_index_order_satisfied(")
            && !order_select.contains("grouped_index_order_satisfied(")
            && !order_select.contains("accepted.generated_index_bridge()")
            && order_select.contains("fn index_range_from_order_for_generated_model_only(")
            && !order_select.contains("fn index_range_from_order("),
        "order-only field-path access fallback must match ORDER BY against accepted index terms, build field-path access from reduced contracts, and keep generated order matching only for expression indexes",
    );
    assert!(
        order_contract.contains("fn deterministic_secondary_index_order_terms_satisfied(")
            && order_contract.contains("fn grouped_index_order_terms_satisfied("),
        "accepted field-path order fallback must expose order-contract helpers that consume accepted index terms directly",
    );
    assert!(
        session_cache.contains("fn visible_indexes_for_accepted_schema(")
            && session_cache
                .contains("VisibleIndexes::accepted_schema_visible(model.indexes(), schema_info)")
            && session_cache_compact.contains(
                "SchemaInfo::from_accepted_snapshot_for_model(E::MODEL,&accepted_schema)"
            )
            && !session_cache.contains("fn visible_indexes_for_model("),
        "shared query planning must build visible indexes from accepted SchemaInfo, not raw generated model indexes",
    );
    assert!(
        session_mod.contains("fn visible_indexes_for_store_accepted_schema(")
            && session_mod
                .contains("VisibleIndexes::accepted_schema_visible(model.indexes(), schema_info)")
            && !session_mod.contains("fn visible_indexes_for_store_model("),
        "session explain planning must resolve visible indexes through accepted schema metadata",
    );
    assert!(
        executor_explain.contains("fn finalize_explain_access_choice_for_visible_indexes(")
            && executor_explain.contains("visible_indexes: &VisibleIndexes<'_>,")
            && executor_explain.contains("fn finalize_explain_access_choice_for_model_only(")
            && executor_explain.contains("explain_execution_for_model_only")
            && executor_explain.contains("explain_execution_with_visible_indexes(visible_indexes)")
            && !executor_explain.contains("fn finalize_explain_access_choice_for_visibility(")
            && !executor_explain.contains("None => self.model().indexes()"),
        "runtime explain access-choice finalization must use caller-resolved visible indexes, keeping generated model indexes only on the explicit model-only explain lane",
    );
}

#[test]
fn query_owned_visible_explain_uses_accepted_schema_info() {
    let executor_explain = read_source("src/db/executor/explain/mod.rs");

    assert!(
        executor_explain
            .contains("if let Some(schema_info) = visible_indexes.accepted_schema_info()")
            && executor_explain.contains(
                "plan.finalize_access_choice_for_model_with_accepted_indexes_and_schema("
            )
            && executor_explain.contains("visible_indexes.accepted_field_path_indexes()"),
        "query-owned visible-index explain must reuse accepted SchemaInfo when visibility was derived from accepted schema",
    );
}

#[test]
fn runtime_access_choice_projection_uses_accepted_visible_indexes() {
    let access_choice = read_source("src/db/query/plan/access_choice/mod.rs");
    let access_plan = read_source("src/db/query/plan/access_plan.rs");
    let pipeline = read_source("src/db/query/plan/pipeline.rs");
    let session_query_explain = read_source("src/db/session/query/explain.rs");
    let session_sql_explain = read_source("src/db/session/sql/execute/explain.rs");

    assert!(
        access_choice.contains("fn rerank_access_plan_by_residual_burden_with_accepted_indexes(")
            && access_choice.contains(
                "fn project_access_choice_explain_snapshot_with_accepted_indexes_and_schema(",
            )
            && access_choice.contains("plan_access_selection_with_order_and_accepted_indexes(")
            && pipeline.contains("rerank_access_plan_by_residual_burden_with_accepted_indexes(")
            && access_plan
                .contains("fn finalize_access_choice_for_model_with_accepted_indexes_and_schema(",)
            && session_query_explain
                .contains("finalize_access_choice_for_model_with_accepted_indexes_and_schema(")
            && session_sql_explain
                .contains("finalize_access_choice_for_model_with_accepted_indexes_and_schema("),
        "runtime access-choice reranking and explain projection must rebuild candidate access with accepted field-path contracts when accepted authority is present",
    );
}

#[test]
fn save_preflight_relations_use_accepted_contracts() {
    let save_validation = read_source("src/db/executor/mutation/save_validation.rs");
    let relation_save_validate = read_source("src/db/relation/save_validate.rs");

    assert!(
        save_validation.contains("validate_save_strong_relations_with_accepted_contract::<E>(")
            && save_validation.contains("self.accepted_row_decode_contract(),")
            && !save_validation.contains("validate_save_strong_relations::<E>(&self.db, entity)?"),
        "save relation preflight must use accepted row contracts instead of reopening E::MODEL relation metadata",
    );
    assert!(
        relation_save_validate
            .contains("validate_save_strong_relations_with_accepted_contract<E>",)
            && !relation_save_validate.contains("strong_relations_for_model_iter")
            && !relation_save_validate.contains("E::MODEL"),
        "save relation validation must derive relation metadata from accepted row contracts",
    );
}

#[test]
fn reverse_relation_runtime_paths_use_accepted_contracts() {
    let reverse_index = read_source("src/db/relation/reverse_index.rs");
    let delete_validate = read_source("src/db/relation/validate.rs");
    let runtime_hooks = read_source("src/db/runtime_hooks/mod.rs");
    let commit_prepare = read_source("src/db/commit/prepare.rs");

    assert!(
        reverse_index.contains("accepted_strong_relations_for_row_contract(")
            && reverse_index.contains("source_row_contract: StructuralRowContract,")
            && !reverse_index.contains("strong_relations_for_model_iter")
            && !reverse_index.contains("source_model: &'static EntityModel"),
        "reverse-index mutation preparation must derive relation fields from accepted row contracts",
    );
    assert!(
        delete_validate.contains("accepted_strong_relations_for_row_contract(")
            && delete_validate.contains("accepted_source_row_contract::<S>(")
            && !delete_validate.contains("model_has_strong_relations_to_target(S::MODEL"),
        "delete-side relation validation must derive relation fields from accepted row contracts",
    );
    assert!(
        !runtime_hooks.contains("model_has_strong_relations_to_target("),
        "delete hook traversal must not use generated model relation metadata as a runtime prefilter",
    );
    assert!(
        commit_prepare
            .contains("row_contract.clone(),\n        structural.data_key.storage_key(),"),
        "commit reverse-index preparation must receive the accepted structural row contract",
    );
}

#[test]
fn forward_index_write_keys_use_accepted_row_contract_slots() {
    let entity_authority = read_source("src/db/executor/authority/entity.rs");
    let commit_prepare = read_source("src/db/commit/prepare.rs");
    let index_key_build = read_source("src/db/index/key/build.rs");
    let index_plan = read_source("src/db/index/plan/mod.rs");
    let index_plan_read = read_source("src/db/index/plan/read.rs");
    let index_readers = read_source("src/db/index/readers.rs");
    let unique_plan = read_source("src/db/index/plan/unique.rs");
    let structural_row = read_source("src/db/data/structural_row.rs");

    assert!(
        structural_row.contains("pub(in crate::db) fn field_slot_index_by_name(")
            && structural_row.contains("self.accepted_decode_contract.is_some()"),
        "structural row contracts must expose accepted-first field-name to slot lookup",
    );
    assert!(
        index_key_build.contains("pub(crate) fn new_from_slots_with_contract(")
            && index_key_build.contains("row_contract.field_slot_index_by_name(field)?")
            && index_key_build
                .contains("pub(crate) fn new_from_slots_with_accepted_field_path_index")
            && index_key_build.contains("fn build_accepted_field_path_index_key_from_slots(")
            && index_key_build.contains("accepted_field_path_component_bytes_from_slots(")
            && !index_key_build.contains("pub(crate) fn new_from_slots(\n")
            && !index_key_build.contains("compile_scalar_index_key_item_program("),
        "write-time field-path index key construction must resolve key shape through accepted index contracts and slots through accepted row contracts",
    );
    assert!(
        index_key_build.contains("pub(crate) fn new_from_slot_ref_reader_with_access_contract")
            && index_key_build.contains("schema_info.field_slot_index(field)")
            && index_key_build.contains("build_index_key_from_access_contract(")
            && index_key_build
                .contains("pub(crate) fn new_from_slot_ref_reader_with_accepted_field_path_index")
            && index_key_build.contains("accepted_index.fields()")
            && index_key_build.contains("IndexId::new(entity_tag, accepted_index.ordinal())")
            && index_key_build.contains("IndexId::new(entity_tag, index.ordinal())")
            && !index_key_build.contains("pub(crate) fn new_from_slot_ref_reader_with_schema")
            && !index_key_build.contains("pub(crate) fn new_from_slot_ref_reader("),
        "cursor anchor and predicate index-key paths must use accepted schema/index/row-contract slot authority instead of generated model slot lookup",
    );
    assert!(
        entity_authority.contains("index_range_anchor_key_from_slot_ref_reader")
            && entity_authority.contains(
                "field-path index cursor anchor derivation requires accepted index contract"
            )
            && entity_authority.contains(".field_path_indexes()")
            && entity_authority
                .contains("IndexKey::new_from_slot_ref_reader_with_accepted_field_path_index(")
            && entity_authority
                .contains("IndexKey::new_from_slot_ref_reader_with_access_contract(")
            && entity_authority.contains("let index = index_range.index();")
            && entity_authority.contains("if index.has_expression_key_items() {")
            && !entity_authority.contains("fn index_key_from_slot_ref_reader"),
        "runtime field-path cursor anchors must use accepted index contracts while expression indexes stay on the explicit generated deferred lane",
    );
    assert!(
        index_plan.contains("IndexKey::new_from_slots_with_contract(")
            && index_plan.contains("accepted_field_path_index_for_generated_index(")
            && index_plan.contains("accepted_index: Option<&SchemaIndexInfo>")
            && index_plan.contains("IndexKey::new_from_slots_with_accepted_field_path_index(")
            && index_plan.contains("fn accepted_index_fields_csv(")
            && index_plan.contains("SchemaIndexInfo::store")
            && index_plan.contains("SchemaIndexInfo::unique")
            && index_plan.contains("PredicateProgram::compile_with_row_contract(")
            && index_plan.contains("row_contract,")
            && !index_plan.contains("IndexKey::new_from_slots("),
        "forward-index mutation planning must pass accepted index and row contracts into field-path index key, store, uniqueness, and predicate construction",
    );
    assert!(
        index_plan_read.contains("index: IndexReadContract<'_>")
            && !index_plan_read.contains("index: &IndexModel")
            && index_readers.contains("pub(in crate::db) struct IndexReadContract")
            && index_readers.contains("unique: bool")
            && index_readers.contains("fields: &'a str")
            && !index_readers.contains("model::index::IndexModel"),
        "preflight index readers must consume reduced accepted index contract facts instead of generated IndexModel definitions",
    );
    assert!(
        unique_plan.contains("IndexKey::new_from_slots_with_contract(")
            && unique_plan.contains("accepted_index: Option<&SchemaIndexInfo>")
            && unique_plan.contains("read_contract: IndexReadContract<'_>")
            && unique_plan.contains("read_contract.unique()")
            && unique_plan.contains("IndexKey::new_from_slots_with_accepted_field_path_index(")
            && unique_plan.contains("row_contract,"),
        "unique-index validation must rebuild stored field-path index keys through accepted index and row contracts and consume accepted uniqueness",
    );
    assert!(
        commit_prepare.contains("struct AcceptedCommitSchemaContracts")
            && commit_prepare.contains("accepted_commit_schema_contracts(")
            && commit_prepare.contains("SchemaInfo::from_accepted_snapshot_for_model(")
            && commit_prepare.contains("&schema_contracts.schema_info")
            && commit_prepare.contains("&schema_contracts.row_contract")
            && commit_prepare.contains("has_accepted_field_path_indexes")
            && commit_prepare
                .contains("schema_contracts.schema_info.field_path_indexes().is_empty()")
            && commit_prepare.contains("has_deferred_expression_indexes")
            && commit_prepare.contains("index.has_expression_key_items()")
            && !commit_prepare.contains("authority.model.indexes().is_empty()"),
        "commit preflight must carry accepted schema info beside the accepted row contract and gate field-path forward-index planning on accepted index contracts",
    );
}

#[test]
fn predicate_fast_paths_use_accepted_scalar_slot_helpers() {
    let predicate_runtime = read_source("src/db/predicate/runtime/mod.rs");

    assert!(
        predicate_runtime.contains("slots.field_leaf_codec(field_slot)")
            && predicate_runtime.contains("slots.required_value_storage_scalar(field_slot)")
            && !predicate_runtime.contains("slots\n        .field_decode_contract(field_slot)")
            && !predicate_runtime.contains("slots.field_decode_contract(field_slot)"),
        "conditional-index predicate fast paths must use accepted-aware scalar slot helpers",
    );
}

#[test]
fn lowered_executor_scans_use_reduced_index_scan_facts() {
    let access_lowering = read_source("src/db/access/lowering.rs");
    let index_scan = read_source("src/db/executor/stream/access/scan.rs");
    let physical_access = read_source("src/db/executor/stream/access/physical.rs");

    assert!(
        access_lowering.contains("pub(in crate::db) struct LoweredIndexScanContract")
            && access_lowering.contains("store_path: &'static str")
            && access_lowering.contains("scan_contract: LoweredIndexScanContract")
            && access_lowering.contains("pub(in crate::db) const fn scan_contract(")
            && access_lowering.contains("pub(in crate::db) const fn store_path(")
            && !access_lowering.contains(
                "pub(in crate::db) struct LoweredIndexPrefixSpec {\n    index: IndexModel"
            )
            && !access_lowering.contains(
                "pub(in crate::db) struct LoweredIndexRangeSpec {\n    index: IndexModel"
            )
            && index_scan.contains("index: LoweredIndexScanContract")
            && index_scan.contains("spec.scan_contract()")
            && index_scan.contains("index.unique()")
            && index_scan.contains("index.name()")
            && physical_access.contains("index: LoweredIndexScanContract")
            && physical_access.contains("spec.scan_contract()"),
        "lowered executor scan specs must reduce generated index models to scan facts after raw bounds are materialized",
    );
}

#[test]
fn runtime_access_capabilities_use_reduced_index_shape_facts() {
    let access_capabilities = read_source("src/db/access/capabilities.rs");
    let execution_path_payload = read_source("src/db/access/execution_contract/types.rs");
    let route_pushdown = read_source("src/db/executor/planning/route/pushdown.rs");
    let aggregate_capability = read_source("src/db/executor/aggregate/capability.rs");
    let logical_semantics = read_source("src/db/query/plan/semantics/logical.rs");
    let access_plan = read_source("src/db/query/plan/access_plan.rs");
    let access_path = read_source("src/db/access/path.rs");
    let access_plan_core = read_source("src/db/access/plan.rs");
    let access_choice = read_source("src/db/query/plan/access_choice/mod.rs");
    let cache_key = read_source("src/db/query/intent/cache_key.rs");
    let planner_prefix = read_source("src/db/query/plan/planner/prefix.rs");
    let planner_compare = read_source("src/db/query/plan/planner/compare.rs");
    let planner_range = read_source("src/db/query/plan/planner/range/extract.rs");

    assert!(
        access_capabilities.contains("pub(in crate::db) struct IndexShapeDetails")
            && access_capabilities.contains("name: &'static str")
            && access_capabilities.contains("ordinal: u16")
            && access_capabilities.contains("unique: bool")
            && access_capabilities.contains("key_items: IndexKeyItemsRef")
            && !access_capabilities.contains("index: IndexModel,\n    slot_arity: usize"),
        "runtime access capabilities must carry reduced index shape facts instead of exposing generated IndexModel",
    );
    assert!(
        execution_path_payload.contains("index: IndexShapeDetails")
            && execution_path_payload.contains("IndexShapeDetails::from_access_contract(")
            && !execution_path_payload.contains("model::index::IndexModel")
            && !execution_path_payload.contains("index: IndexModel"),
        "executable access payloads must carry reduced index shape facts instead of generated IndexModel",
    );
    assert!(
        route_pushdown.contains("details.key_items()")
            && route_pushdown.contains("details.key_arity()")
            && !route_pushdown.contains("model::index::IndexModel")
            && aggregate_capability.contains("index: Option<IndexShapeDetails>")
            && !aggregate_capability.contains("index::IndexModel"),
        "route and aggregate capability checks must consume reduced index facts",
    );
    assert!(
        logical_semantics.contains("index_key_items_for_slot_map()")
            && !logical_semantics.contains("selected_index_model()?;"),
        "logical runtime semantics must compile index slot targets from reduced executable access facts",
    );
    assert!(
        access_plan.contains("access.has_selected_index_access_path()")
            && !access_plan.contains("access.selected_index_model().is_some()"),
        "access-planned query shells must detect index-backed shapes through reduced access capabilities",
    );
    assert!(
        access_path.contains("pub(crate) struct SemanticIndexAccessContract")
            && access_path.contains("fn from_access_contract(")
            && access_path.contains("index: SemanticIndexAccessContract")
            && access_path.contains(
                "pub(crate) struct SemanticIndexRangeSpec {\n    index: SemanticIndexAccessContract"
            )
            && !access_path.contains("IndexPrefix {\n        index: IndexModel")
            && !access_path.contains("IndexMultiLookup {\n        index: IndexModel")
            && !access_path.contains("selected_index_model")
            && !access_path.contains("as_index_prefix(&self)")
            && !access_path.contains("as_index_multi_lookup(&self)")
            && logical_semantics
                .contains("residual_query_predicate_after_filtered_access_contract")
            && !logical_semantics.contains("selected_index_model")
            && access_choice.contains("selected_index_contract()")
            && !access_choice.contains("selected_index_model")
            && cache_key.contains("as_index_prefix_contract()")
            && cache_key.contains("as_index_multi_lookup_contract()"),
        "selected runtime access-path consumers must use reduced semantic index contracts instead of selected generated IndexModel accessors",
    );
    assert!(
        access_plan_core.contains("fn index_prefix_from_contract(")
            && access_plan_core.contains("fn index_multi_lookup_from_contract(")
            && !access_plan_core.contains("fn index_prefix(index: IndexModel")
            && !access_plan_core.contains("fn index_multi_lookup(index: IndexModel")
            && planner_prefix.contains("AccessPlan::index_prefix_from_contract(")
            && planner_prefix.contains("AccessPlan::index_multi_lookup_from_contract(")
            && planner_compare.contains("SemanticIndexRangeSpec::from_access_contract(")
            && planner_range.contains("SemanticIndexRangeSpec::from_access_contract(")
            && !planner_prefix.contains("AccessPlan::index_prefix(*index")
            && !planner_prefix.contains("AccessPlan::index_multi_lookup(**index")
            && !planner_compare.contains("SemanticIndexRangeSpec::new(\n            *index")
            && !planner_range.contains("SemanticIndexRangeSpec::new(*index"),
        "selected predicate planner construction must convert chosen index candidates into reduced semantic contracts before building access paths",
    );
}

#[test]
fn access_choice_candidate_scores_use_reduced_index_contract_facts() {
    let access_path = read_source("src/db/access/path.rs");
    let evaluator = read_source("src/db/query/plan/access_choice/evaluator/mod.rs");
    let evaluator_prefix = read_source("src/db/query/plan/access_choice/evaluator/prefix.rs");
    let evaluator_range = read_source("src/db/query/plan/access_choice/evaluator/range.rs");
    let planner_ranking = read_source("src/db/query/plan/planner/ranking.rs");
    let planner_prefix = read_source("src/db/query/plan/planner/prefix.rs");
    let planner_compare = read_source("src/db/query/plan/planner/compare.rs");
    let planner_range = read_source("src/db/query/plan/planner/range/extract.rs");
    let planner_order_select = read_source("src/db/query/plan/planner/order_select.rs");

    assert!(
        access_path.contains("pub(in crate::db) const fn is_filtered(")
            && planner_ranking.contains("fn access_candidate_score_from_index_contract(")
            && planner_ranking.contains("index.is_filtered()")
            && planner_ranking.contains("selected_index_contract_satisfies_secondary_order(")
            && evaluator.contains("SemanticIndexAccessContract::from_index(*index)")
            && evaluator.contains("access_candidate_score_from_index_contract(")
            && !evaluator.contains("candidate_satisfies_secondary_order(")
            && evaluator_prefix.contains("SemanticIndexAccessContract::from_index(*index)")
            && evaluator_prefix.contains("index_contract.key_arity()")
            && evaluator_prefix.contains("index_contract.is_filtered()")
            && evaluator_range.contains("SemanticIndexAccessContract::from_index(*index)")
            && evaluator_range.contains("index_contract.is_filtered()")
            && evaluator_range.contains("single_range_compare_bound_count(index_contract")
            && !evaluator.contains("index.predicate().is_some()")
            && !evaluator_prefix.contains("index.predicate().is_some()")
            && !evaluator_prefix.contains("leading_index_key_item")
            && !evaluator_prefix.contains("index.key_items()")
            && !evaluator_range.contains("index.predicate().is_some()")
            && !evaluator_range.contains("leading_index_key_item")
            && !evaluator_range.contains("index_key_item_count")
            && !evaluator_range.contains("index_key_item_at")
            && !evaluator_range.contains("index.fields()")
            && !evaluator_range.contains("index.is_field_indexable"),
        "access-choice candidate scoring must derive filtered and arity facts from reduced semantic index contracts instead of reopening generated IndexModel predicates",
    );
    assert!(
        planner_prefix.contains("access_candidate_score_from_index_contract(")
            && planner_compare.contains("access_candidate_score_from_index_contract(")
            && planner_range.contains("access_candidate_score_from_index_contract(")
            && planner_order_select.contains("index_contract.has_expression_key_items()")
            && !planner_prefix.contains("index.predicate().is_some()")
            && !planner_compare.contains("index.predicate().is_some()")
            && !planner_range.contains("index.predicate().is_some()")
            && !planner_prefix.contains("candidate_satisfies_secondary_order(")
            && !planner_compare.contains("candidate_satisfies_secondary_order(")
            && !planner_range.contains("candidate_satisfies_secondary_order(")
            && !planner_prefix.contains("leading_index_key_item")
            && !planner_prefix.contains("index.key_items()")
            && !planner_compare.contains("leading_index_key_item")
            && !planner_compare.contains("index.fields()")
            && !planner_compare.contains("index.is_field_indexable")
            && !planner_range.contains("index.key_items()")
            && !planner_range.contains("index.fields()")
            && !planner_range.contains("index.is_field_indexable"),
        "planner candidate ranking and key-shape extraction must use reduced semantic index contracts",
    );
}

#[test]
fn global_distinct_grouped_runtime_keeps_prepared_authority() {
    let grouped_entrypoints = read_source("src/db/executor/pipeline/entrypoints/grouped.rs");
    let aggregate_distinct = read_source("src/db/executor/aggregate/distinct.rs");
    let aggregate_numeric = read_source("src/db/executor/aggregate/numeric/mod.rs");
    let aggregate_execution = read_source("src/db/executor/aggregate/execution.rs");

    assert!(
        grouped_entrypoints.contains(
            "fn grouped_path_runtime(\n        &self,\n        authority: EntityAuthority,"
        ) && grouped_entrypoints.contains("self.grouped_path_runtime(authority)?")
            && !grouped_entrypoints
                .contains("let authority = EntityAuthority::for_generated_type_for_test::<E>();"),
        "grouped runtime preparation must consume prepared accepted authority instead of reopening generated authority",
    );
    assert!(
        aggregate_execution.contains("GlobalDistinct {\n        authority: EntityAuthority,")
            && aggregate_numeric.contains("let authority = plan.authority();")
            && aggregate_distinct
                .contains("self.prepare_grouped_route_runtime(route, authority, None, None)?"),
        "global DISTINCT aggregate execution must carry prepared accepted authority into grouped runtime",
    );
}

#[test]
fn generated_only_prepared_plan_constructor_is_test_only() {
    let prepared_plan = read_source("src/db/executor/prepared_execution_plan/mod.rs");
    let entity_authority = read_source("src/db/executor/authority/entity.rs");
    let executor_mod = read_source("src/db/executor/mod.rs");
    let query_intent = read_source("src/db/query/intent/query.rs");
    let save_mod = read_source("src/db/executor/mutation/save/mod.rs");
    let save_validation = read_source("src/db/executor/mutation/save_validation.rs");
    let save_typed = read_source("src/db/executor/mutation/save/typed.rs");
    let save_batch = read_source("src/db/executor/mutation/save/batch.rs");
    let save_structural = read_source("src/db/executor/mutation/save/structural.rs");
    let session_mod = read_source("src/db/session/mod.rs");
    let session_query_cache = read_source("src/db/session/query/cache.rs");

    assert!(
        prepared_plan.contains("#[cfg(test)]\n    pub(in crate::db) fn new(")
            && prepared_plan.contains("#[cfg(test)]\n    fn build(")
            && prepared_plan.contains("EntityAuthority::for_generated_type_for_test::<E>()")
            && prepared_plan.contains(".with_cursor_schema_info_for_test(")
            && entity_authority.contains(
                "#[cfg(test)]\n    pub(in crate::db) const fn for_generated_type_for_test"
            )
            && !entity_authority.contains("pub const fn for_type")
            && executor_mod.contains(
                "#[cfg(test)]\nimpl<E> From<CompiledQuery<E>> for PreparedExecutionPlan<E>"
            ),
        "generated-only PreparedExecutionPlan constructors must stay test-only so runtime plans use accepted-backed shared authority",
    );
    assert!(
        query_intent.contains("#[cfg(test)]\n    pub(in crate::db) fn into_plan("),
        "compiled-query plan extraction must remain test-only with the generated-only prepared-plan conversion",
    );
    assert!(
        save_mod.contains("accepted_schema_info: SchemaInfo,")
            && save_mod
                .contains("pub(in crate::db::executor::mutation) const fn accepted_schema_info(")
            && save_validation.contains("schema: &SchemaInfo,")
            && save_typed.contains("let schema = self.accepted_schema_info();")
            && save_batch.contains("let schema = self.accepted_schema_info();")
            && save_structural.contains("let schema = self.accepted_schema_info();")
            && !save_mod.contains("SchemaInfo::cached_for_generated_entity_model(E::MODEL)")
            && !save_validation.contains("SchemaInfo::cached_for_generated_entity_model(E::MODEL)")
            && !save_typed.contains("Self::schema_info()")
            && !save_batch.contains("Self::schema_info()")
            && !save_structural.contains("Self::schema_info()")
            && !save_validation
                .contains("EntityAuthority::for_generated_type_for_test::<E>().schema_info()"),
        "save validation metadata lookup must use session-selected accepted SchemaInfo instead of reopening generated schema authority",
    );
    assert!(
        session_mod.contains("self.db.recovered_store(E::Store::PATH)?")
            && session_mod.contains("ensure_accepted_schema_snapshot(schema_store, E::ENTITY_TAG, E::PATH, E::MODEL)")
            && session_mod.contains("SchemaInfo::from_accepted_snapshot_for_model(E::MODEL, &accepted_schema)")
            && session_mod.contains("self.save_executor::<E>(contract, schema_info, schema_fingerprint)")
            && !session_mod.contains(
                "self.ensure_accepted_schema_snapshot_for_authority(&EntityAuthority::for_generated_type_for_test::<E>())",
            ),
        "session save bootstrap must pass accepted schema metadata into SaveExecutor without constructing generated executor authority",
    );
    assert!(
        session_query_cache
            .contains("query.try_build_trivial_scalar_load_plan_with_schema_info(schema_info)?")
            && !session_query_cache
                .contains("query.try_build_trivial_scalar_load_plan_for_model_only()?"),
        "shared query cache trivial scalar fast path must finalize executor metadata with accepted SchemaInfo instead of generated schema fallback",
    );
}

#[test]
fn session_explain_access_choice_uses_accepted_schema_info() {
    let session_query_explain = read_source("src/db/session/query/explain.rs");
    let session_query_explain_compact = compact_source(&session_query_explain);
    let session_sql_explain = read_source("src/db/session/sql/execute/explain.rs");
    let session_sql_explain_compact = compact_source(&session_sql_explain);
    let sql_aggregate_binding = read_source("src/db/sql/lowering/aggregate/command/binding.rs");
    let query_access_plan = read_source("src/db/query/plan/access_plan.rs");

    assert!(
        session_query_explain.contains(
            "plan.finalize_access_choice_for_model_with_accepted_indexes_and_schema("
        )
            && query_access_plan.contains(
                "fn finalize_access_choice_for_model_only_with_indexes("
            )
            && query_access_plan.contains(
                "fn finalize_access_choice_for_model_with_accepted_indexes_and_schema("
            )
            && session_query_explain_compact.contains(
                "SchemaInfo::from_accepted_snapshot_for_model(query.structural().model(),&accepted_schema,"
            )
            && !session_query_explain.contains("plan.finalize_access_choice_for_model_only_with_indexes(")
            && session_sql_explain.contains(
                "plan.finalize_access_choice_for_model_with_accepted_indexes_and_schema("
            )
            && session_sql_explain
                .contains("bind_lowered_sql_query_structural_with_schema(")
            && session_sql_explain.contains(
                "bind_lowered_sql_explain_global_aggregate_structural_with_schema("
            )
            && session_sql_explain_compact.contains(
                "SchemaInfo::from_accepted_snapshot_for_model(authority.model(),accepted_schema)"
            )
            && !session_sql_explain.contains("plan.finalize_access_choice_for_model_only_with_indexes(")
            && !session_sql_explain.contains("bind_lowered_sql_query_structural(")
            && !session_sql_explain
                .contains("bind_lowered_sql_explain_global_aggregate_structural(")
            && sql_aggregate_binding.contains(
                "apply_lowered_base_query_shape_with_schema("
            ),
        "session explain binding and access-choice finalization must use accepted SchemaInfo instead of generated schema fallback",
    );
}

#[test]
fn standalone_generated_query_planning_is_model_only() {
    let query_plan_pipeline = read_source("src/db/query/plan/pipeline.rs");
    let schema_info = read_source("src/db/schema/info.rs");

    assert!(
        query_plan_pipeline.contains("fn build_query_model_plan_for_model_only")
            && query_plan_pipeline
                .contains("fn build_query_model_plan_with_indexes_for_model_only")
            && query_plan_pipeline.contains("fn try_build_trivial_scalar_load_plan_for_model_only")
            && query_plan_pipeline
                .contains("fn prepare_query_model_scalar_planning_state_for_model_only")
            && query_plan_pipeline
                .contains("SchemaInfo::cached_for_generated_entity_model(query.model()).clone()")
            && schema_info.contains("fn cached_for_generated_entity_model(")
            && !schema_info.contains("fn cached_for_entity_model(")
            && query_plan_pipeline
                .contains("fn prepare_query_model_scalar_planning_state_with_schema_info")
            && !query_plan_pipeline.contains("fn build_query_model_plan<K>")
            && !query_plan_pipeline.contains("fn build_query_model_plan_with_indexes<K>")
            && !query_plan_pipeline.contains("fn prepare_query_model_scalar_planning_state<'"),
        "standalone generated-schema query planning wrappers must stay explicit model-only surfaces",
    );
}

#[test]
fn executor_plan_validation_uses_accepted_schema_info() {
    let access_mod = read_source("src/db/access/mod.rs");
    let access_validate = read_source("src/db/access/validate.rs");
    let entity_authority = read_source("src/db/executor/authority/entity.rs");

    assert!(
        entity_authority.contains("if !plan.has_static_planning_shape()")
            && entity_authority
                .contains("executor plan validation requires planner-frozen static shape",)
            && entity_authority.contains("executor plan validation requires accepted schema info")
            && entity_authority.contains("validate_access_runtime_invariants_with_schema(")
            && !entity_authority.contains("fn schema_info(")
            && !entity_authority
                .contains("SchemaInfo::cached_for_generated_entity_model(self.model)")
            && !entity_authority.contains("validate_access_runtime_invariants_model(")
            && !entity_authority.contains("validate_access_structure_model(self.schema_info()"),
        "executor plan validation must require planner-frozen static shape and authority-carried accepted schema info instead of reopening generated schema authority",
    );
    assert!(
        access_mod.contains("validate_access_runtime_invariants_with_schema")
            && access_validate.contains("schema.field_is_indexed(field)")
            && !access_validate.contains("fn validate_index_reference_model("),
        "runtime access validation must check index references through schema info instead of generated entity model membership",
    );
}

#[test]
fn raw_entity_authority_bootstrap_stays_layout_free() {
    let entity_authority = read_source("src/db/executor/authority/entity.rs");
    let executor_explain = read_source("src/db/executor/explain/mod.rs");
    let route_shape = read_source("src/db/executor/planning/route/contracts/shape.rs");
    let query_plan_covering = read_source("src/db/query/plan/covering/mod.rs");
    let session_query_explain = read_source("src/db/session/query/explain.rs");
    let prepared_plan = read_source("src/db/executor/prepared_execution_plan/mod.rs");
    let session_sql_explain = read_source("src/db/session/sql/execute/explain.rs");

    assert!(
        entity_authority.contains("row_layout: Option<RowLayout>,")
            && entity_authority.contains("row_layout: None,")
            && entity_authority.contains("fn with_generated_row_layout_for_test(")
            && entity_authority.contains("row_layout: Some(RowLayout::from_generated_model_for_test(self.model))")
            && entity_authority.contains(
                "entity authority row layout must be selected from accepted schema or explicit test layout",
            )
            && !entity_authority.contains("row_layout: RowLayout::from_generated_model_for_test(model)"),
        "raw EntityAuthority bootstrap must not attach generated row layout outside explicit test layout construction",
    );
    assert!(
        prepared_plan.contains("assemble_load_execution_node_descriptor_for_authority(")
            && !prepared_plan.contains("self.authority.fields(),")
            && executor_explain.contains("explain_execution_descriptor_from_plan_with_authority(")
            && executor_explain.contains(
                "finalized_execution_diagnostics_from_plan_with_authority_and_descriptor_mutator("
            )
            && session_query_explain
                .contains("accepted_entity_authority_for_schema::<E>(&accepted_schema)")
            && session_query_explain.contains(
                ".explain_execution_descriptor_from_plan_with_authority(&plan, &authority)"
            )
            && session_query_explain.contains(
                ".finalized_execution_diagnostics_from_plan_with_authority_and_descriptor_mutator("
            )
            && executor_explain.contains("explain_execution_descriptor_from_model_only_plan(")
            && executor_explain.contains("freeze_load_execution_route_facts_for_model_only(")
            && !executor_explain.contains("freeze_load_execution_route_facts(")
            && !session_query_explain
                .contains(".explain_execution_descriptor_from_model_only_plan(&plan)")
            && session_sql_explain.contains("freeze_load_execution_route_facts_for_authority(")
            && session_sql_explain.contains(
                ".finalized_execution_diagnostics_from_plan_with_authority_and_descriptor_mutator("
            )
            && !session_sql_explain.contains("authority.fields(),"),
        "prepared/session descriptors and SQL EXPLAIN route facts must consume accepted authority instead of split generated field metadata",
    );
    assert!(
        entity_authority.contains("covering_read_execution_plan_with_schema_info(")
            && entity_authority.contains("covering_hybrid_projection_plan_with_schema_info(")
            && !entity_authority.contains("covering_read_execution_plan_from_fields(")
            && !entity_authority.contains("covering_hybrid_projection_plan_from_fields(")
            && query_plan_covering.contains("fn resolve_covering_field_slot_with_schema(")
            && query_plan_covering.contains("schema.field_slot_index(field_name)?")
            && query_plan_covering.contains("schema.field_kind(field_name).copied()"),
        "authority-owned covering read and hybrid projection planning must resolve projected field slots from accepted SchemaInfo",
    );
    assert!(
        entity_authority.contains("pub(in crate::db) fn aggregate_route_shape")
            || entity_authority.contains("pub(in crate::db) fn aggregate_route_shape<'")
    );
    assert!(
        entity_authority.contains("AggregateRouteShape::new_from_schema_info(")
            && executor_explain
                .contains(".aggregate_route_shape(kind, strategy.explain_projected_field())")
            && !executor_explain
                .contains("strategy.explain_projected_field(),\n            E::MODEL.fields(),")
            && session_sql_explain.contains(".aggregate_route_shape(")
            && !session_sql_explain.contains("model.fields(),")
            && route_shape.contains("pub(in crate::db) fn new_from_schema_info(")
            && route_shape.contains("schema.field_slot_index(target_field)")
            && route_shape.contains(".primary_key_name()"),
        "aggregate execution explain route shapes must use accepted SchemaInfo instead of generated field tables",
    );
}

#[test]
fn generated_row_contract_runtime_fallbacks_are_test_only() {
    let persisted_row_contract = read_source("src/db/data/persisted_row/contract.rs");
    let persisted_row_patch = read_source("src/db/data/persisted_row/patch.rs");

    assert!(
        persisted_row_contract
            .contains("#[cfg(test)]\nfn decode_runtime_value_from_generated_row_contract(")
            && persisted_row_contract
                .contains("#[cfg(test)]\nfn decode_scalar_slot_value_from_generated_row_contract",)
            && persisted_row_contract.contains(
                "#[cfg(test)]\nfn validate_non_scalar_slot_value_with_generated_row_contract(",
            )
            && persisted_row_contract.contains(
                "#[cfg(test)]\npub(in crate::db::data::persisted_row) fn canonical_row_from_runtime_value_source_with_generated_contract",
            )
            && persisted_row_contract
                .contains("#[cfg(not(test))]\nfn generated_row_contract_reached_runtime_boundary(")
            && persisted_row_contract.contains("requires accepted row contract for entity",),
        "row-contract runtime decode/validation must fail closed in production instead of falling back to generated field metadata",
    );
    assert!(
        persisted_row_patch.contains(
            "#[cfg(test)]\nfn canonical_row_from_structural_slot_reader_with_generated_contract(",
        ) && persisted_row_patch
            .contains("raw row canonicalization requires accepted row contract for entity",),
        "raw-row canonicalization must not retain a production generated-contract fallback",
    );
}

#[test]
fn sql_command_lowering_uses_accepted_schema_for_runtime_explain() {
    let sql_compile_core = read_source("src/db/session/sql/compile/core.rs");
    let sql_lowering_prepare = read_source("src/db/sql/lowering/prepare.rs");
    let sql_lowering_select = read_source("src/db/sql/lowering/select/mod.rs");
    let sql_global_aggregate_binding =
        read_source("src/db/sql/lowering/aggregate/command/binding.rs");

    assert!(
        sql_compile_core.contains("Self::compile_explain(statement, entity_name, model, schema)",)
            && sql_compile_core.contains(
                "lower_sql_command_from_prepared_statement_with_schema(prepared, model, schema)",
            )
            && !sql_compile_core
                .contains("lower_sql_command_from_prepared_statement(prepared, model)"),
        "runtime SQL EXPLAIN compilation must lower with accepted SchemaInfo instead of generated model schema fallback",
    );
    assert!(
        sql_lowering_prepare.contains(
            "#[cfg(test)]\npub(crate) fn lower_sql_command_from_prepared_statement_for_model_only("
        ) && sql_lowering_prepare
            .contains("pub(crate) fn lower_sql_command_from_prepared_statement_with_schema(")
            && sql_lowering_prepare.contains("fn lower_prepared_statement_for_model_only(")
            && sql_lowering_prepare.contains("fn lower_prepared_statement_with_schema(")
            && sql_lowering_prepare.contains("fn lower_explain_select_prepared_for_model_only(")
            && sql_lowering_prepare.contains("fn lower_explain_select_prepared_with_schema(")
            && sql_lowering_prepare
                .contains("lower_select_shape_with_schema(statement.clone(), model, schema)"),
        "shared SQL command lowering must keep generated-schema command lowering test-only/model-only and expose an accepted-schema runtime path",
    );
    assert!(
        sql_lowering_select.contains(
            "#[cfg(test)]\npub(in crate::db::sql::lowering) fn lower_select_shape_for_model_only("
        ) && sql_lowering_select
            .contains("pub(in crate::db::sql::lowering) fn lower_select_shape_with_schema(",),
        "generated-schema SELECT lowering must remain test-only/model-only while runtime callers use explicit SchemaInfo",
    );
    assert!(
        sql_global_aggregate_binding.contains("#[cfg(test)]\n    fn into_typed_for_model_only")
            && sql_global_aggregate_binding
                .contains("compile_sql_global_aggregate_command_for_model_only")
            && sql_global_aggregate_binding
                .contains("compile_sql_global_aggregate_command_from_prepared_for_model_only")
            && sql_global_aggregate_binding
                .contains("bind_lowered_sql_global_aggregate_command_for_model_only")
            && sql_global_aggregate_binding
                .contains("compile_sql_global_aggregate_command_core_from_prepared_with_schema")
            && !sql_global_aggregate_binding.contains("fn into_typed<E:")
            && !sql_global_aggregate_binding
                .contains("pub(crate) fn compile_sql_global_aggregate_command<E:")
            && !sql_global_aggregate_binding
                .contains("pub(crate) fn compile_sql_global_aggregate_command_from_prepared<E:")
            && !sql_global_aggregate_binding
                .contains("fn bind_lowered_sql_global_aggregate_command<E:"),
        "generated-schema SQL global aggregate lowering must be explicitly test-only/model-only while runtime aggregate binding uses explicit SchemaInfo",
    );
}

#[test]
fn typed_runtime_dispatch_selects_accepted_entity_authority_at_session_boundary() {
    let session_mod = read_source("src/db/session/mod.rs");
    let session_query_cache = read_source("src/db/session/query/cache.rs");
    let session_sql_cache = read_source("src/db/session/sql/cache.rs");
    let session_sql_execute = read_source("src/db/session/sql/execute/mod.rs");
    let session_sql_explain = read_source("src/db/session/sql/execute/explain.rs");
    let session_sql_write = read_source("src/db/session/sql/execute/write.rs");
    let entity_authority = read_source("src/db/executor/authority/entity.rs");

    assert!(
        session_mod.contains("pub(in crate::db) fn accepted_entity_authority<E>")
            && session_mod.contains("pub(in crate::db) fn accepted_entity_authority_for_schema<E>")
            && session_mod.contains("EntityAuthority::from_accepted_schema_for_type::<E>(")
            && !session_mod.contains("EntityAuthority::for_generated_type_for_test::<E>()")
            && entity_authority.contains("fn from_accepted_schema_for_type<E>")
            && entity_authority
                .contains("AcceptedRowLayoutRuntimeDescriptor::from_generated_compatible_schema(")
            && entity_authority.contains("with_accepted_row_decode_contract(")
            && session_query_cache.contains("accepted_entity_authority::<E>()")
            && session_query_cache.contains("cached_shared_query_plan_for_accepted_authority(")
            && !session_query_cache.contains("EntityAuthority::for_generated_type_for_test::<E>()")
            && session_sql_cache.contains("accepted_entity_authority::<E>()")
            && !session_sql_cache.contains("EntityAuthority::for_generated_type_for_test::<E>()")
            && session_sql_execute.contains("sql_select_prepared_plan_for_entity::<E>(query)")
            && session_sql_execute.contains("accepted_entity_authority::<E>()")
            && !session_sql_execute.contains("EntityAuthority::for_generated_type_for_test::<E>()")
            && session_sql_explain.contains("accepted_schema: &AcceptedSchemaSnapshot")
            && session_sql_explain.contains("cached_shared_query_plan_for_accepted_authority(")
            && !session_sql_explain.contains("ensure_accepted_schema_snapshot_for_authority(")
            && !session_sql_explain.contains("cached_shared_query_plan_for_authority(")
            && session_sql_write.contains("accepted_entity_authority_for_schema::<E>")
            && !session_sql_write.contains("EntityAuthority::for_generated_type_for_test::<E>()"),
        "typed runtime SQL/query dispatch must select accepted EntityAuthority at the session boundary instead of passing generated authority to lower helpers",
    );
}

#[test]
fn cursor_boundary_validation_uses_authority_schema_info() {
    let cursor_boundary = read_source("src/db/cursor/boundary.rs");
    let cursor_mod = read_source("src/db/cursor/mod.rs");
    let cursor_spine = read_source("src/db/cursor/spine.rs");
    let continuation = read_source("src/db/query/plan/continuation.rs");
    let entity_authority = read_source("src/db/executor/authority/entity.rs");
    let entity_authority_compact = compact_source(&entity_authority);

    assert!(
        cursor_boundary.contains("schema: &SchemaInfo,")
            && !cursor_boundary.contains("SchemaInfo::cached_for_generated_entity_model(model)")
            && !cursor_boundary.contains("fn boundary_schema("),
        "cursor boundary validation must consume caller-supplied schema info instead of reopening generated schema metadata",
    );
    assert!(
        cursor_mod.contains("schema: &SchemaInfo,")
            && cursor_spine.contains("fn schema_info(&self) -> &SchemaInfo;")
            && continuation.contains("schema_info: &SchemaInfo,"),
        "cursor preparation and revalidation must thread planner/session-selected schema info through the cursor spine",
    );
    assert!(
        entity_authority.contains("accepted_schema_info: Option<Arc<SchemaInfo>>,")
            && entity_authority_compact
                .contains("fncursor_schema_info(&self)->Result<&SchemaInfo,CursorPlanError>")
            && entity_authority.contains("contract.prepare_scalar_cursor(")
            && entity_authority.contains("contract.revalidate_scalar_cursor(")
            && entity_authority_compact.contains(
                "authority.with_accepted_row_decode_contract(row_shape,row_decode_contract,schema_info",
            ),
        "entity authority must carry accepted schema info into scalar cursor validation",
    );
}

#[test]
fn prepared_static_shape_finalization_uses_authority_schema_info() {
    let entity_authority = read_source("src/db/executor/authority/entity.rs");
    let query_plan_logical = read_source("src/db/query/plan/semantics/logical.rs");
    let predicate_runtime = read_source("src/db/predicate/runtime/mod.rs");
    let predicate_capability = read_source("src/db/predicate/capability.rs");
    let schema_info = read_source("src/db/schema/info.rs");

    assert!(
        entity_authority.contains(
            "plan.finalize_static_planning_shape_for_model_with_schema(self.model, schema_info)",
        ) && !entity_authority.contains(".finalize_static_planning_shape_for_model_only(self.model)")
            && !entity_authority.contains("PreparedShapeFinalizationOutcome::GeneratedFallback")
            && query_plan_logical.contains(
                "#[cfg(test)]\n    pub(in crate::db) fn finalize_static_planning_shape_for_model_only("
            ),
        "prepared execution finalization must use authority-carried schema info and keep generated static-shape finalization model-only and test-only",
    );
    assert!(
        schema_info.contains("leaf_codec: LeafCodec,")
            && schema_info.contains("pub(in crate::db) fn field_slot_has_scalar_leaf")
            && predicate_runtime.contains("pub(in crate::db) fn compile_with_schema_info(")
            && predicate_runtime.contains("schema_info.field_slot_index(field_name)")
            && predicate_runtime
                .contains("PredicateCapabilityContext::runtime_schema(schema_info)")
            && query_plan_logical
                .contains("PredicateProgram::compile_with_schema_info(schema_info, predicate)")
            && !query_plan_logical
                .contains("PredicateProgram::compile_for_model_only(model, predicate)")
            && predicate_runtime
                .contains("#[cfg(test)]\n    pub(in crate::db) fn compile_for_model_only(")
            && predicate_capability
                .contains("#[cfg(test)]\n    pub(in crate::db) fn runtime_for_model_only("),
        "prepared predicate compilation and scalar fast-path classification must use schema info, keeping generated model wrappers model-only and test-only",
    );
}

#[test]
fn scalar_aggregate_expression_compilation_uses_accepted_schema_info() {
    let scalar_expr = read_source("src/db/query/plan/expr/scalar.rs");
    let scalar_expr_mod = read_source("src/db/query/plan/expr/mod.rs");
    let aggregate_helpers = read_source("src/db/sql/lowering/aggregate/lowering/helpers.rs");
    let aggregate_terminal = read_source("src/db/executor/aggregate/scalar_terminals/terminal.rs");
    let aggregate_request = read_source("src/db/executor/aggregate/scalar_terminals/request.rs");
    let aggregate_runtime = read_source("src/db/executor/aggregate/scalar_terminals/mod.rs");
    let session_global_aggregate = read_source("src/db/session/sql/execute/global_aggregate.rs");
    let session_sql_execute = read_source("src/db/session/sql/execute/mod.rs");

    assert!(
        scalar_expr.contains(
            "#[cfg(test)]\n#[must_use]\npub(in crate::db) fn compile_scalar_projection_expr_for_model_only("
        ) && scalar_expr_mod.contains(
            "#[cfg(test)]\npub(in crate::db) use scalar::compile_scalar_projection_expr_for_model_only;"
        ),
        "generated-schema scalar projection compiler wrapper must stay explicitly model-only and test-only",
    );
    assert!(
        scalar_expr.contains(
            "pub(in crate::db) fn compile_scalar_projection_expr_with_schema(\n    schema: &SchemaInfo,"
        ) && scalar_expr.contains(
            "pub(in crate::db) fn compile_scalar_projection_plan_with_schema(\n    schema: &SchemaInfo,"
        ) && !scalar_expr.contains(
            "pub(in crate::db) fn compile_scalar_projection_expr_with_schema(\n    model:"
        ) && !scalar_expr.contains(
            "pub(in crate::db) fn compile_scalar_projection_plan_with_schema(\n    model:"
        ),
        "shared scalar projection compilers must take schema info directly without generated model parameters",
    );
    assert!(
        aggregate_helpers.contains("schema: &SchemaInfo,")
            && aggregate_helpers
                .contains("compile_scalar_projection_expr_with_schema(schema, expr)",)
            && !aggregate_helpers
                .contains("compile_scalar_projection_expr_for_model_only(model, expr)"),
        "SQL aggregate scalar-expression validation must compile against caller-supplied schema info",
    );
    assert!(
        aggregate_terminal.contains("schema: &SchemaInfo,")
            && aggregate_terminal
                .contains("compile_scalar_projection_expr_from_schema(schema, expr)",)
            && aggregate_terminal.contains("schema.field_slot_index(field.as_str()).is_none()")
            && !aggregate_terminal.contains("EntityModel")
            && !aggregate_terminal
                .contains("compile_scalar_projection_expr_for_model_only(model, expr)")
            && !aggregate_terminal.contains("model.resolve_field_slot(field.as_str()).is_none()"),
        "structural aggregate terminal expression compilation must use accepted schema info for scalar slot resolution",
    );
    assert!(
        aggregate_request.contains("schema_info: SchemaInfo,")
            && aggregate_request.contains("pub(super) const fn schema_info(&self) -> &SchemaInfo")
            && aggregate_runtime.contains("request.schema_info()")
            && aggregate_runtime
                .contains("terminal.uses_shared_count_terminal(request.schema_info())")
            && !aggregate_runtime.contains("terminal.uses_shared_count_terminal(E::MODEL)")
            && !aggregate_runtime.contains(
                "compile_structural_scalar_aggregate_terminal(\n                    E::MODEL,"
            )
            && aggregate_terminal
                .contains("pub(super) fn uses_shared_count_terminal(&self, schema: &SchemaInfo)")
            && aggregate_terminal
                .contains("schema\n                    .field_nullable(target_slot.field())")
            && !aggregate_terminal.contains("model.fields().get(target_slot.index())")
            && session_global_aggregate.contains("accepted_schema_info_for_entity::<E>()")
            && session_global_aggregate.contains(
                "StructuralAggregateRequest::new(terminals, projection, having, schema_info)",
            ),
        "session global aggregate execution must carry accepted schema info into structural aggregate runtime",
    );
    assert!(
        session_global_aggregate.contains("fn execute_global_aggregate_statement<E>")
            && !session_global_aggregate.contains("EntityAuthority")
            && !session_global_aggregate.contains("for_authority")
            && session_sql_execute.contains(
                "CompiledSqlCommand::GlobalAggregate { command } => {\n                self.execute_global_aggregate_statement::<E>(",
            )
            && !session_sql_execute.contains(
                "CompiledSqlCommand::GlobalAggregate { command } => {\n                let authority = EntityAuthority::for_generated_type_for_test::<E>();",
            ),
        "SQL global aggregate execution must not retain a generated EntityAuthority bootstrap lane",
    );
}

#[test]
fn fluent_terminal_field_slots_use_accepted_schema_info() {
    let fluent_builder = read_source("src/db/query/fluent/load/builder.rs");
    let fluent_validation = read_source("src/db/query/fluent/load/validation.rs");
    let query_intent = read_source("src/db/query/intent/model.rs");
    let sql_select = read_source("src/db/sql/lowering/select/mod.rs");
    let sql_select_aggregate = read_source("src/db/sql/lowering/select/aggregate.rs");
    let sql_prepare = read_source("src/db/sql/lowering/prepare.rs");
    let session_sql_compile = read_source("src/db/session/sql/compile/core.rs");
    let session_mod = read_source("src/db/session/mod.rs");
    let symbols = read_source("src/db/query/plan/validate/symbols.rs");
    let query_plan_group = read_source("src/db/query/plan/group.rs");
    let query_plan_logical = read_source("src/db/query/plan/semantics/logical.rs");
    let sql_aggregate_strategy = read_source("src/db/sql/lowering/aggregate/strategy.rs");
    let session_global_aggregate = read_source("src/db/session/sql/execute/global_aggregate.rs");

    assert!(
        fluent_validation.contains("accepted_schema_info_for_entity::<E>()")
            && fluent_validation.contains("resolve_aggregate_target_field_slot_with_schema(")
            && !fluent_validation.contains("resolve_aggregate_target_field_slot(E::MODEL"),
        "fluent aggregate/projection terminal field slots must resolve through accepted schema info instead of generated model slot order",
    );
    assert!(
        fluent_builder.contains("query.group_by_with_schema(&field, &schema)")
            && fluent_builder
                .contains("query.having_group_with_schema(&field, &schema, op, value)")
            && sql_select.contains("query.group_by_with_schema(field, schema)?")
            && sql_select_aggregate.contains("schema: &SchemaInfo,")
            && sql_select_aggregate.contains("resolve_group_field_slot_with_schema(")
            && !sql_select_aggregate.contains("resolve_group_field_slot(model")
            && query_intent.contains("fn push_group_field_with_schema(")
            && query_intent.contains("fn push_having_group_clause_with_schema("),
        "fluent and SQL grouped field slots must use accepted schema info at session/lowering boundaries",
    );
    assert!(
        sql_prepare.contains("fn lower_prepared_sql_select_statement_with_schema(")
            && session_sql_compile.contains("lower_prepared_sql_select_statement_with_schema("),
        "session SQL SELECT compilation must lower grouped HAVING canonicalization with accepted schema info",
    );
    assert!(
        session_mod.contains("pub(in crate::db) fn accepted_schema_info_for_entity<E>")
            && session_mod.contains("SchemaInfo::from_accepted_snapshot_for_model(")
            && symbols
                .contains("pub(in crate::db) fn resolve_aggregate_target_field_slot_with_schema(")
            && symbols.contains("pub(in crate::db) fn resolve_group_field_slot_with_schema(")
            && symbols.contains(".field_slot_index(field)")
            && !symbols.contains("fn resolve_aggregate_target_field_slot(")
            && !symbols.contains("model.resolve_field_slot(field)?;"),
        "session and planner symbol helpers must expose accepted-schema field-slot resolution",
    );
    assert!(
        session_global_aggregate.contains("accepted_schema_info_for_entity::<E>()")
            && !session_global_aggregate.contains("ensure_accepted_schema_snapshot::<E>()"),
        "SQL global aggregate execution should reuse the session accepted-schema info helper",
    );
    assert!(
        sql_aggregate_strategy.contains("resolve_aggregate_target_field_slot_with_schema(")
            && !sql_aggregate_strategy.contains("resolve_aggregate_target_field_slot(model"),
        "SQL aggregate field-target strategies must resolve terminal slots through accepted schema info",
    );
    assert!(
        query_plan_group.contains("pub(in crate::db) fn resolve_with_schema_info(")
            && query_plan_group.contains("pub(in crate::db) fn grouped_aggregate_execution_specs(\n    schema_info: &SchemaInfo,")
            && query_plan_group.contains(
                "pub(in crate::db) fn resolved_grouped_distinct_execution_strategy_with_schema_info("
            )
            && query_plan_group.contains("kind: schema_info.field_kind(field).copied(),")
            && !query_plan_group.contains("resolve_for_model(")
            && !query_plan_group.contains("resolved_grouped_distinct_execution_strategy_for_model")
            && !query_plan_group.contains("model_field.name()")
            && query_plan_logical.contains("grouped_aggregate_execution_specs(\n        schema_info,")
            && query_plan_logical
                .contains("resolved_grouped_distinct_execution_strategy_with_schema_info("),
        "grouped aggregate execution specs and grouped DISTINCT target slots must resolve through schema info, not generated model fields",
    );
}

#[test]
fn value_stays_out_of_persisted_field_contracts() {
    let forbidden_impls = [
        "implPersistedFieldSlotCodecforValue",
        "implPersistedFieldSlotCodecforVec<Value>",
        "implPersistedStructuredFieldCodecforValue",
        "implPersistedStructuredFieldCodecforVec<Value>",
        "implFieldTypeMetaforValue",
        "implFieldTypeMetaforVec<Value>",
    ];
    let mut violations = Vec::new();

    // Scan production source only. Compile-fail fixtures intentionally mention
    // these shapes so user-facing errors stay locked down separately.
    for path in rust_sources_under("src") {
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
        let compact = compact_source(&source);
        for forbidden in forbidden_impls {
            if compact.contains(forbidden) {
                violations.push(format!("{} contains {forbidden}", path.display()));
            }
        }
    }

    assert!(
        violations.is_empty(),
        "Value is runtime-only and must not implement persisted-field contracts:\n{}",
        violations.join("\n"),
    );
}
