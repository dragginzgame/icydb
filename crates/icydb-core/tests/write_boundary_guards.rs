use std::{
    fs,
    path::{Path, PathBuf},
};

fn read_source(relative_path: &str) -> String {
    let mut path = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.push(relative_path);

    fs::read_to_string(&path)
        .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()))
}

fn read_sources(relative_paths: &[&str]) -> String {
    relative_paths
        .iter()
        .map(|relative_path| read_source(relative_path))
        .collect::<Vec<_>>()
        .join("\n")
}

fn rust_sources_under(relative_path: &str) -> Vec<PathBuf> {
    let mut root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    root.push(relative_path);

    rust_sources_under_path(root)
}

fn rust_sources_under_path(root: PathBuf) -> Vec<PathBuf> {
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

fn entity_attribute_blocks(source: &str) -> Vec<&str> {
    let mut blocks = Vec::new();
    let mut search_from = 0usize;

    while let Some(relative_start) = source[search_from..].find("#[entity(") {
        let start = search_from + relative_start;
        let mut depth = 0u32;
        let mut end = None;

        for (offset, character) in source[start..].char_indices() {
            match character {
                '(' => depth = depth.saturating_add(1),
                ')' => {
                    depth = depth.saturating_sub(1);
                    if depth == 0 {
                        end = Some(start + offset + character.len_utf8());
                        break;
                    }
                }
                _ => {}
            }
        }

        let Some(end) = end else {
            panic!("unterminated #[entity(...)] attribute in source");
        };
        blocks.push(&source[start..end]);
        search_from = end;
    }

    blocks
}

fn read_rust_sources_under(relative_path: &str) -> String {
    rust_sources_under(relative_path)
        .iter()
        .map(|path| {
            fs::read_to_string(path)
                .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()))
        })
        .collect::<Vec<_>>()
        .join("\n")
}

fn relative_source_path(path: &Path) -> String {
    let manifest_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    path.strip_prefix(manifest_root)
        .unwrap_or_else(|err| panic!("failed to relativize {}: {err}", path.display()))
        .to_string_lossy()
        .replace('\\', "/")
}

fn strip_cfg_test_items(source: &str) -> String {
    let mut output = String::new();
    let mut pending_cfg_test = false;
    let mut skip_depth = 0usize;

    for line in source.lines() {
        let trimmed = line.trim();
        if skip_depth > 0 {
            skip_depth = skip_depth
                .saturating_add(line.matches('{').count())
                .saturating_sub(line.matches('}').count());
            continue;
        }

        if trimmed.starts_with("#[cfg(test)]") {
            pending_cfg_test = true;
            continue;
        }
        if pending_cfg_test {
            let opens = line.matches('{').count();
            let closes = line.matches('}').count();
            if opens > 0 {
                skip_depth = opens.saturating_sub(closes);
            }
            pending_cfg_test = false;
            continue;
        }

        output.push_str(line);
        output.push('\n');
    }

    output
}

fn compact_source(source: &str) -> String {
    source
        .chars()
        .filter(|character| !character.is_whitespace())
        .collect()
}

#[test]
fn data_store_insert_stays_canonical_row_only() {
    let source = compact_source(&read_source("src/db/data/store.rs"));

    assert!(
        source.contains(
            "pub(incrate::db)fninsert(&mutself,key:RawDataStoreKey,row:CanonicalRow,)->Option<RawRow>"
        ),
        "DataStore::insert must remain CanonicalRow-only at the production write boundary",
    );
    assert!(
        !source.contains("pubfninsert(&mutself,key:RawDataStoreKey,row:RawRow)"),
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
    let row_reader = read_source("src/db/data/persisted_row/reader/structural_slot_reader.rs");
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
                .contains("contract.required_accepted_field_decode_contract(slot)?")
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
    let row_reader = read_source("src/db/data/persisted_row/reader/structural_slot_reader.rs");
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
        ) && commit_prepare_compact.contains(
            "SchemaInfo::from_accepted_snapshot_for_model_with_expression_indexes(authority.model,&accepted,true,)",
        )
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
            .contains("letSome(field)=accepted_contract.field_for_slot(slot)else{continue;};")
            && save_validation_compact
                .contains("accepted_contract.required_field_for_slot(E::PATH,primary_key_slot)?")
            && save_validation_compact.contains(
                "letSome(field)=accepted_contract.field_for_slot(field_index)else{continue;};"
            )
            && relation_save_validate_compact.contains(
                "letSome(field)=accepted_row_decode_contract.field_for_slot(slot)else{continue;};"
            )
            && !save_validation_compact.contains(".field_for_slot(primary_key_slot).ok_or_else(")
            && !save_validation_compact.contains(".field_for_slot(field_index).ok_or_else("),
        "accepted typed-save validation must use required primary-key lookup and skip retired accepted row-decode slots explicitly",
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
        schema_info.contains("fn accepted_indexed_field_ids(")
            && schema_info.contains("index.key().references_field(field.id())")
            && schema_info
                .contains("let indexed_field_ids = accepted_indexed_field_ids(snapshot);")
            && schema_info.contains("indexed: indexed_field_ids.contains(&field.id()),")
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
            && schema_info.contains(
                "pub(in crate::db) const fn expression_indexes(&self) -> &[SchemaExpressionIndexInfo]",
            )
            && schema_info.contains("pub(in crate::db) struct SchemaExpressionIndexInfo")
            && schema_info.contains("pub(in crate::db) struct SchemaIndexExpressionInfo")
            && schema_info.contains("fn schema_index_info_from_accepted_index(")
            && schema_info.contains("fn schema_expression_index_info_from_accepted_index(")
            && schema_info_compact.contains("indexes:snapshot.indexes().iter().filter_map(|index|schema_index_info_from_accepted_index(index,snapshot)).collect(),")
            && schema_info.contains("include_expression_indexes: bool")
            && schema_info.contains(
                "from_accepted_snapshot_for_model_with_expression_indexes(model, schema, false)"
            )
            && schema_info.contains("expression_indexes: snapshot")
            && schema_info.contains(
                "schema_expression_index_info_from_accepted_index(index, snapshot)"
            )
            && schema_info.contains("fn schema_index_info_from_generated_index("),
        "accepted SchemaInfo must expose field-path and expression index metadata from persisted contracts while keeping generated projection isolated to generated schema views",
    );
}

#[test]
fn sql_ddl_drop_index_uses_persisted_index_origin() {
    let ddl = read_rust_sources_under("src/db/sql/ddl");
    let mutation = read_rust_sources_under("src/db/schema/mutation");
    let session_sql = read_source("src/db/session/sql/mod.rs");

    assert!(
        ddl.contains("pub(in crate::db) fn prepare_sql_ddl_statement(")
            && ddl.contains("pub(in crate::db) fn bind_sql_ddl_statement(")
            && !ddl.contains("use crate::model::EntityModel")
            && !ddl.contains("model: &EntityModel")
            && !ddl.contains("model: &'static EntityModel")
            && !ddl.contains("model.indexes()")
            && !ddl.contains("E::MODEL"),
        "SQL DDL binding must stay catalog-native and must not reopen generated EntityModel authority",
    );
    assert!(
        mutation.contains("fn resolve_sql_ddl_secondary_index_drop_candidate(")
            && mutation.contains("if index.generated()")
            && !mutation.contains("model.indexes()")
            && !mutation.contains("model: &EntityModel")
            && !mutation.contains("use crate::model::EntityModel"),
        "DROP INDEX resolution must reject generated indexes through persisted accepted index origin",
    );
    let session_sql_compact = compact_source(&session_sql);
    assert!(
        session_sql_compact.contains("letschema_info=catalog.accepted_schema_info_for::<E>();")
            && session_sql.contains("prepare_sql_ddl_statement(\n            &statement,\n            catalog.snapshot(),\n            &schema_info,\n            E::Store::PATH,\n        )")
            && !session_sql.contains("prepare_sql_ddl_statement(\n            &statement,\n            &accepted_schema,\n            &schema_info,\n            E::MODEL,"),
        "session DDL preparation may bridge generated model into accepted SchemaInfo but must not pass it into DDL binding",
    );
}

#[test]
fn sql_ddl_add_column_uses_schema_owned_field_allocation() {
    let ddl_field = read_source("src/db/sql/ddl/field.rs");
    let mutation = read_rust_sources_under("src/db/schema/mutation");
    let ddl_field_compact = compact_source(&ddl_field);

    assert!(
        ddl_field.contains("resolve_sql_ddl_field_addition_name_candidate(")
            && ddl_field.contains("build_sql_ddl_field_addition_candidate(")
            && !ddl_field.contains(".field_nullable(statement.column_name.as_str())")
            && !ddl_field_compact.contains("!statement.nullable&&default.is_none()")
            && !ddl_field.contains("fn next_sql_ddl_field_id(")
            && !ddl_field.contains("fn next_sql_ddl_field_slot(")
            && !ddl_field.contains("PersistedFieldOrigin::SqlDdl")
            && !ddl_field.contains("SchemaFieldWritePolicy::from_model_policies(None, None)"),
        "SQL DDL ADD COLUMN must bind author intent without owning field existence, required-default, ID, slot, origin, or write-policy allocation",
    );
    assert!(
        mutation.contains("pub(in crate::db) enum SchemaDdlFieldAdditionCandidateError")
            && mutation
                .contains("pub(in crate::db) fn resolve_sql_ddl_field_addition_name_candidate(")
            && mutation.contains("pub(in crate::db) fn build_sql_ddl_field_addition_candidate(")
            && mutation.contains("SchemaDdlFieldAdditionCandidateError::Duplicate")
            && mutation.contains("SchemaDdlFieldAdditionCandidateError::RequiredWithoutDefault")
            && mutation.contains("fn next_sql_ddl_field_id(")
            && mutation.contains("fn next_sql_ddl_field_slot(")
            && mutation.contains("PersistedFieldOrigin::SqlDdl")
            && mutation.contains("SchemaFieldWritePolicy::from_model_policies(None, None)"),
        "schema mutation code must own DDL field existence, required-default, ID, slot, origin, and write-policy allocation",
    );
}

#[test]
fn sql_ddl_default_encoding_uses_schema_owned_field_codecs() {
    let ddl_field = read_source("src/db/sql/ddl/field.rs");
    let mutation = read_rust_sources_under("src/db/schema/mutation");

    assert!(
        ddl_field.contains("encode_sql_ddl_add_column_default(")
            && ddl_field.contains("encode_sql_ddl_alter_column_default(")
            && !ddl_field.contains("encode_runtime_value_for_accepted_field_contract")
            && !ddl_field.contains("AcceptedFieldDecodeContract")
            && !ddl_field.contains("canonicalize_strict_sql_literal_for_persisted_kind"),
        "SQL DDL default binding must delegate payload encoding to schema-owned mutation code",
    );
    assert!(
        mutation.contains("pub(in crate::db) fn encode_sql_ddl_add_column_default(")
            && mutation.contains("pub(in crate::db) fn encode_sql_ddl_alter_column_default(")
            && mutation.contains("encode_runtime_value_for_accepted_field_contract")
            && mutation.contains("AcceptedFieldDecodeContract")
            && mutation.contains("canonicalize_strict_sql_literal_for_persisted_kind"),
        "schema mutation code must own DDL default canonicalization and accepted-field codec encoding",
    );
}

#[test]
fn sql_ddl_drop_column_uses_schema_owned_field_drop_candidate_resolution() {
    let ddl_field = read_source("src/db/sql/ddl/field.rs");
    let mutation = read_rust_sources_under("src/db/schema/mutation");

    assert!(
        ddl_field.contains("resolve_sql_ddl_field_drop_candidate(")
            && !ddl_field.contains("primary_key_field_ids().contains")
            && !ddl_field.contains("resolve_sql_ddl_field_drop_dependent_index"),
        "SQL DDL DROP COLUMN must map schema-owned candidate classification instead of scanning primary-key or index metadata itself",
    );
    assert!(
        mutation.contains("enum SchemaDdlFieldDropCandidateError")
            && mutation.contains("pub(in crate::db) fn resolve_sql_ddl_field_drop_candidate(")
            && mutation.contains("fn resolve_sql_ddl_field_drop_dependent_index(")
            && mutation.contains("primary_key_field_ids().contains")
            && mutation.contains("field.generated()"),
        "schema mutation code must own DROP COLUMN candidate ownership, primary-key, and dependency classification",
    );
}

#[test]
fn sql_ddl_field_metadata_changes_use_schema_owned_candidate_resolution() {
    let ddl_field = read_source("src/db/sql/ddl/field.rs");
    let ddl_mod = read_source("src/db/sql/ddl/mod.rs");
    let mutation = read_rust_sources_under("src/db/schema/mutation");

    assert!(
        ddl_field.contains("resolve_sql_ddl_field_set_default_candidate(")
            && ddl_field.contains("resolve_sql_ddl_field_drop_default_candidate(")
            && ddl_field.contains("resolve_sql_ddl_field_nullability_candidate(")
            && ddl_field.contains("resolve_sql_ddl_field_rename_candidate(")
            && !ddl_field.contains("field.generated()"),
        "SQL DDL field metadata changes must map schema-owned candidate classification instead of checking field ownership directly",
    );
    assert!(
        !ddl_mod.contains("clone_with_name(")
            && ddl_mod.contains("admit_sql_ddl_field_rename_candidate(")
            && ddl_mod.contains("rename.field(),")
            && ddl_mod.contains("rename.new_name(),"),
        "SQL DDL lowering must ask schema mutation to construct rename admission targets instead of synthesizing renamed field snapshots",
    );
    assert!(
        mutation.contains("enum SchemaDdlFieldDefaultCandidateError")
            && mutation.contains("enum SchemaDdlFieldNullabilityCandidateError")
            && mutation.contains("enum SchemaDdlFieldRenameCandidateError")
            && mutation
                .contains("pub(in crate::db) fn resolve_sql_ddl_field_set_default_candidate(")
            && mutation
                .contains("pub(in crate::db) fn resolve_sql_ddl_field_drop_default_candidate(")
            && mutation
                .contains("pub(in crate::db) fn resolve_sql_ddl_field_nullability_candidate(")
            && mutation.contains("pub(in crate::db) fn resolve_sql_ddl_field_rename_candidate(")
            && mutation.contains("field.generated()"),
        "schema mutation code must own generated-field classification for DDL field metadata candidates",
    );
}

#[test]
fn sql_ddl_create_index_uses_schema_owned_index_candidate_identity() {
    let ddl_index = read_source("src/db/sql/ddl/index.rs");
    let mutation = read_rust_sources_under("src/db/schema/mutation");

    assert!(
        ddl_index.contains("build_sql_ddl_secondary_index_candidate(")
            && ddl_index.contains("resolve_sql_ddl_secondary_index_addition_candidate(")
            && !ddl_index.contains("accepted_index_field_path_snapshot(")
            && !ddl_index.contains("PersistedIndexKeySnapshot::")
            && !ddl_index.contains("PersistedIndexKeyItemSnapshot::")
            && !ddl_index.contains("PersistedIndexExpressionSnapshot::new(")
            && !ddl_index.contains("format!(\"expr:v1:{}\"")
            && !ddl_index.contains("find_field_path_index_by_name(")
            && !ddl_index.contains("existing_field_path_index_matches_request(")
            && !ddl_index.contains("find_expression_index_by_name(")
            && !ddl_index.contains("existing_expression_index_matches_request(")
            && !ddl_index.contains("reject_duplicate_field_path_index(")
            && !ddl_index.contains("reject_duplicate_expression_index(")
            && !ddl_index.contains("PersistedIndexSnapshot::new_sql_ddl")
            && !ddl_index.contains("next_secondary_index_ordinal"),
        "SQL DDL CREATE INDEX must bind key intent without owning accepted index conflict matching, ordinal, or origin allocation",
    );
    assert!(
        mutation.contains("pub(in crate::db) enum SchemaDdlSecondaryIndexAdditionCandidate")
            && mutation.contains("pub(in crate::db) enum SchemaDdlSecondaryIndexKeyIntent")
            && mutation.contains(
                "pub(in crate::db) fn resolve_sql_ddl_secondary_index_addition_candidate("
            )
            && mutation.contains("PersistedIndexKeySnapshot::")
            && mutation.contains("PersistedIndexKeyItemSnapshot::")
            && mutation.contains("PersistedIndexExpressionSnapshot::new(")
            && mutation.contains("format!(\"expr:v1:{}\"")
            && mutation.contains("fn secondary_index_exact_addition_match(")
            && mutation.contains("fn secondary_index_duplicate_contract_match(")
            && mutation.contains("pub(in crate::db) fn build_sql_ddl_secondary_index_candidate(")
            && mutation.contains("fn next_sql_ddl_secondary_index_ordinal(")
            && mutation.contains("PersistedIndexSnapshot::new_sql_ddl("),
        "schema mutation code must own DDL secondary-index key snapshots, conflict matching, ordinal, and origin allocation",
    );
}

#[test]
fn sql_ddl_frontend_does_not_write_schema_store_directly() {
    let sql_ddl = read_rust_sources_under("src/db/sql");
    let session_sql = read_rust_sources_under("src/db/session/sql");

    for (surface, source) in [("SQL parser/DDL", sql_ddl), ("session SQL", session_sql)] {
        assert!(
            !source.contains("insert_persisted_snapshot")
                && !source.contains("publish_accepted_snapshot")
                && !source.contains("with_schema_mut")
                && !source.contains("SchemaStore")
                && !source.contains("schema_store"),
            "{surface} must not write accepted schema storage directly; SQL DDL must publish through schema-owned mutation/reconciliation surfaces",
        );
    }
}

#[test]
fn schema_mutation_publication_boundary_uses_runner_preflight() {
    let mutation = read_sources(&[
        "src/db/schema/mutation/mod.rs",
        "src/db/schema/mutation/runner.rs",
    ]);
    let transition = read_source("src/db/schema/transition.rs");
    let reconcile = read_source("src/db/schema/reconcile.rs");
    let reconcile_compact = compact_source(&reconcile);
    let startup_field_path = read_source("src/db/schema/reconcile/startup_field_path.rs");

    assert!(
        mutation.contains("pub(in crate::db::schema) enum MutationPublicationPreflight")
            && mutation.contains("PhysicalWorkReady")
            && mutation.contains("MissingRunnerCapabilities")
            && mutation.contains("pub(in crate::db::schema) struct SchemaMutationRunnerContract")
            && mutation.contains("pub(in crate::db::schema) fn publication_preflight(")
            && mutation.contains("SchemaMutationRunnerPreflight::Ready")
            && mutation.contains("MutationPublicationPreflight::PhysicalWorkReady")
            && mutation.contains("`PhysicalWorkReady` is still not publishable in 0.152"),
        "schema mutation publication must expose a runner-preflight decision before any physical-work mutation can publish",
    );
    assert!(
        transition.contains("pub(in crate::db::schema) fn publication_preflight(")
            && transition.contains("runner: &SchemaMutationRunnerContract")
            && transition.contains("self.mutation_plan.publication_preflight(runner)"),
        "schema transition plans must expose publication preflight instead of forcing reconciliation to reopen mutation internals",
    );
    assert!(
        reconcile.contains("let runner = SchemaMutationRunnerContract::new(&[]);")
            && reconcile.contains("match plan.publication_preflight(&runner)")
            && reconcile.contains("MutationPublicationPreflight::PublishableNow => Ok(())")
            && reconcile.contains("MutationPublicationPreflight::MissingRunnerCapabilities")
            && reconcile.contains("MutationPublicationPreflight::Rejected")
            && reconcile.contains("plan.supported_developer_physical_path()")
            && reconcile.contains("supported schema mutation requires startup runner execution")
            && reconcile_compact
                .contains("MutationPublicationPreflight::PhysicalWorkReady{step_count,required,}")
            && !reconcile.contains("match plan.publication_status()"),
        "startup reconciliation must consult runner preflight with no physical runner installed, keeping rebuild-required mutation publication fail-closed",
    );
    assert!(
        reconcile.contains("mod startup_field_path;")
            && reconcile.contains("execute_supported_field_path_index_addition(")
            && startup_field_path.contains("fn execute_supported_field_path_index_addition(")
            && startup_field_path.contains("supported_developer_physical_path()")
            && startup_field_path.contains("SchemaMutationRunnerInput::new(")
            && startup_field_path.contains("StructuralRowContract::from_accepted_schema_snapshot(")
            && startup_field_path.contains("StartupFieldPathRebuildGate::from_raw_rows(")
            && startup_field_path.contains("validate_before_physical_work(")
            && startup_field_path.contains("SchemaFieldPathIndexRebuildRow::new(")
            && startup_field_path.contains("SchemaFieldPathIndexRunner::run(")
            && startup_field_path
                .contains("StartupFieldPathPublicationDecision::from_runner_report(")
            && startup_field_path.contains("publish_accepted_snapshot(")
            && startup_field_path.contains("validate_before_schema_publication(")
            && startup_field_path.contains("validate_physical_store_before_schema_publication(")
            && reconcile.contains("SchemaPublicationGate::startup(entity_tag)")
            && startup_field_path.contains(
                "schema_store.insert_persisted_snapshot(self.entity_tag, accepted_after)"
            ),
        "runtime startup reconciliation must route the supported field-path index-add path through the startup rebuild/publication gate without folding the adapter back into general reconciliation",
    );
}

#[test]
fn schema_mutation_runner_publication_requires_physical_store_publish() {
    let runner = read_source("src/db/schema/mutation/runner.rs");
    let runner_compact = compact_source(&runner);
    let field_path_runner = read_source("src/db/schema/mutation/field_path/runner.rs");
    let field_path_publication = read_source("src/db/schema/mutation/field_path/publication.rs");
    let field_path_publication_compact = compact_source(&field_path_publication);

    assert!(
        runner.contains("PublishPhysicalStore")
            && runner.contains("pub(in crate::db::schema) fn with_physical_store_published(")
            && runner_compact.contains("self.has_completed_phase(SchemaMutationRunnerPhase::PublishSnapshot)&&self.has_completed_phase(SchemaMutationRunnerPhase::PublishPhysicalStore)"),
        "generic runner diagnostics must require both snapshot and physical-store publication before physical work allows publication",
    );
    assert!(
        field_path_publication.contains(
            "SchemaFieldPathIndexStagedStorePublicationBlocker::PhysicalStoreNotPublished",
        ) && field_path_publication_compact.contains("store_visibility:SchemaMutationStoreVisibility::StagedOnly,runner_report:self.runner_report.with_snapshot_published(),")
            && field_path_publication.contains(
                "let runner_report = self\n            .publication_report\n            .runner_report()\n            .with_physical_store_published();",
            ),
        "field-path snapshot handoff must stay staged until published-store promotion advances the runner report",
    );
    assert!(
        field_path_runner.contains("self.published_store_report.publication_readiness()"),
        "top-level field-path runner readiness must come from the published-store report, not the snapshot handoff report",
    );
}

#[test]
fn schema_mutation_field_path_runner_stays_accepted_schema_authority() {
    let field_path_runner = read_rust_sources_under("src/db/schema/mutation/field_path");
    let field_path_runner_compact = compact_source(&field_path_runner);

    assert!(
        field_path_runner.contains("SchemaMutationRunnerInput")
            && field_path_runner.contains("SchemaFieldPathIndexRebuildTarget")
            && field_path_runner.contains("SchemaMutationExecutionPlan")
            && field_path_runner_compact.contains("input.accepted_after().entity_path()")
            && field_path_runner.contains("SchemaMutationExecutionStep::BuildFieldPathIndex {",),
        "field-path runner modules must consume schema-owned runner input, execution plans, and accepted rebuild targets",
    );
    assert!(
        !field_path_runner.contains("EntityModel")
            && !field_path_runner.contains("IndexModel")
            && !field_path_runner.contains("model_only_from_generated_index")
            && !field_path_runner.contains("SchemaInfo::cached_for_generated_entity_model")
            && !field_path_runner.contains("EntityAuthority::for_generated_type_for_test"),
        "field-path runner modules must not reopen generated model/index authority",
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "source-boundary guard keeps related accepted visible-index assertions together"
)]
fn runtime_visible_indexes_are_accepted_schema_filtered() {
    let access_path = read_source("src/db/access/path.rs");
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
            && plan_mod.contains("accepted_expression_indexes: Vec<AcceptedPlannerExpressionIndex>")
            && plan_mod.contains("pub(in crate::db) struct AcceptedPlannerExpressionIndex")
            && plan_mod.contains("accepted_schema_info: Option<SchemaInfo>")
            && plan_mod.contains("generated_model_only_indexes: Cow")
            && plan_mod.contains("pub(in crate::db) fn generated_model_only_indexes(&self)")
            && plan_mod.contains("pub(in crate::db) const fn accepted_expression_indexes(")
            && !plan_mod.contains("generated_candidate_bridge_indexes")
            && !plan_mod.contains("generated_expression_candidate_indexes")
            && !plan_mod.contains("struct GeneratedExpressionCandidateIndex")
            && !plan_mod.contains(".filter_map(GeneratedExpressionCandidateIndex::from_index)")
            && plan_mod.contains("pub(in crate::db) const fn accepted_schema_info(")
            && plan_mod.contains("accepted_schema_info: Some(schema_info.clone())")
            && plan_mod.contains("AcceptedPlannerFieldPathIndex::from_schema_index")
            && plan_mod.contains("AcceptedPlannerExpressionIndex::from_schema_index")
            && plan_mod.contains("SemanticIndexAccessContract::from_accepted_field_path_index")
            && plan_mod.contains("SemanticIndexAccessContract::from_accepted_expression_index")
            && !plan_mod.contains("fn generated_predicate_bridge_for_accepted_field_path_index")
            && !plan_mod.contains("generated_predicate_bridge: Option<&'static IndexModel>")
            && access_path.contains("accepted_index_predicate_semantics(")
            && access_path.contains("parse_sql_predicate(predicate_sql)")
            && access_path.contains("map_or(Predicate::False")
            && access_path.contains("SemanticIndexKeyItems::Fields(")
            && access_path.contains("SemanticIndexKeyItems::Accepted(")
            && !plan_mod.contains("if accepted_expression_indexes.is_empty()")
            && !plan_mod_compact.contains(
                "schema_info.field_path_indexes().iter().any(|accepted|accepted.name()==index.name())",
            )
            && plan_mod.contains("VisibleIndexAuthority::AcceptedSchema")
            && plan_mod.contains("accepted_field_path_index_count"),
        "VisibleIndexes must carry accepted field-path and expression planner contracts without a generated expression candidate bridge on the accepted runtime lane",
    );
    assert!(
        access_planner.contains("visible_indexes: &VisibleIndexes<'_>,")
            && access_planner.contains("visible_indexes.accepted_planner_indexes()")
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
            && planner_mod
                .contains("accepted_expression_indexes: &[AcceptedPlannerExpressionIndex],",)
            && planner_mod.contains("fn semantic_candidate_indexes_from_generated_model_only(")
            && planner_mod.contains("fn semantic_candidate_indexes_from_accepted_indexes(")
            && planner_mod
                .contains(".map(AcceptedPlannerFieldPathIndex::semantic_access_contract)")
            && planner_mod
                .contains(".map(AcceptedPlannerExpressionIndex::semantic_access_contract)")
            && planner_mod.contains("order_fallback_selection(")
            && planner_mod.contains("index_range_from_order_with_accepted_indexes(")
            && order_select.contains("let accepted_order_terms = accepted.order_terms();")
            && order_select.contains("accepted.semantic_access_contract()")
            && order_select.contains("whole_index_ordered_range_scan_from_contract(")
            && order_select.contains("deterministic_secondary_index_order_terms_satisfied(",)
            && order_select.contains("grouped_index_order_terms_satisfied(")
            && order_select.contains("index_key_item_order_terms(index_contract.key_items())")
            && !order_select.contains(
                "let index_contract = SemanticIndexAccessContract::model_only_from_generated_index(*index);"
            )
            && order_select.contains("if !index_contract.has_expression_key_items() {")
            && !order_select.contains("deterministic_secondary_index_order_satisfied(")
            && !order_select.contains("grouped_index_order_satisfied(")
            && !order_select.contains("accepted.generated_index_bridge()")
            && order_select.contains("fn index_range_from_order_for_generated_model_only(")
            && !order_select.contains("fn index_range_from_order("),
        "order-only accepted access fallback must match ORDER BY against accepted index terms and build accepted access from reduced contracts",
    );
    assert!(
        order_contract.contains("fn deterministic_secondary_index_order_terms_satisfied(")
            && order_contract.contains("fn grouped_index_order_terms_satisfied("),
        "accepted field-path order fallback must expose order-contract helpers that consume accepted index terms directly",
    );
    assert!(
        session_cache.contains("fn visible_indexes_for_accepted_schema(")
            && session_cache.contains("VisibleIndexes::accepted_schema_visible(schema_info)")
            && !session_cache.contains("visible_indexes_for_accepted_schema(\n        model:")
            && !session_cache.contains("visible_indexes_for_accepted_schema(\n        _model:")
            && session_cache
                .contains("SchemaInfo::from_accepted_snapshot_for_model_with_expression_indexes(")
            && session_cache.contains("true,")
            && session_cache_compact.contains("ifletSome(schema_info)=authority.accepted_schema_info()&&(!accepted_schema_has_expression_indexes(accepted_schema)||!schema_info.expression_indexes().is_empty())")
            && session_cache.contains("return schema_info.clone();")
            && !session_cache.contains("fn visible_indexes_for_model("),
        "shared query planning must reuse accepted authority SchemaInfo when it already carries expression metadata and must build visible indexes from accepted SchemaInfo",
    );
    assert!(
        session_mod.contains("fn visible_indexes_for_store_accepted_schema(")
            && session_mod.contains("VisibleIndexes::accepted_schema_visible(schema_info)")
            && !session_mod.contains("visible_indexes_for_store_accepted_schema(\n        &self,\n        store_path: &str,\n        model:")
            && !session_mod.contains("visible_indexes_for_store_accepted_schema(\n        &self,\n        store_path: &str,\n        _model:")
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
            && executor_explain.contains("visible_indexes.accepted_field_path_indexes()")
            && executor_explain.contains("visible_indexes.accepted_expression_indexes()"),
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
            && !access_choice.contains("GeneratedExpressionCandidateIndex")
            && !access_choice.contains("generated_expression_candidate_indexes")
            && access_choice
                .contains("accepted_expression_indexes: &[AcceptedPlannerExpressionIndex]",)
            && !access_choice.contains("generated_candidate_bridge_indexes")
            && access_choice.contains("fn semantic_candidate_indexes_from_generated_model_only(")
            && access_choice.contains("fn semantic_candidate_indexes_from_accepted_indexes(")
            && access_choice
                .contains(".map(AcceptedPlannerFieldPathIndex::semantic_access_contract)")
            && access_choice
                .contains(".map(AcceptedPlannerExpressionIndex::semantic_access_contract)")
            && access_choice.contains("plan_access_selection_with_order_and_semantic_indexes(")
            && !pipeline.contains("visible_indexes.generated_expression_candidate_indexes()")
            && pipeline.contains("visible_indexes.accepted_expression_indexes()")
            && pipeline.contains("rerank_access_plan_by_residual_burden_with_accepted_indexes(")
            && !access_plan.contains("GeneratedExpressionCandidateIndex")
            && access_plan
                .contains("fn finalize_access_choice_for_model_with_accepted_indexes_and_schema(",)
            && session_query_explain
                .contains("finalize_access_choice_for_model_with_accepted_indexes_and_schema(")
            && session_sql_explain
                .contains("finalize_access_choice_for_model_with_accepted_indexes_and_schema("),
        "runtime access-choice reranking and explain projection must rebuild candidate access with accepted field-path and expression contracts when accepted authority is present",
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
            && relation_save_validate.contains("PrimaryKeyComponent::from_runtime_value(value)")
            && !relation_save_validate.contains("strong_relations_for_model_iter")
            && !relation_save_validate.contains("E::MODEL"),
        "save relation validation must derive relation metadata from accepted row contracts and fail closed at the admitted strong-relation target boundary",
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
            && reverse_index.contains("source_key_value: &PrimaryKeyValue,")
            && reverse_index.contains("relation_target_keys_for_source_slots(")
            && reverse_index.contains("target_key_value: &PrimaryKeyValue,")
            && !reverse_index.contains("strong_relations_for_model_iter")
            && !reverse_index.contains("source_model: &'static EntityModel"),
        "reverse-index mutation preparation must derive relation fields from accepted row contracts and keep source/target identity on PrimaryKeyValue boundaries",
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
            .contains("schema_contracts.row_contract.clone(),\n        &source_primary_key,"),
        "commit reverse-index preparation must receive the accepted structural row contract and fail-closed source row identity",
    );
}

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "source-boundary guard keeps related forward-index assertions together"
)]
fn forward_index_write_keys_use_accepted_row_contract_slots() {
    let entity_authority = read_source("src/db/executor/authority/entity.rs");
    let commit_prepare = read_source("src/db/commit/prepare.rs");
    let commit_prepare_compact = compact_source(&commit_prepare);
    let index_key_build = read_source("src/db/index/key/build.rs");
    let index_plan = read_source("src/db/index/plan/mod.rs");
    let index_plan_read = read_source("src/db/index/plan/read.rs");
    let index_readers = read_source("src/db/index/readers.rs");
    let structural_row = read_source("src/db/data/structural_row.rs");

    assert!(
        structural_row.contains("pub(in crate::db) fn field_slot_index_by_name(")
            && structural_row.contains("self.accepted_decode_contract.is_some()"),
        "structural row contracts must expose accepted-first field-name to slot lookup",
    );
    assert!(
        index_key_build.contains("pub(crate) fn new_from_slots_with_accepted_field_path_index")
            && index_key_build.contains("fn build_accepted_field_path_index_key_from_slots(")
            && index_key_build.contains("accepted_field_path_component_bytes_from_slots(")
            && index_key_build
                .contains("pub(crate) fn new_from_slots_with_accepted_expression_index")
            && index_key_build.contains("fn build_accepted_expression_index_key_from_slots(")
            && !index_key_build.contains("pub(crate) fn new_from_slots_with_contract(")
            && !index_key_build.contains("index_component_bytes_from_slots_with_contract(")
            && !index_key_build.contains("row_contract.field_slot_index_by_name(field)?")
            && !index_key_build.contains("pub(crate) fn new_from_slots(\n")
            && !index_key_build.contains("compile_scalar_index_key_item_program("),
        "write-time index key construction must resolve key shape through accepted index contracts without retaining the generated structural contract builder",
    );
    assert!(
        index_key_build.contains("pub(crate) fn new_from_slot_ref_reader_with_access_contract")
            && index_key_build.contains("schema_info.field_slot_index(field)")
            && index_key_build.contains("build_index_key_from_access_contract(")
            && index_key_build
                .contains("pub(crate) fn new_from_slot_ref_reader_with_accepted_field_path_index")
            && index_key_build
                .contains("pub(crate) fn new_from_slots_with_accepted_expression_index")
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
        index_plan.contains("for accepted_index in schema_info.field_path_indexes()")
            && index_plan.contains("for accepted_index in accepted_expression_indexes")
            && index_plan.contains("fn accepted_predicate_program_for_accepted_field_path_index(")
            && index_plan.contains("fn accepted_predicate_program_for_accepted_expression_index(")
            && index_plan
                .contains("let accepted_expression_indexes = schema_info.expression_indexes();")
            && index_plan
                .contains("plan_accepted_field_path_index_mutation_for_slot_reader_structural(")
            && index_plan
                .contains("plan_accepted_expression_index_mutation_for_slot_reader_structural(")
            && index_plan.contains("accepted_index: &SchemaIndexInfo")
            && index_plan.contains("accepted_index: &SchemaExpressionIndexInfo")
            && !index_plan.contains("accepted_index: Option<&SchemaIndexInfo>")
            && !index_plan.contains("predicate_bridge: Option<&IndexModel>")
            && index_plan.contains(
                "IndexKey::new_from_slots_with_accepted_field_path_index_primary_key_value("
            )
            && index_plan.contains(
                "IndexKey::new_from_slots_with_accepted_expression_index_primary_key_value("
            )
            && index_plan.contains("fn accepted_index_fields_csv(")
            && index_plan.contains("fn accepted_expression_index_fields_csv(")
            && index_plan.contains("let index_store = accepted_index.store();")
            && index_plan.contains("let index_is_unique = accepted_index.unique();")
            && index_plan.contains("PredicateProgram::compile_with_row_contract(")
            && index_plan.contains("row_contract,")
            && !index_plan.contains("struct GeneratedExpressionIndex")
            && !index_plan.contains("GeneratedExpressionIndex")
            && !index_plan.contains("IndexKey::new_from_slots_with_contract(")
            && !index_plan.contains("fn generated_expression_index_fields_csv(")
            && !index_plan
                .contains("compile_generated_expression_index_membership_predicate_structural")
            && !index_plan
                .contains("plan_generated_expression_index_mutation_for_slot_reader_structural(")
            && !index_plan
                .contains("fn generated_predicate_program_for_accepted_field_path_index(")
            && !index_plan.contains("IndexKey::new_from_slots("),
        "forward-index mutation planning must iterate accepted index contracts directly without a generated expression-index write fallback",
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
        commit_prepare.contains("struct AcceptedCommitSchemaContracts")
            && commit_prepare.contains("accepted_commit_schema_contracts(")
            && commit_prepare
                .contains("SchemaInfo::from_accepted_snapshot_for_model_with_expression_indexes(")
            && commit_prepare.contains("schema_info: Option<SchemaInfo>")
            && commit_prepare_compact
                .contains("schema_info:(!accepted.persisted_snapshot().indexes().is_empty()).then(||")
            && commit_prepare
                .contains("schema_contracts.schema_info.is_some()")
            && commit_prepare_compact
                .contains("letSome(schema_info)=schema_contracts.schema_info.as_ref()else{returnOk(empty_forward_index_plan());};")
            && commit_prepare.contains("schema_info,")
            && commit_prepare.contains("&schema_contracts.row_contract")
            && commit_prepare.contains("fn empty_forward_index_plan() -> IndexMutationPlan")
            && !commit_prepare.contains("has_deferred_expression_indexes")
            && !commit_prepare.contains("authority.model.indexes()")
            && !commit_prepare.contains("index.has_expression_key_items()")
            && !commit_prepare.contains("authority.model.indexes().is_empty()"),
        "commit preflight must build expression-aware accepted schema info only when accepted indexes require forward-index planning",
    );
}

#[test]
fn forward_index_write_predicates_use_accepted_contracts() {
    let index_plan = read_source("src/db/index/plan/mod.rs");

    assert!(
        index_plan.contains("fn accepted_predicate_program_for_accepted_field_path_index(")
            && index_plan.contains("fn accepted_predicate_program_for_accepted_expression_index(")
            && index_plan.contains("parse_sql_predicate(predicate_sql)")
            && index_plan.contains("map_or(Predicate::False")
            && index_plan.contains("PredicateProgram::compile_with_row_contract(")
            && !index_plan.contains("requires generated predicate bridge")
            && !index_plan.contains("requires generated predicate program")
            && !index_plan
                .contains("fn generated_predicate_program_for_accepted_field_path_index("),
        "accepted write predicates must compile from accepted predicate SQL instead of generated predicate metadata",
    );
}

#[test]
fn unique_index_validation_splits_accepted_and_generated_authority() {
    let unique_plan = read_source("src/db/index/plan/unique.rs");

    assert!(
        unique_plan.contains("enum UniqueKeyAuthority")
            && unique_plan.contains("AcceptedFieldPath(&'a SchemaIndexInfo)")
            && unique_plan.contains("AcceptedExpression(&'a SchemaExpressionIndexInfo)")
            && unique_plan
                .contains("fn validate_unique_constraint_accepted_field_path_structural(")
            && unique_plan
                .contains("fn validate_unique_constraint_accepted_expression_structural(")
            && unique_plan.contains("read_contract: IndexReadContract<'_>")
            && unique_plan.contains("read_contract.unique()")
            && unique_plan.contains(
                "IndexKey::new_from_slots_with_accepted_field_path_index_primary_key_value("
            )
            && unique_plan.contains(
                "IndexKey::new_from_slots_with_accepted_expression_index_primary_key_value("
            )
            && unique_plan.contains("row_contract,")
            && !unique_plan.contains("GeneratedExpression(")
            && !unique_plan.contains("GeneratedExpressionIndex")
            && !unique_plan
                .contains("fn validate_unique_constraint_generated_expression_structural(")
            && !unique_plan.contains("IndexKey::new_from_slots_with_contract(")
            && !unique_plan.contains("index.model_index()")
            && !unique_plan.contains("GeneratedExpression(&'a IndexModel)")
            && !unique_plan.contains("model::index::IndexModel"),
        "unique-index validation must rebuild stored index keys through accepted index contracts and consume accepted uniqueness",
    );
}

#[test]
fn recovery_rebuild_expression_indexes_uses_accepted_commit_preflight() {
    let commit_rebuild = read_source("src/db/commit/rebuild.rs");
    let commit_prepare = read_source("src/db/commit/prepare.rs");
    let commit_prepare_compact = compact_source(&commit_prepare);
    let index_plan = read_source("src/db/index/plan/mod.rs");

    assert!(
        commit_rebuild.contains("ensure_accepted_schema_snapshot(")
            && commit_rebuild.contains("accepted_commit_schema_fingerprint(&accepted_schema)")
            && commit_rebuild.contains("CommitRowOp::new(")
            && commit_rebuild.contains("db.prepare_row_commit_op(&row_op)")
            && commit_rebuild.contains("for index_op in prepared.index_ops")
            && !commit_rebuild.contains("IndexKey::new_from_slots_with_contract(")
            && !commit_rebuild.contains("GeneratedExpressionIndex")
            && !commit_rebuild.contains("GeneratedExpressionCandidateIndex"),
        "startup index rebuild must derive expression-index operations through accepted commit preflight instead of rebuilding generated expression keys directly",
    );
    assert!(
        commit_prepare.contains("accepted_commit_schema_contracts(")
            && commit_prepare
                .contains("SchemaInfo::from_accepted_snapshot_for_model_with_expression_indexes(")
            && commit_prepare.contains("true,")
            && commit_prepare.contains("schema_contracts.schema_info.is_some()")
            && commit_prepare_compact
                .contains("letSome(schema_info)=schema_contracts.schema_info.as_ref()else{returnOk(empty_forward_index_plan());};")
            && commit_prepare.contains("prepare_forward_index_commit_leaf("),
        "commit preflight used by recovery must build expression-aware accepted SchemaInfo before indexed forward-index planning",
    );
    assert!(
        index_plan.contains("for accepted_index in accepted_expression_indexes")
            && index_plan
                .contains("plan_accepted_expression_index_mutation_for_slot_reader_structural(")
            && index_plan.contains(
                "IndexKey::new_from_slots_with_accepted_expression_index_primary_key_value("
            )
            && !index_plan.contains("GeneratedExpressionIndex")
            && !index_plan
                .contains("plan_generated_expression_index_mutation_for_slot_reader_structural("),
        "recovery rebuild reaches an accepted-expression-only forward-index planner",
    );
}

#[test]
fn accepted_schema_fingerprints_are_snapshot_only() {
    let fingerprint = read_source("src/db/schema/fingerprint.rs");
    let commit_prepare = read_source("src/db/commit/prepare.rs");
    let session_query_cache = read_source("src/db/session/query/cache.rs");
    let session_sql_cache = read_source("src/db/session/sql/cache.rs");

    assert!(
        fingerprint.contains("pub(in crate::db) fn accepted_commit_schema_fingerprint(")
            && fingerprint.contains("pub(in crate::db) fn accepted_schema_cache_fingerprint(")
            && fingerprint.contains(
                "pub(in crate::db) fn accepted_schema_cache_fingerprint_for_persisted_snapshot("
            )
            && fingerprint
                .contains("pub(in crate::db) fn accepted_schema_cache_fingerprint_from_raw(")
            && fingerprint.contains("schema_with_cache_fingerprint_version(schema)")
            && fingerprint.contains("normalized_schema.entity_path(),")
            && fingerprint.contains("hash_labeled_str(&mut hasher, \"entity_path\", entity_path)")
            && fingerprint.contains("encode_persisted_schema_snapshot(&normalized_schema)?")
            && !fingerprint.contains("accepted_commit_schema_fingerprint_for_model")
            && !fingerprint.contains("accepted_schema_cache_fingerprint_for_model")
            && !fingerprint.contains("fn accepted_schema_runtime_fingerprint_for_model")
            && !fingerprint.contains("hash_model_index_contract_for_cache(&mut hasher, model)")
            && !fingerprint.contains("hash_labeled_str(&mut hasher, \"model_path\", model.path())"),
        "accepted commit/cache fingerprints must derive from the accepted persisted schema snapshot, not generated EntityModel index contracts",
    );
    assert!(
        commit_prepare.contains("accepted_commit_schema_fingerprint(&accepted)")
            && session_query_cache.contains("accepted_schema_cache_fingerprint(accepted_schema)")
            && session_query_cache.contains("schema_version: SchemaVersion")
            && session_query_cache.contains("schema_fingerprint_method_version: u8")
            && session_sql_cache.contains("schema_version: SchemaVersion")
            && session_sql_cache.contains("schema_fingerprint_method_version: u8")
            && session_query_cache.contains("accepted_schema_cache_fingerprint_method_version()")
            && session_sql_cache.contains("accepted_schema_cache_fingerprint_method_version()")
            && !commit_prepare.contains("accepted_commit_schema_fingerprint_for_model(")
            && !session_query_cache.contains("accepted_schema_cache_fingerprint_for_model("),
        "runtime commit preparation and cache identity must call accepted-snapshot fingerprint APIs directly and carry schema version plus fingerprint method version with raw fingerprint bytes",
    );
}

#[test]
fn schema_admission_fingerprints_stay_out_of_query_hot_paths() {
    let transition = read_source("src/db/schema/transition.rs");
    let reconcile = read_source("src/db/schema/reconcile.rs");

    assert!(
        transition.contains("SchemaAdmissionIdentityComparison")
            && transition.contains("accepted_schema_admission_fingerprint(snapshot)?")
            && reconcile
                .contains("SchemaAdmissionIdentityComparison::from_snapshots(actual, expected)?")
            && reconcile.contains("schema_admission_rejection(admission_identity)"),
        "schema admission identity must stay rooted in schema transition/reconciliation authority",
    );

    for (label, source) in [
        ("session", read_rust_sources_under("src/db/session")),
        ("query", read_rust_sources_under("src/db/query")),
        ("executor", read_rust_sources_under("src/db/executor")),
    ] {
        assert!(
            !source.contains("accepted_schema_admission_fingerprint"),
            "{label} hot-path sources must not compute candidate admission fingerprints",
        );
        assert!(
            !source.contains("SchemaAdmissionIdentityComparison"),
            "{label} hot-path sources must not compare candidate admission identities",
        );
        assert!(
            !source.contains("schema_admission_rejection"),
            "{label} hot-path sources must not run schema-version admission policy",
        );
    }
}

#[test]
fn generated_schema_versions_stay_proposal_or_test_only() {
    let proposal = read_source("src/db/schema/proposal.rs");
    let reconcile = read_source("src/db/schema/reconcile.rs");

    assert!(
        proposal.contains(
            "declared_schema_version: SchemaVersion::new(model.declared_schema_version()),"
        ) && proposal
            .contains("SchemaRowLayout::new(self.declared_schema_version(), field_to_slot)")
            && proposal.contains("self.declared_schema_version(),")
            && reconcile.contains("compiled_schema_proposal_for_model(model)")
            && reconcile
                .contains("SchemaAdmissionIdentityComparison::from_snapshots(actual, expected)?"),
        "source-declared schema versions must enter runtime only through schema proposal and admission",
    );

    for (label, source) in [
        ("session", read_rust_sources_under("src/db/session")),
        ("query", read_rust_sources_under("src/db/query")),
        ("executor", read_rust_sources_under("src/db/executor")),
        ("fingerprint", read_source("src/db/schema/fingerprint.rs")),
        ("runtime", read_source("src/db/schema/runtime.rs")),
    ] {
        let production_source = strip_cfg_test_items(&source);
        assert!(
            !production_source.contains("declared_schema_version()"),
            "{label} production code must not read generated declared schema versions",
        );
    }

    let session_sql_cache = read_source("src/db/session/sql/cache.rs");
    let test_sql_cache_impl = session_sql_cache
        .split("#[cfg(test)]\nimpl SqlCompiledCommandCacheKey {")
        .nth(1)
        .expect("SQL cache test-key impl should be cfg(test)")
        .split("\nimpl<C: CanisterKind> DbSession<C>")
        .next()
        .expect("SQL cache test-key impl should end before DbSession impl");
    assert!(
        test_sql_cache_impl.contains("compiled_schema_proposal_for_model(E::MODEL)")
            && test_sql_cache_impl.contains("SQL cache test schema snapshot should be accepted"),
        "SQL generated-model cache-key bridge must remain an explicit test helper",
    );

    let save_executor = read_source("src/db/executor/mutation/save/mod.rs");
    let test_save_constructor = save_executor
        .split("#[cfg(test)]\n    #[must_use]\n    pub(in crate::db) fn new(")
        .nth(1)
        .expect("save generated-model constructor should be cfg(test)")
        .split("\n    // Borrow the accepted row contract selected by the session write boundary.")
        .next()
        .expect("save test constructor should end before accepted contract accessors");
    assert!(
        test_save_constructor
            .contains("AcceptedRowDecodeContract::from_generated_model_for_tests(E::MODEL)")
            && test_save_constructor.contains("compiled_schema_proposal_for_model(E::MODEL)")
            && test_save_constructor
                .contains("test save executor schema snapshot should be accepted"),
        "save generated-model schema proposal bridge must remain test-only",
    );

    let runtime = read_source("src/db/schema/runtime.rs");
    let test_runtime_bridge = runtime
        .split("#[cfg(test)]\n    pub(in crate::db) fn from_generated_model_for_tests(")
        .nth(1)
        .expect("runtime generated-model row contract bridge should be cfg(test)")
        .split("\n    /// Return the accepted physical slot count required by this row contract.")
        .next()
        .expect("runtime generated-model bridge should end before accepted accessors");
    assert!(
        test_runtime_bridge.contains("compiled_schema_proposal_for_model(model)")
            && test_runtime_bridge
                .contains("generated model proposal should produce an accepted test schema"),
        "generated-model row contract bridge must stay an explicit test helper",
    );
}

#[test]
fn catalog_diagnostics_expose_method_qualified_schema_fingerprints() {
    let schema_store = read_source("src/db/schema/store.rs");
    let diagnostics_model = read_source("src/db/diagnostics/model.rs");
    let storage_report = read_source("src/db/diagnostics/storage_report.rs");
    let diagnostics_tests = read_source("src/db/diagnostics/tests/mod.rs");

    assert!(
        schema_store.contains("schema_fingerprint_method_version: u8")
            && schema_store.contains("schema_fingerprint_method_version,")
            && schema_store.contains(
                "pub(in crate::db) const fn schema_fingerprint_method_version(self) -> u8"
            )
            && storage_report.contains("metadata.schema_fingerprint_method_version()")
            && diagnostics_model.contains("schema_fingerprint_method_version: Option<u8>")
            && diagnostics_model
                .contains("pub const fn schema_fingerprint_method_version(&self) -> Option<u8>")
            && diagnostics_tests.contains("\"schema_fingerprint_method_version\""),
        "catalog diagnostics must expose schema fingerprint bytes together with their method version",
    );
}

#[test]
fn query_catalog_context_exposes_full_accepted_identity_without_version_policy() {
    let session_mod = read_source("src/db/session/mod.rs");
    let session_sql_compiled = read_source("src/db/session/sql/compiled.rs");
    let session_query_cache = read_source("src/db/session/query/cache.rs");
    let session_sql_cache = read_source("src/db/session/sql/cache.rs");

    assert!(
        session_mod.contains("pub(in crate::db) const fn schema_version(&self) -> SchemaVersion")
            && session_mod.contains("self.identity.accepted_schema_version()")
            && session_mod
                .contains("pub(in crate::db) const fn fingerprint_method_version(&self) -> u8")
            && session_mod.contains("self.identity.fingerprint_method_version()")
            && session_mod.contains(
                "pub(in crate::db) const fn fingerprint(&self) -> CommitSchemaFingerprint"
            )
            && session_mod.contains("self.identity.accepted_schema_fingerprint()"),
        "accepted query catalog context must expose the full accepted version/method/fingerprint identity from the catalog header",
    );
    assert!(
        session_sql_compiled
            .contains("pub(in crate::db) const fn schema_version(&self) -> SchemaVersion")
            && session_sql_compiled.contains("self.catalog.schema_version()")
            && session_sql_compiled
                .contains("pub(in crate::db) const fn schema_fingerprint(&self)")
            && session_sql_compiled.contains("self.catalog.fingerprint()")
            && session_sql_compiled
                .contains("pub(in crate::db) const fn schema_fingerprint_method_version(&self)")
            && session_sql_compiled.contains("self.catalog.fingerprint_method_version()"),
        "compiled SQL execution context must consume accepted identity metadata from the same query catalog context",
    );
    for (label, source) in [
        ("query cache", session_query_cache),
        ("SQL cache", session_sql_cache),
        ("compiled SQL context", session_sql_compiled),
    ] {
        assert!(
            !source.contains("schema_admission_rejection")
                && !source.contains("SchemaAdmissionIdentityComparison")
                && !source.contains("accepted_schema_admission_fingerprint"),
            "{label} must consume accepted identity after admission instead of owning schema-version policy",
        );
    }
}

#[test]
fn schema_store_publication_stays_version_passive() {
    let store = read_source("src/db/schema/store.rs");
    let store_compact = compact_source(&store);
    let reconcile = read_source("src/db/schema/reconcile.rs");

    assert!(
        store_compact
            .contains("letkey=RawSchemaKey::from_entity_version(entity,snapshot.version());")
            && store.contains("RawSchemaSnapshot::from_persisted_snapshot(snapshot)?")
            && store.contains("pub(in crate::db) fn insert_persisted_snapshot_if_latest_identity(")
            && store.contains("self.insert_persisted_snapshot(expected.entity_tag(), snapshot)"),
        "schema store publication must key persisted snapshots by the snapshot-declared version",
    );
    assert!(
        reconcile.contains(
            "schema_store.insert_persisted_snapshot_if_latest_identity(accepted_before_identity, after)",
        ),
        "SQL DDL publication must condition accepted-after storage on the binding-time accepted identity without version synthesis",
    );
    assert!(
        reconcile.contains("let proposal = compiled_schema_proposal_for_model(model);")
            && reconcile.contains("let expected = proposal.initial_persisted_schema_snapshot();")
            && reconcile.contains("schema_store.insert_persisted_snapshot(entity_tag, &expected)")
            && reconcile.contains("store.with_schema_mut(|schema_store| {\n        schema_store.insert_persisted_snapshot(entity_tag, &expected)\n    })"),
        "first-contact schema reconciliation must publish the proposal-produced snapshot without version synthesis",
    );

    for (label, source) in [("store", store), ("reconcile", reconcile)] {
        let compact = compact_source(&source);
        assert!(
            !source.contains("next_schema")
                && !source.contains("next_version")
                && !compact.contains("snapshot.version().get()+1")
                && !compact.contains("snapshot.version().get().saturating_add(1)")
                && !compact.contains("row_layout().version().get()+1")
                && !compact.contains("row_layout().version().get().saturating_add(1)"),
            "{label} schema publication code must not auto-increment schema versions",
        );
    }
}

#[test]
fn schema_version_hard_cut_rejects_obsolete_and_non_positive_internal_formats() {
    let codec = read_source("src/db/schema/codec.rs");
    let codec_compact = compact_source(&codec);
    let integrity = read_source("src/db/schema/integrity.rs");
    let entity_model = read_source("src/model/entity.rs");
    let macro_zero = read_source("../../testing/macro-tests/tests/ui/schema_version_zero.stderr");

    assert!(
        codec.contains("const SCHEMA_SNAPSHOT_CODEC_VERSION: u32 =")
            && codec_compact.contains("ifself.codec_version!=SCHEMA_SNAPSHOT_CODEC_VERSION")
            && codec.contains("unsupported persisted schema snapshot codec version"),
        "schema snapshot codec must hard-cut obsolete internal snapshot formats",
    );
    assert!(
        integrity.contains("version.get() == 0")
            && integrity.contains("schema_version must be positive")
            && entity_model.contains("schema_version > 0")
            && entity_model.contains("generated schema_version must be positive")
            && macro_zero.contains("schema_version must be a positive integer"),
        "generated and persisted schema_version boundaries must reject non-positive versions",
    );
    assert!(
        !codec.contains("unwrap_or(SchemaVersion::initial())")
            && !integrity.contains("unwrap_or(SchemaVersion::initial())"),
        "schema decode/integrity must not infer missing versions from SchemaVersion::initial()",
    );
}

#[test]
fn workspace_entity_declarations_keep_explicit_schema_versions() {
    let manifest_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let repo_roots = [
        "../../schema",
        "../../testing/macro-tests/src",
        "../../testing/macro-tests/tests/ui",
        "../../testing/wasm-helpers/src",
    ];
    let omitted_version_negative_fixture = "testing/macro-tests/tests/ui/schema_version_missing.rs";

    let mut sources = Vec::new();
    for repo_root in repo_roots {
        let mut root = manifest_root.clone();
        root.push(repo_root);
        sources.extend(rust_sources_under_path(root));
    }
    sources.sort();
    sources.dedup();

    let mut violations = Vec::new();
    for source_path in sources {
        let relative = source_path
            .strip_prefix(manifest_root.join("../.."))
            .unwrap_or_else(|err| {
                panic!(
                    "failed to compute repo-relative source path for {}: {err}",
                    source_path.display(),
                )
            })
            .to_string_lossy()
            .replace('\\', "/");
        if relative == omitted_version_negative_fixture {
            continue;
        }

        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        for entity_attr in entity_attribute_blocks(&source) {
            if !entity_attr.contains("schema_version") {
                violations.push(relative.clone());
            }
        }
    }

    assert!(
        violations.is_empty(),
        "all non-negative-test generated entity declarations must carry explicit schema_version. Violations: {}",
        violations.join(", "),
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
    let covering = read_source("src/db/executor/covering.rs");

    assert!(
        access_lowering.contains("pub(in crate::db) struct LoweredIndexScanContract")
            && access_lowering.contains("store_path: String")
            && access_lowering.contains("scan_contract: LoweredIndexScanContract")
            && access_lowering.contains("pub(in crate::db) fn scan_contract(")
            && access_lowering.contains("pub(in crate::db) const fn store_path(")
            && !access_lowering.contains(
                "pub(in crate::db) struct LoweredIndexPrefixSpec {\n    index: IndexModel"
            )
            && !access_lowering.contains(
                "pub(in crate::db) struct LoweredIndexRangeSpec {\n    index: IndexModel"
            )
            && index_scan.contains("index: LoweredIndexScanContract")
            && index_scan.contains("index: &LoweredIndexScanContract")
            && index_scan.contains("index.name()")
            && physical_access.contains("IndexScan::range_structural(")
            && covering.contains("let scan_contract = spec.scan_contract()")
            && covering.contains("scan_contract.store_path()")
            && covering.contains("IndexScan::components_structural("),
        "lowered executor scan specs must reduce generated index models to scan facts after raw bounds are materialized",
    );
}

#[test]
fn runtime_access_shape_facts_use_reduced_index_shape_facts() {
    let access_shape_facts = read_source("src/db/access/shape_facts.rs");
    let execution_path_payload = read_source("src/db/access/execution_contract/types.rs");
    let route_pushdown = read_source("src/db/executor/planning/route/pushdown.rs");
    let aggregate_capability = read_source("src/db/executor/aggregate/capability.rs");
    let logical_semantics = read_source("src/db/query/plan/semantics/logical.rs");
    let access_plan = read_source("src/db/query/plan/access_plan.rs");
    let access_model_only = read_source("src/db/access/model_only.rs");
    let access_path = read_source("src/db/access/path.rs");
    let access_plan_core = read_source("src/db/access/plan.rs");
    let access_choice = read_source("src/db/query/plan/access_choice/mod.rs");
    let cache_key = read_source("src/db/query/intent/cache_key.rs");
    let planner_prefix = read_source("src/db/query/plan/planner/prefix.rs");
    let planner_compare = read_source("src/db/query/plan/planner/compare.rs");
    let planner_range = read_source("src/db/query/plan/planner/range/extract.rs");

    assert!(
        access_shape_facts.contains("pub(in crate::db) struct IndexShapeDetails")
            && access_shape_facts.contains("index: SemanticIndexAccessContract")
            && access_shape_facts.contains("fn name(&self) -> &str")
            && access_shape_facts.contains("fn key_items(&self) -> SemanticIndexKeyItemsRef")
            && access_shape_facts.contains("fn key_arity(&self) -> usize")
            && !access_shape_facts.contains("index: IndexModel,\n    slot_arity: usize"),
        "runtime access-shape facts must carry reduced index details instead of exposing generated IndexModel",
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
            && access_model_only.contains("Module: access::model_only")
            && access_model_only.contains("fn model_only_from_generated_index(index: IndexModel)")
            && access_model_only.contains("Accepted runtime planning, explain, writes")
            && access_path.contains("fn from_access_contract(")
            && access_path.contains("index: SemanticIndexAccessContract")
            && !access_path.contains("fn from_index(index: IndexModel)")
            && !access_path.contains("fn from_generated_index(index: IndexModel)")
            && !access_path.contains("SemanticIndexAccessContract::from_index")
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
        access_path.contains("pub(in crate::db) fn is_filtered(")
            && planner_ranking.contains("fn access_candidate_score_from_index_contract(")
            && planner_ranking.contains("index.is_filtered()")
            && planner_ranking.contains("selected_index_contract_satisfies_secondary_order(")
            && evaluator.contains("index: SemanticIndexAccessContract")
            && evaluator.contains("access_candidate_score_from_index_contract(")
            && !evaluator.contains("candidate_satisfies_secondary_order(")
            && evaluator_prefix.contains("evaluate_prefix_compare_candidate_from_contract(")
            && evaluator_prefix.contains("index_contract.key_arity()")
            && evaluator_prefix.contains("index_contract.is_filtered()")
            && evaluator_range.contains("evaluate_range_candidate_from_contract(")
            && evaluator_range.contains("index_contract.is_filtered()")
            && evaluator_range.contains("single_range_compare_bound_count(&index_contract")
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
            && session_mod.contains("fn accepted_save_contract_for_descriptor<E>")
            && session_mod.contains("SchemaInfo::from_accepted_snapshot_for_model(E::MODEL, accepted_schema)")
            && session_mod.contains("accepted_save_contract_for_descriptor::<E>(&accepted_schema, &accepted_row_layout)")
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
    let session_query_cache = read_source("src/db/session/query/cache.rs");
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
            && session_query_explain.contains("let authority = prepared_plan.authority();")
            && session_query_explain.contains("authority.accepted_schema_info()")
            && !session_query_explain.contains("ensure_accepted_schema_snapshot::<E>()")
            && !session_query_explain.contains("plan.finalize_access_choice_for_model_only_with_indexes(")
            && session_query_cache.contains("accepted_schema_catalog_context_for_query::<E>()")
            && session_sql_explain.contains(
                "plan.finalize_access_choice_for_model_with_accepted_indexes_and_schema("
            )
            && session_sql_explain
                .contains("bind_lowered_sql_query_structural_with_schema(")
            && session_sql_explain.contains(
                "bind_lowered_sql_explain_global_aggregate_structural_with_schema("
            )
            && session_sql_explain_compact.contains(
                "fnexplain_lowered_sql_for_authority(&self,lowered:&LoweredSqlCommand,authority:EntityAuthority,accepted_schema:&AcceptedSchemaSnapshot,schema_info:&SchemaInfo,"
            )
            && session_sql_explain_compact.contains(
                "fnexplain_lowered_sql_execution_for_authority(&self,lowered:&LoweredSqlCommand,authority:EntityAuthority,accepted_schema:&AcceptedSchemaSnapshot,schema_info:&SchemaInfo,"
            )
            && !session_sql_explain.contains("plan.finalize_access_choice_for_model_only_with_indexes(")
            && !session_sql_explain.contains("bind_lowered_sql_query_structural(")
            && !session_sql_explain
                .contains("bind_lowered_sql_explain_global_aggregate_structural(")
            && sql_aggregate_binding.contains(
                "apply_lowered_base_query_shape_with_schema("
            ),
        "session explain binding and access-choice finalization must use accepted authority/catalog SchemaInfo instead of generated schema fallback",
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
            && query_plan_pipeline
                .contains("&VisibleIndexes::generated_model_only(query.model().indexes())")
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
fn remaining_runtime_generated_index_sets_are_model_only() {
    let access_choice = read_source("src/db/query/plan/access_choice/mod.rs");
    let access_plan = read_source("src/db/query/plan/access_plan.rs");
    let commit_prepare = read_source("src/db/commit/prepare.rs");
    let executor_explain = read_source("src/db/executor/explain/mod.rs");
    let index_plan = read_source("src/db/index/plan/mod.rs");
    let planner_mod = read_source("src/db/query/plan/planner/mod.rs");
    let query_plan_pipeline = read_source("src/db/query/plan/pipeline.rs");
    let query_plan = read_source("src/db/query/plan/mod.rs");
    let session_mod = read_source("src/db/session/mod.rs");
    let session_query_cache = read_source("src/db/session/query/cache.rs");

    assert!(
        executor_explain.contains("fn finalize_explain_access_choice_for_model_only(")
            && executor_explain.contains("self.model().indexes()")
            && executor_explain.contains("fn explain_execution_descriptor_for_model_only(")
            && executor_explain.contains("fn explain_execution_for_model_only(")
            && executor_explain
                .contains("&VisibleIndexes::generated_model_only(E::MODEL.indexes())")
            && query_plan_pipeline.contains("fn build_query_model_plan_for_model_only")
            && query_plan_pipeline
                .contains("&VisibleIndexes::generated_model_only(query.model().indexes())"),
        "remaining generated index-set fallbacks must stay on explicit model-only explain/planning surfaces",
    );
    assert!(
        !query_plan.contains("generated_expression_candidate_indexes: Cow")
            && !query_plan.contains("struct GeneratedExpressionCandidateIndex")
            && query_plan.contains("semantic_access_contract: SemanticIndexAccessContract")
            && query_plan.contains("generated_model_only_indexes: Cow")
            && query_plan.contains("pub(in crate::db) const fn generated_model_only(")
            && query_plan.contains("fn generated_model_only_for_test(")
            && !query_plan.contains("fn schema_owned(")
            && !query_plan.contains("fn planner_visible(")
            && query_plan.contains("pub(in crate::db) fn generated_model_only_indexes(&self)")
            && query_plan.contains("generated_model_only_indexes: Cow::Borrowed(indexes)")
            && query_plan.contains("generated_model_only_indexes: Cow::Borrowed(&[])")
            && session_mod.contains("VisibleIndexes::accepted_schema_visible(schema_info)")
            && session_query_cache.contains("VisibleIndexes::accepted_schema_visible(schema_info)")
            && !session_mod.contains("VisibleIndexes::accepted_schema_visible(model.indexes(),")
            && !session_query_cache
                .contains("VisibleIndexes::accepted_schema_visible(model.indexes(),")
            && !session_mod.contains(".generated_expression_candidate_indexes()")
            && !session_query_cache.contains(".generated_expression_candidate_indexes()"),
        "accepted runtime visible indexes must not receive generated model indexes; generated index sets remain model-only",
    );
    assert!(
        !access_choice.contains("generated_expression_candidate_indexes")
            && !access_plan.contains("generated_expression_candidate_indexes")
            && !planner_mod.contains("generated_expression_candidate_indexes")
            && !access_choice.contains("GeneratedExpressionCandidateIndex")
            && !access_plan.contains("GeneratedExpressionCandidateIndex")
            && !planner_mod.contains("GeneratedExpressionCandidateIndex"),
        "accepted-runtime expression candidate entrypoints must be removed rather than wrapped around generated IndexModel slices",
    );
    assert!(
        access_choice.contains("fn semantic_candidate_indexes_from_generated_model_only(")
            && access_choice
                .contains("SemanticIndexAccessContract::model_only_from_generated_index(*index)")
            && planner_mod.contains("fn semantic_candidate_indexes_from_generated_model_only(")
            && planner_mod
                .contains("SemanticIndexAccessContract::model_only_from_generated_index(*index)")
            && !session_mod
                .contains("SemanticIndexAccessContract::model_only_from_generated_index")
            && !session_query_cache
                .contains("SemanticIndexAccessContract::model_only_from_generated_index")
            && !commit_prepare
                .contains("SemanticIndexAccessContract::model_only_from_generated_index")
            && !index_plan.contains("SemanticIndexAccessContract::model_only_from_generated_index"),
        "model_only_from_generated_index must stay isolated to generated/model-only planner projection and tests, not accepted session/write/recovery runtime",
    );
}

#[test]
fn executor_plan_validation_uses_accepted_schema_info() {
    let access_mod = read_source("src/db/access/mod.rs");
    let access_validate = read_source("src/db/access/validate.rs");
    let entity_authority = read_source("src/db/executor/authority/entity.rs");

    assert!(
        entity_authority.contains("if !plan.has_static_execution_planning_contract()")
            && entity_authority
                .contains("executor plan validation requires planner-frozen static execution planning contract",)
            && entity_authority.contains("executor plan validation requires accepted schema info")
            && entity_authority.contains("validate_access_runtime_invariants_with_schema(")
            && !entity_authority.contains("fn schema_info(")
            && !entity_authority
                .contains("SchemaInfo::cached_for_generated_entity_model(self.model)")
            && !entity_authority.contains("validate_access_runtime_invariants_model(")
            && !entity_authority.contains("validate_access_structure_model(self.schema_info()"),
        "executor plan validation must require planner-frozen static execution planning contract and authority-carried accepted schema info instead of reopening generated schema authority",
    );
    assert!(
        access_mod.contains("validate_access_runtime_invariants_with_schema")
            && access_validate.contains("schema.field_is_indexed(field)")
            && !access_validate.contains("fn validate_index_reference_model("),
        "runtime access validation must check index references through schema info instead of generated entity model membership",
    );
}

#[test]
fn trivial_load_fast_path_uses_accepted_schema_authority() {
    let query_intent_model = read_source("src/db/query/intent/model.rs");
    let query_intent = read_source("src/db/query/intent/query.rs");
    let query_pipeline = read_source("src/db/query/plan/pipeline.rs");
    let session_query_cache = read_source("src/db/session/query/cache.rs");
    let query_semantics = read_source("src/db/query/plan/semantics/logical.rs");

    assert!(
        query_intent_model.contains("trivial_scalar_load_fast_path_eligible_with_schema(")
            && query_intent_model.contains("schema_info: &SchemaInfo")
            && query_intent_model.contains(".primary_key_names()")
            && !query_intent_model.contains("self.model.primary_key_names()"),
        "trivial scalar-load fast-path eligibility must compare primary-key ordering against accepted SchemaInfo",
    );
    assert!(
        query_intent.contains("trivial_scalar_load_fast_path_eligible_with_schema(")
            && query_pipeline
                .contains(".trivial_scalar_load_fast_path_eligible_with_schema(&schema_info)")
            && session_query_cache
                .contains(".trivial_scalar_load_fast_path_eligible_with_schema(&schema_info)")
            && !query_intent.contains("fn trivial_scalar_load_fast_path_eligible(&self) -> bool")
            && !query_pipeline.contains(".trivial_scalar_load_fast_path_eligible()")
            && !session_query_cache.contains(".trivial_scalar_load_fast_path_eligible()"),
        "runtime trivial scalar-load cache/build paths must thread accepted SchemaInfo instead of reopening generated primary-key metadata",
    );
    assert!(
        query_semantics.contains(
            "#[cfg(test)]\n    pub(in crate::db) fn finalize_planner_route_profile_for_model("
        ) && query_semantics.contains("#[cfg(test)]\nfn ordered_primary_key_names(")
            && query_semantics.contains("project_planner_route_profile_for_schema("),
        "generated-model route-profile projection must remain test-only while runtime uses schema-owned route-profile projection",
    );
}

#[test]
fn raw_entity_authority_bootstrap_stays_layout_free() {
    let entity_authority = read_source("src/db/executor/authority/entity.rs");
    let executor_explain = read_source("src/db/executor/explain/mod.rs");
    let route_shape = read_source("src/db/executor/planning/route/contracts/shape.rs");
    let query_plan_covering = read_source("src/db/query/plan/covering/mod.rs");
    let session_query_explain = read_source("src/db/session/query/explain.rs");
    let shared_prepared_plan =
        read_source("src/db/executor/prepared_execution_plan/shared_plan.rs");
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
            && shared_prepared_plan.contains("pub(in crate::db) fn authority(&self)")
            && session_query_explain.contains("let authority = prepared_plan.authority();")
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
            && route_shape.contains("schema.primary_key_names()")
            && route_shape.contains("primary_key_names.len() == 1"),
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
    let sql_semantic_compiler = read_source("src/db/session/sql/compile/semantic_compiler.rs");
    let sql_lowering_prepare = read_source("src/db/sql/lowering/prepare.rs");
    let sql_lowering_select = read_source("src/db/sql/lowering/select/mod.rs");
    let sql_global_aggregate_binding =
        read_source("src/db/sql/lowering/aggregate/command/binding.rs");

    assert!(
        sql_semantic_compiler
            .contains("Self::compile_explain(statement, entity_name, model, schema)",)
            && sql_semantic_compiler.contains(
                "lower_sql_command_from_prepared_statement_with_schema(prepared, model, schema)",
            )
            && !sql_semantic_compiler
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
            && sql_global_aggregate_binding.contains(
                "compile_structural_sql_global_aggregate_command_from_prepared_with_schema"
            )
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
    let session_query_cache_compact = compact_source(&session_query_cache);
    let session_sql = read_source("src/db/session/sql/mod.rs");
    let session_sql_cache = read_source("src/db/session/sql/cache.rs");
    let session_sql_compile_cache = read_source("src/db/session/sql/compile_cache.rs");
    let session_sql_compile_cache_compact = compact_source(&session_sql_compile_cache);
    let session_sql_execute = read_source("src/db/session/sql/execute/mod.rs");
    let session_sql_execute_compact = compact_source(&session_sql_execute);
    let session_sql_explain = read_source("src/db/session/sql/execute/explain.rs");
    let session_sql_write = read_rust_sources_under("src/db/session/sql/execute/write");
    let entity_authority = read_source("src/db/executor/authority/entity.rs");

    assert!(
        session_mod.contains("pub(in crate::db) fn accepted_entity_authority_for_schema<E>")
            && !session_mod.contains("pub(in crate::db) fn accepted_entity_authority<E>")
            && session_mod.contains("EntityAuthority::from_accepted_schema_for_type::<E>(")
            && !session_mod.contains("EntityAuthority::for_generated_type_for_test::<E>()")
            && entity_authority.contains("fn from_accepted_schema_for_type<E>")
            && entity_authority
                .contains("AcceptedRowLayoutRuntimeContract::from_generated_compatible_schema(")
            && entity_authority.contains("with_accepted_row_decode_contract(")
            && session_query_cache.contains("accepted_schema_catalog_context_for_query::<E>()")
            && session_query_cache_compact
                .contains("catalog.accepted_entity_authority_for::<E>()")
            && session_query_cache.contains(
                "cached_shared_query_plan_for_accepted_authority_with_schema_fingerprint_and_visibility(",
            )
            && !session_query_cache.contains("EntityAuthority::for_generated_type_for_test::<E>()")
            && session_sql_cache.contains("accepted_schema_catalog_context_for_query::<E>()")
            && !session_sql_cache.contains("EntityAuthority::for_generated_type_for_test::<E>()")
            && session_sql_compile_cache_compact
                .contains("catalog.accepted_entity_authority_for::<E>()")
            && session_sql_compile_cache.contains("Some(authority)")
            && session_sql_compile_cache.contains("None")
            && !session_sql_compile_cache
                .contains("EntityAuthority::for_generated_type_for_test::<E>()")
            && session_sql.contains("accepted_schema_catalog_context_for_query::<E>()")
            && !session_sql.contains("accepted_entity_authority::<E>()")
            && session_sql.contains(
                "sql_select_prepared_plan_for_accepted_authority_with_schema_fingerprint(",
            )
            && session_sql_execute.contains("sql_select_prepared_plan_for_entity::<E>(query)")
            && session_sql_execute.contains("accepted_schema_catalog_context_for_query::<E>()")
            && !session_sql_execute.contains("accepted_entity_authority::<E>()")
            && session_sql_execute.contains("context.accepted_authority()")
            && session_sql_execute_compact
                .contains("context.accepted_catalog().accepted_entity_authority_for::<E>()")
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
fn sql_write_selectors_reuse_accepted_authority_schema_info() {
    let insert = read_source("src/db/session/sql/execute/write/insert.rs");
    let update = read_source("src/db/session/sql/execute/write/update.rs");
    let update_compact = compact_source(&update);
    let write_mod = read_source("src/db/session/sql/execute/write/mod.rs");
    let write_mod_compact = compact_source(&write_mod);

    assert!(
        insert.contains("let schema_info = authority.accepted_schema_info().ok_or_else(|| {")
            && insert.contains("let save_schema_info = schema_info.clone();")
            && insert.contains(") -> Result<crate::db::schema::SchemaInfo, QueryError>")
            && insert.contains("Ok(save_schema_info)")
            && insert.contains("save_schema_info = Some(self.execute_sql_insert_select_source_patches::<E>(")
            && insert.contains(
                "accepted_sql_write_save_contract::<E>(&schema, &descriptor, save_schema_info)",
            )
            && insert.contains(
                "QueryError::invariant(\"SQL INSERT SELECT authority must carry accepted schema info\")",
            )
            && insert.contains("bind_prepared_sql_select_statement_structural_with_schema(")
            && !insert.contains("SchemaInfo::from_accepted_snapshot_for_model"),
        "SQL INSERT SELECT selector binding must reuse accepted authority SchemaInfo instead of rebuilding the same accepted schema projection",
    );
    assert!(
        update.contains("let schema_info = authority.accepted_schema_info().ok_or_else(|| {")
            && update.contains("let save_schema_info = schema_info.clone();")
            && update.contains(
                "accepted_sql_write_save_contract::<E>(&schema, &descriptor, Some(save_schema_info))",
            )
            && update.contains(
                "QueryError::invariant(\"SQL UPDATE selector authority must carry accepted schema info\")",
            )
            && update.contains(
                "fn sql_update_selector_query<E>(\n        schema_info: &crate::db::schema::SchemaInfo,"
            )
            && update_compact.contains(
                "letselector=bind_sql_update_selector_query_structural_with_schema(E::MODEL,statement,MissingRowPolicy::Ignore,schema_info,"
            )
            && !update.contains("SchemaInfo::from_accepted_snapshot_for_model"),
        "SQL UPDATE selector binding must reuse accepted authority SchemaInfo instead of rebuilding the same accepted schema projection",
    );
    assert!(
        write_mod_compact.contains(
            "fnaccepted_sql_write_save_contract<E>(schema:&AcceptedSchemaSnapshot,descriptor:&AcceptedRowLayoutRuntimeContract<'_>,schema_info:Option<SchemaInfo>,"
        ) && write_mod.contains("if let Some(schema_info) = schema_info {")
            && write_mod.contains("accepted_commit_schema_fingerprint(schema)")
            && write_mod.contains("accepted_save_contract_for_descriptor::<E>(schema, descriptor)"),
        "SQL write save-contract construction must reuse caller-provided accepted SchemaInfo when selector authority already built it",
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
                "authority.with_accepted_row_decode_contract(row_proof,row_decode_contract,schema_info",
            ),
        "entity authority must carry accepted schema info into scalar cursor validation",
    );
}

#[test]
fn prepared_static_contract_finalization_uses_authority_schema_info() {
    let entity_authority = read_source("src/db/executor/authority/entity.rs");
    let query_plan_logical = read_source("src/db/query/plan/semantics/logical.rs");
    let predicate_runtime = read_source("src/db/predicate/runtime/mod.rs");
    let predicate_capability = read_source("src/db/predicate/capability.rs");
    let schema_info = read_source("src/db/schema/info.rs");

    assert!(
        entity_authority.contains("plan.finalize_static_execution_planning_contract_for_model_with_schema(")
            && entity_authority.contains("self.model,")
            && entity_authority.contains("schema_info,")
            && !entity_authority
                .contains(".finalize_static_execution_planning_contract_for_model_only(self.model)")
            && !entity_authority.contains("PreparedShapeFinalizationOutcome::GeneratedFallback")
            && query_plan_logical.contains(
                "#[cfg(test)]\n    pub(in crate::db) fn finalize_static_execution_planning_contract_for_model_only("
            ),
        "prepared execution finalization must use authority-carried schema info and keep generated static execution-planning contract finalization model-only and test-only",
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
            && session_global_aggregate
                .contains("accepted_schema_catalog_context_for_query::<E>()")
            && session_global_aggregate.contains("catalog.accepted_schema_info_for::<E>()")
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
    let session_sql_compile = read_source("src/db/session/sql/compile/semantic_compiler.rs");
    let session_mod = read_source("src/db/session/mod.rs");
    let symbols = read_source("src/db/query/plan/validate/symbols.rs");
    let query_plan_group = read_source("src/db/query/plan/group.rs");
    let query_plan_logical = read_source("src/db/query/plan/semantics/logical.rs");
    let sql_aggregate_strategy = read_source("src/db/sql/lowering/aggregate/strategy.rs");
    let session_global_aggregate = read_source("src/db/session/sql/execute/global_aggregate.rs");
    let accepted_schema_info_for_entity = compact_source(
        session_mod
            .split("pub(in crate::db) fn accepted_schema_info_for_entity<E>")
            .nth(1)
            .expect("session should expose accepted schema-info helper")
            .split("// Derive typed executor authority")
            .next()
            .expect("accepted schema-info helper section should be bounded"),
    );

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
            && session_mod.contains("pub(in crate::db) fn accepted_schema_info_for<E>")
            && accepted_schema_info_for_entity
                .contains("letcatalog=self.accepted_schema_catalog_context_for_query::<E>()?;")
            && accepted_schema_info_for_entity
                .contains("Ok(catalog.accepted_schema_info_for::<E>())")
            && !accepted_schema_info_for_entity.contains("ensure_accepted_schema_snapshot")
            && !accepted_schema_info_for_entity
                .contains("SchemaInfo::from_accepted_snapshot_for_model")
            && symbols
                .contains("pub(in crate::db) fn resolve_aggregate_target_field_slot_with_schema(")
            && symbols.contains("pub(in crate::db) fn resolve_group_field_slot_with_schema(")
            && symbols.contains(".field_slot_index(field)")
            && !symbols.contains("fn resolve_aggregate_target_field_slot(")
            && !symbols.contains("model.resolve_field_slot(field)?;"),
        "session and planner symbol helpers must expose accepted-schema field-slot resolution",
    );
    assert!(
        session_global_aggregate.contains("accepted_schema_catalog_context_for_query::<E>()")
            && session_global_aggregate.contains("catalog.accepted_schema_info_for::<E>()")
            && session_global_aggregate.contains(
                "cached_shared_query_plan_for_entity_with_catalog::<E>(&query, &catalog)",
            )
            && !session_global_aggregate.contains("ensure_accepted_schema_snapshot::<E>()"),
        "SQL global aggregate execution should reuse one accepted catalog context for schema info and shared query-plan cache lookup",
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

#[test]
fn storage_capability_policy_roots_do_not_branch_on_concrete_storage_modes() {
    let checked_roots = [
        "src/db/executor",
        "src/db/relation",
        "src/db/commit",
        "src/db/diagnostics",
    ];
    let checked_files = ["src/db/schema/reconcile.rs"];
    let allowed_storage_mode_files = [
        "src/db/diagnostics/storage_report.rs",
        "src/db/diagnostics/model.rs",
    ];
    let forbidden = [
        "StoreStorage::Heap",
        "StoreStorage::Stable",
        "StoreStorage::Journaled",
        "StoreRuntimeStorageMode::Heap",
        "StoreRuntimeStorageMode::Stable",
        "StoreRuntimeStorageMode::Journaled",
        "DataStoreBackend::Heap",
        "DataStoreBackend::Journaled",
        "IndexStoreBackend::Heap",
        "IndexStoreBackend::Journaled",
        "SchemaStoreBackend::Heap",
        "SchemaStoreBackend::Journaled",
        ".storage_mode()",
    ];
    let mut sources = Vec::new();
    for root in checked_roots {
        sources.extend(rust_sources_under(root));
    }
    sources.extend(checked_files.iter().map(|path| {
        let mut absolute = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        absolute.push(path);
        absolute
    }));
    sources.sort();

    let mut violations = Vec::new();
    for path in sources {
        let relative = relative_source_path(&path);
        if relative.contains("/tests/") {
            continue;
        }
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
        let production_source = strip_cfg_test_items(&source);
        let symbols: Vec<&str> = forbidden
            .iter()
            .copied()
            .filter(|symbol| {
                if (*symbol == ".storage_mode()" || symbol.starts_with("StoreRuntimeStorageMode::"))
                    && allowed_storage_mode_files.contains(&relative.as_str())
                {
                    return false;
                }
                production_source.contains(symbol)
            })
            .collect();
        if !symbols.is_empty() {
            violations.push(format!("{relative} ({})", symbols.join(", ")));
        }
    }

    assert!(
        violations.is_empty(),
        "runtime policy roots must consume storage capability axes, not concrete storage modes:\n{}",
        violations.join("\n"),
    );
}

#[test]
fn relation_and_commit_policy_consume_storage_capability_axes() {
    let relation_save_validate = read_source("src/db/relation/save_validate.rs");
    let commit_window = read_source("src/db/executor/mutation/commit_window.rs");
    let commit_replay = read_source("src/db/commit/replay.rs");
    let commit_rebuild = read_source("src/db/commit/rebuild.rs");

    assert!(
        relation_save_validate.contains(".storage_capabilities().relation_source()")
            && relation_save_validate.contains(".storage_capabilities().relation_target()")
            && relation_save_validate.contains("StoreRelationSourceCapability::DurableSource")
            && relation_save_validate.contains("StoreRelationTargetCapability::VolatileTarget")
            && !relation_save_validate.contains("data_is_heap_storage")
            && !relation_save_validate.contains("StoreRuntimeStorageMode::Journaled")
            && !relation_save_validate.contains("strong_relation_heap_target_unsupported"),
        "strong relation admission must be expressed through source/target relation capability axes",
    );
    assert!(
        commit_window.contains(".storage_capabilities().commit_participation()")
            && commit_window.contains("StoreCommitParticipation::Durable")
            && commit_window.contains("StoreCommitParticipation::LiveOnly")
            && !commit_window.contains(".storage_mode()")
            && !commit_window.contains("StoreRuntimeStorageMode::"),
        "mutation commit classification must consume commit-participation capabilities",
    );
    assert!(
        commit_replay.contains(".storage_capabilities().recovery()")
            && commit_replay.contains("StoreRecoveryCapability::None")
            && commit_rebuild.contains(".storage_capabilities().recovery()")
            && commit_rebuild.contains("StoreRecoveryCapability::StableCommitReplay"),
        "commit recovery replay/rebuild must consume recovery capabilities instead of replaying every registered store",
    );
}

#[test]
fn mutation_durability_trace_stays_proof_facing_and_not_diagnostics_authority() {
    let commit_window = read_source("src/db/executor/mutation/commit_window.rs");
    let save_shared = read_source("src/db/executor/mutation/save/shared.rs");
    let metrics_sink = read_source("src/metrics/sink.rs");
    let diagnostics = read_rust_sources_under("src/db/diagnostics");

    assert!(
        commit_window.contains("fn record_mutation_commit_plan(")
            && commit_window.contains("record(MetricsEvent::MutationCommitPlan"),
        "mutation durability classification should have one executor-owned trace emitter",
    );
    assert!(
        save_shared.contains("record_mutation_commit_plan(E::PATH, commit_class);")
            && !save_shared.contains("MetricsEvent::MutationCommitPlan"),
        "save-row fast paths should use the executor-owned mutation durability trace helper",
    );
    assert!(
        metrics_sink.contains("MetricsEvent::MutationCommitPlan { .. } => {}"),
        "mutation durability trace events should remain proof-facing instead of becoming global diagnostics authority",
    );
    assert!(
        !diagnostics.contains("MutationCommitPlan")
            && !diagnostics.contains("MutationCommitClass")
            && !diagnostics.contains("classify_mutation_commit_plan"),
        "diagnostics must project store capabilities, not own mutation durability classification",
    );
}

#[test]
fn journaled_diagnostics_project_registry_capabilities_without_backend_authority() {
    let diagnostics = rust_sources_under("src/db/diagnostics")
        .into_iter()
        .filter(|path| !relative_source_path(path).contains("/tests/"))
        .map(|path| {
            fs::read_to_string(&path)
                .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()))
        })
        .collect::<Vec<_>>()
        .join("\n");
    let diagnostics = strip_cfg_test_items(&diagnostics);
    let storage_report = read_source("src/db/diagnostics/storage_report.rs");
    let storage_report_compact = compact_source(&storage_report);

    assert!(
        storage_report_compact.contains("letcapabilities=store_handle.storage_capabilities();")
            && storage_report_compact
                .contains("letstorage_mode=snapshot_storage_mode(capabilities.storage_mode());")
            && storage_report.contains("DataStoreSnapshot::new(")
            && storage_report.contains("IndexStoreSnapshot::new(")
            && storage_report.contains("SchemaStoreSnapshot::new("),
        "storage diagnostics must project capabilities from registry store handles",
    );
    assert!(
        storage_report.contains("StoreRuntimeStorageMode::Journaled")
            && storage_report.contains("StoreSnapshotStorageMode::Journaled"),
        "diagnostics may render journaled storage mode as a projection",
    );

    for forbidden in [
        "StoreRuntimeStorageCapabilities::stable()",
        "StoreRuntimeStorageCapabilities::heap()",
        "StoreRuntimeStorageCapabilities::journaled()",
        "DataStoreBackend::",
        "IndexStoreBackend::",
        "SchemaStoreBackend::",
        "EntityModel",
        "IndexModel",
    ] {
        assert!(
            !diagnostics.contains(forbidden),
            "diagnostics production code must not derive authority from {forbidden}",
        );
    }
}

#[test]
fn storage_capability_policy_stays_out_of_codecs_and_commit_marker_format() {
    let checked_roots = [
        "src/db/codec",
        "src/db/data/persisted_row/codec",
        "src/db/index/key/codec",
        "src/db/commit/store",
    ];
    let checked_files = ["src/db/schema/codec.rs", "src/db/commit/marker.rs"];
    let forbidden = [
        "StoreRuntimeStorageCapabilities",
        "StoreCommitParticipation",
        "StoreRecoveryCapability",
        "MutationCommitClass",
        "storage_capabilities()",
        "commit_participation()",
        "recovery()",
    ];
    let mut sources = Vec::new();
    for root in checked_roots {
        sources.extend(rust_sources_under(root));
    }
    sources.extend(checked_files.iter().map(|path| {
        let mut absolute = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        absolute.push(path);
        absolute
    }));
    sources.sort();

    let mut violations = Vec::new();
    for path in sources {
        let relative = relative_source_path(&path);
        if relative.ends_with("/tests.rs") || relative.contains("/tests/") {
            continue;
        }
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
        let production_source = strip_cfg_test_items(&source);
        let symbols = forbidden
            .iter()
            .copied()
            .filter(|symbol| production_source.contains(symbol))
            .collect::<Vec<_>>();
        if !symbols.is_empty() {
            violations.push(format!("{relative} ({})", symbols.join(", ")));
        }
    }

    assert!(
        violations.is_empty(),
        "row/index/schema codecs and commit-marker format must stay independent of storage capability policy:\n{}",
        violations.join("\n"),
    );
}

#[test]
fn stable_and_heap_compatibility_contracts_remain_distinct_from_journaled() {
    let registry_handle = read_source("src/db/registry/handle.rs");
    let registry_tests = read_source("src/db/registry/tests.rs");
    let heap_runtime_tests = read_source("src/db/session/tests/heap_runtime.rs");
    let commit_window = read_source("src/db/executor/mutation/commit_window.rs");
    let commit_replay = read_source("src/db/commit/replay.rs");
    let commit_rebuild = read_source("src/db/commit/rebuild.rs");
    let registry_handle_compact = compact_source(&registry_handle);
    let commit_window_compact = compact_source(&commit_window);

    assert!(
        registry_handle_compact.contains("pubconstfnstable()->Self{Self{storage_mode:StoreRuntimeStorageMode::Stable,allocation_identity:StoreAllocationIdentityCapability::Present,durability:StoreDurability::Durable,recovery:StoreRecoveryCapability::StableCommitReplay,commit_participation:StoreCommitParticipation::Durable,schema_metadata:StoreSchemaMetadataCapability::DurableAcceptedHistory,relation_source:StoreRelationSourceCapability::DurableSource,relation_target:StoreRelationTargetCapability::DurableTarget,live_validation:StoreLiveValidationCapability::Supported,}}")
            && registry_handle_compact.contains("pubconstfnheap()->Self{Self{storage_mode:StoreRuntimeStorageMode::Heap,allocation_identity:StoreAllocationIdentityCapability::Absent,durability:StoreDurability::Volatile,recovery:StoreRecoveryCapability::None,commit_participation:StoreCommitParticipation::LiveOnly,schema_metadata:StoreSchemaMetadataCapability::LiveRebuiltMetadata,relation_source:StoreRelationSourceCapability::LiveSource,relation_target:StoreRelationTargetCapability::VolatileTarget,live_validation:StoreLiveValidationCapability::Supported,}}"),
        "stable and heap capability descriptors must keep their pre-journaled allocation, durability, recovery, commit, schema, relation, and validation contracts",
    );
    assert!(
        registry_handle_compact.contains("pubconstfnabsent()->Self{Self{data:None,index:None,schema:None,journal:None,}}")
            && registry_handle_compact.contains("pubconstfnnew(data:StoreAllocationIdentity,index:StoreAllocationIdentity,schema:StoreAllocationIdentity,)->Self{Self{data:Some(data),index:Some(index),schema:Some(schema),journal:None,}}")
            && registry_handle_compact.contains("StoreRuntimeStorageMode::Stable=>{self.data.is_some()&&self.index.is_some()&&self.schema.is_some()&&self.journal.is_none()}")
            && registry_handle_compact.contains("StoreRuntimeStorageMode::Heap=>{self.data.is_none()&&self.index.is_none()&&self.schema.is_none()&&self.journal.is_none()}"),
        "stable allocation identity must stay three-role durable metadata and heap allocation identity must stay explicitly absent",
    );
    assert!(
        registry_tests
            .contains("fn register_store_with_stable_allocation_identities_binds_metadata()")
            && registry_tests.contains("StoreRuntimeStorageMode::Stable")
            && registry_tests.contains("StoreRecoveryCapability::StableCommitReplay")
            && registry_tests.contains(
                "fn register_store_with_absent_allocation_identities_binds_store_handles()"
            )
            && registry_tests.contains("StoreRuntimeStorageMode::Heap")
            && registry_tests.contains("StoreCommitParticipation::LiveOnly")
            && registry_tests.contains("StoreRecoveryCapability::None"),
        "registry tests must continue proving stable metadata is present and heap metadata is absent",
    );
    assert!(
        heap_runtime_tests.contains(
            "fn heap_backed_session_reinit_loses_rows_and_indexes_but_reconciles_live_schema()"
        ) && heap_runtime_tests.contains(
            "heap rows must not be recovered from stable commit state after store reinit"
        ) && heap_runtime_tests.contains(
            "heap schema metadata is rebuilt for live validation/diagnostics, not recovered as rows"
        ) && heap_runtime_tests
            .contains("assert_eq!(heap_classes, vec![MutationCommitClass::LiveOnly]);"),
        "heap runtime tests must continue proving volatility, live-only commit classification, and rebuilt live schema metadata",
    );
    assert!(
        commit_window_compact.contains(
            "StoreRecoveryCapability::StableCommitReplay=>recovery_row_ops.push(row_op.clone())"
        ) && commit_window_compact.contains("StoreRecoveryCapability::None=>{}")
            && commit_replay.contains("StoreRecoveryCapability::None => continue")
            && commit_rebuild.contains("StoreRecoveryCapability::StableCommitReplay")
            && commit_rebuild.contains("StoreRecoveryCapability::StableBasePlusJournalReplay"),
        "commit/recovery code must keep stable row-op replay and heap no-recovery participation distinct from journaled replay",
    );
}

#[test]
fn public_docs_describe_storage_tradeoffs_without_heap_durability() {
    let readme = read_source("../../README.md");
    let sql_contract = read_source("../../docs/contracts/SQL_SUBSET.md");
    let demo_schema = read_source("../../schema/demo/rpg/src/schema/relations.rs");
    let readme_compact = compact_source(&readme);
    let sql_contract_compact = compact_source(&sql_contract);

    assert!(
        readme.contains("## Storage Modes")
            && readme.contains("`storage(stable(...))`: durable stable-memory BTrees")
            && readme.contains("`storage(heap())`: volatile Rust `BTreeMap` storage")
            && readme.contains("rows and indexes are not recovered")
            && readme.contains("`storage(journaled(...))`: journaled cached-stable storage")
            && readme.contains("marker-bound journal batches")
            && readme.contains("canonical stable data/index/schema")
            && readme.contains("BTrees")
            && readme.contains("it does not make `heap()` durable"),
        "README storage-mode docs must distinguish direct stable, volatile heap, and journaled cached-stable contracts",
    );
    assert!(
        readme_compact.contains(
            "`data_memory_id`,`index_memory_id`,`schema_memory_id`,and`journal_memory_id`"
        ) && readme.contains("the durable journal tail")
            && readme.contains("not the same contract as direct `stable(...)`"),
        "README must describe journaled memory-id roles without presenting journaled as plain stable storage",
    );
    assert!(
        sql_contract_compact.contains("`stable`isdirectdurablestable-memorystorage")
            && sql_contract_compact.contains("`heap`isvolatilelivestoragewithabsentstableallocationidentityandnorow/indexrecovery")
            && sql_contract_compact.contains("`journaled`isadurablecached-stablestore")
            && sql_contract_compact.contains("thefourthjournal-tailmemoryroleseparatelyfromthethreecanonicalstableroles"),
        "SQL contract docs must explain the storage-mode tradeoffs shown by catalog commands",
    );
    assert!(
        demo_schema.contains("storage(journaled(")
            && demo_schema.contains("data_memory_id = 104")
            && demo_schema.contains("index_memory_id = 105")
            && demo_schema.contains("schema_memory_id = 106")
            && demo_schema.contains("journal_memory_id = 107"),
        "demo RPG schema should remain an explicit four-ID journaled example",
    );
}

#[test]
fn journaled_storage_schema_build_admission_stays_out_of_runtime_backends() {
    let schema_store = read_source("../icydb-schema/src/node/store.rs");
    let schema_derive_store = read_source("../icydb-schema-derive/src/node/store.rs");
    let build_store = read_source("../icydb-build/src/db/store.rs");
    let runtime_capabilities = read_source("src/db/registry/handle.rs");
    let registry = read_source("src/db/registry/registry.rs");
    let data_store = read_source("src/db/data/store.rs");
    let index_store = read_source("src/db/index/store.rs");
    let schema_runtime_store = read_source("src/db/schema/store.rs");
    let diagnostics = read_rust_sources_under("src/db/diagnostics");
    let relation = read_rust_sources_under("src/db/relation");
    let commit = read_rust_sources_under("src/db/commit");
    let executor_mutation = read_rust_sources_under("src/db/executor/mutation");

    assert!(
        schema_store.contains("StoreStorage::Journaled")
            && schema_store.contains("StoreJournaledMemoryConfig")
            && schema_store.contains("StoreStorageCapabilities::journaled")
            && schema_derive_store.contains("ParsedStoreStorage::Journaled")
            && schema_derive_store.contains("fn parse_journaled_memory_config("),
        "0.174.1 should admit journaled storage at the schema/derive model boundary",
    );
    assert!(
        build_store.contains("journaled_store_registry_entry_tokens")
            && build_store.contains("StoreAllocationIdentities::new_journaled")
            && build_store.contains("StoreRuntimeStorageCapabilities::journaled")
            && build_store.contains("JournalTail"),
        "0.174.1 should add generated journaled wiring and four-role allocation metadata",
    );
    assert!(
        runtime_capabilities.contains("StoreRuntimeStorageMode::Journaled")
            && runtime_capabilities.contains("pub const fn journaled()")
            && runtime_capabilities.contains("StableBasePlusJournalReplay")
            && registry.contains("matches_storage_capabilities"),
        "0.174.1 should add runtime capability projection without using diagnostics as authority",
    );

    assert!(
        data_store.contains("DataStoreBackend::Journaled")
            && data_store.contains("pub fn init_journaled")
            && index_store.contains("IndexStoreBackend::Journaled")
            && index_store.contains("pub fn init_journaled")
            && schema_runtime_store.contains("SchemaStoreBackend::Journaled")
            && schema_runtime_store.contains("pub fn init_journaled"),
        "0.174.3 should admit journaled runtime store wrappers after schema/build wiring lands",
    );
    assert!(
        !relation.contains("Journaled")
            && !diagnostics.contains("DataStoreBackend::Journaled")
            && !diagnostics.contains("IndexStoreBackend::Journaled")
            && !diagnostics.contains("SchemaStoreBackend::Journaled"),
        "0.174.1 should not add relation policy or diagnostics authority branches for concrete journaled backends",
    );
    assert!(
        executor_mutation.contains("StoreRecoveryCapability::StableBasePlusJournalReplay")
            && executor_mutation.contains("commit_window_payload_for_prepared_row_ops")
            && executor_mutation.contains("append_prepared_journal_batches")
            && commit.contains("publish_marker_bound_journal_batches")
            && commit.contains("rebuild_journaled_live_projections")
            && commit.contains("JournalTailVisit"),
        "0.174.4 commit/recovery should append, repair, and replay marker-bound journal batches through the journal tail",
    );
}

#[test]
fn commit_recovery_boundary_keeps_journal_replay_marker_bound_and_folds_via_watermark() {
    let commit_root = read_source("src/db/commit/mod.rs");
    let commit_guard = read_source("src/db/commit/guard.rs");
    let commit_window = read_source("src/db/executor/mutation/commit_window.rs");
    let commit_replay = read_source("src/db/commit/replay.rs");
    let commit_recovery = read_source("src/db/commit/recovery.rs");
    let commit_rebuild = read_source("src/db/commit/rebuild.rs");
    let commit_store = read_source("src/db/commit/store/mod.rs");
    let journal_store = read_source("src/db/journal/store.rs");
    let data_store = read_source("src/db/data/store.rs");
    let index_store = read_source("src/db/index/store.rs");
    let schema_store = read_source("src/db/schema/store.rs");

    assert!(
        commit_root.contains("The `CommitMarker` fully specifies every row mutation")
            && commit_root.contains("Recovery replays row ops as recorded, not planner logic"),
        "current durable recovery authority should remain the existing commit-marker protocol",
    );
    assert!(
        commit_guard.contains("pub(crate) fn begin_commit(marker: CommitMarker)")
            && commit_guard.contains("pub(crate) fn finish_commit(")
            && commit_guard.contains("Err(_)` => marker remains persisted for recovery replay"),
        "commit visibility inventory should remain begin_commit/finish_commit marker authority",
    );
    assert!(
        commit_window.contains("fn commit_window_payload_for_prepared_row_ops")
            && commit_window.contains(
                "StoreRecoveryCapability::StableCommitReplay => recovery_row_ops.push(row_op.clone())"
            )
            && commit_window.contains("StoreRecoveryCapability::None => {}"),
        "commit markers should project durable row ops and journal batches from capability axes",
    );
    assert!(
        commit_replay.contains("handle.storage_capabilities().recovery()")
            && commit_replay.contains("StoreRecoveryCapability::None")
            && commit_replay.contains("StoreRecoveryCapability::None => continue"),
        "recovery replay should consume recovery capabilities and skip live-only stores",
    );
    assert!(
        commit_rebuild.contains("handle.storage_capabilities().recovery()")
            && commit_rebuild.contains("StoreRecoveryCapability::StableCommitReplay")
            && commit_rebuild.contains("StoreRecoveryCapability::StableBasePlusJournalReplay")
            && commit_rebuild.contains("rebuild_secondary_indexes_from_rows"),
        "startup index rebuild should consume recovery capabilities for stable and journaled durable stores",
    );
    assert!(
        commit_recovery.contains("fn publish_marker_bound_journal_batches")
            && commit_recovery.contains("fn fold_journaled_tails")
            && commit_recovery.contains("fn rebuild_journaled_live_projections")
            && commit_recovery
                .contains("store.fold_watermark()?.highest_folded_journal_sequence()")
            && commit_recovery.contains("store.persist_fold_watermark(next_watermark)")
            && commit_recovery.contains("store.clear_batches_through(highest_folded)")
            && commit_recovery.contains("JournalTailVisit::Continue")
            && commit_recovery.contains("apply_recovered_journal_put")
            && commit_recovery.contains("fold_recovered_journal_put")
            && commit_recovery.contains("fold_recovered_journal_delete")
            && commit_recovery.contains("fold_journaled_materialized_view"),
        "0.174.5 recovery should repair marker-bound journal publication, fold committed tail batches, and replay only above the fold watermark",
    );
    assert!(
        journal_store.contains("pub(in crate::db) struct FoldWatermark")
            && journal_store.contains("FOLD_WATERMARK_CONTROL_SEQUENCE")
            && journal_store.contains("pub(in crate::db) fn persist_fold_watermark")
            && journal_store.contains("pub(in crate::db) fn clear_batches_through")
            && journal_store.contains(
                "Values above sequence `0` are complete encoded `JournalBatch` envelopes"
            )
            && journal_store.contains("so real")
            && journal_store.contains("journal batches start at sequence `1`"),
        "0.174.5 journal tail storage should own the fold-watermark replay boundary without a fifth memory id",
    );
    assert!(
        data_store.contains("fn fold_recovered_journal_put")
            && data_store.contains("fn fold_recovered_journal_delete")
            && schema_store.contains("fn fold_persisted_snapshot")
            && index_store.contains("fn fold_journaled_materialized_view"),
        "0.174.5 should fold row/schema records and derived index materializations through store-wrapper APIs",
    );
    assert!(
        commit_replay.contains(
            "journaled row-op recovery is unsupported; journaled recovery must use marker-bound journal batches",
        ),
        "journaled recovery must stay on marker-bound journal batches instead of treating journaled rows as stable marker row ops",
    );
    assert!(
        commit_store.contains("struct RawCommitMarker(Vec<u8>)")
            && commit_store.contains("StableCell<RawCommitMarker"),
        "0.174 recovery should keep the existing stable-cell commit-marker store as publication authority",
    );

    let executor_mutation = read_rust_sources_under("src/db/executor/mutation");
    let relation = read_rust_sources_under("src/db/relation");
    assert!(
        !executor_mutation.contains("fold_journal") && !relation.contains("fold_journal"),
        "fold policy should stay in recovery/store-wrapper boundaries, not executor or relation policy roots",
    );
}

#[test]
fn journaled_canonical_btree_mutation_stays_fold_recovery_only() {
    let data_store = strip_cfg_test_items(&read_source("src/db/data/store.rs"));
    let index_store = strip_cfg_test_items(&read_source("src/db/index/store.rs"));
    let schema_store = strip_cfg_test_items(&read_source("src/db/schema/store.rs"));
    let data_compact = compact_source(&data_store);
    let index_compact = compact_source(&index_store);
    let schema_compact = compact_source(&schema_store);

    assert!(
        data_compact.contains("DataStoreBackend::Journaled{live,tombstones,..}=>{tombstones.remove(&key);live.insert(key,row);previous_journaled}")
            && data_compact.contains("DataStoreBackend::Journaled{live,tombstones,..}=>{live.remove(key);tombstones.insert(key.clone());previous_journaled}")
            && !data_compact.contains("DataStoreBackend::Journaled{canonical,..}=>canonical.insert")
            && !data_compact.contains("DataStoreBackend::Journaled{canonical,..}=>canonical.remove"),
        "normal journaled data writes must update only the live/tombstone projection",
    );
    assert!(
        data_compact.contains("fnfold_recovered_journal_put(")
            && data_compact.contains("Ok(canonical.insert(key,row))")
            && data_compact.contains("fnfold_recovered_journal_delete(")
            && data_compact.contains("Ok(canonical.remove(key))"),
        "journaled data canonical mutation should stay behind explicit fold helpers",
    );

    assert!(
        index_compact.contains("IndexStoreBackend::Journaled{live,tombstones,..}=>{tombstones.remove(&key);live.insert(key,entry);previous_journaled}")
            && index_compact.contains("IndexStoreBackend::Journaled{live,tombstones,..}=>{live.remove(key);tombstones.insert(key.clone());previous_journaled}")
            && index_compact.contains("fnfold_journaled_materialized_view(")
            && index_compact.contains("canonical.clear_new();")
            && index_compact.contains("canonical.insert(key,value);"),
        "journaled index writes should update live projection while canonical materialization stays fold-only",
    );

    assert!(
        schema_compact.contains("SchemaStoreBackend::Journaled{live,tombstones,..}=>{tombstones.remove(&key);live.insert(key,snapshot);previous_journaled}")
            && schema_compact.contains("fnfold_persisted_snapshot(")
            && schema_compact.contains("canonical.insert(key,raw_snapshot);"),
        "journaled schema writes should update live projection while canonical snapshot publication stays fold-only",
    );

    for (label, source) in [
        ("executor", read_rust_sources_under("src/db/executor")),
        ("query", read_rust_sources_under("src/db/query")),
        ("session", read_rust_sources_under("src/db/session")),
        ("relation", read_rust_sources_under("src/db/relation")),
    ] {
        for forbidden in [
            "fold_recovered_journal_put",
            "fold_recovered_journal_delete",
            "fold_persisted_snapshot",
            "fold_journaled_materialized_view",
        ] {
            assert!(
                !source.contains(forbidden),
                "{label} policy roots must not call journaled canonical fold helper {forbidden}",
            );
        }
    }
}

#[test]
fn journaled_ordered_overlay_traversal_stays_streaming_and_fold_only() {
    let data_store = strip_cfg_test_items(&read_source("src/db/data/store.rs"));
    let index_store = strip_cfg_test_items(&read_source("src/db/index/store.rs"));
    let schema_store = strip_cfg_test_items(&read_source("src/db/schema/store.rs"));
    let session_sql_cache = strip_cfg_test_items(&read_source("src/db/session/sql/cache.rs"));
    let executor = read_rust_sources_under("src/db/executor");
    let query = read_rust_sources_under("src/db/query");
    let session_query = read_rust_sources_under("src/db/session/query");
    let session_sql = read_rust_sources_under("src/db/session/sql");

    assert!(
        data_store.contains("visit_ordered_overlay(")
            && !data_store.contains("journaled_entries_snapshot"),
        "journaled data traversal must keep streaming through ordered_overlay without a materialized snapshot helper",
    );

    let index_store_compact = compact_source(&index_store);
    assert!(
        index_store.contains("visit_ordered_overlay(")
            && index_store.contains("fn journaled_entries_snapshot_for_fold(")
            && index_store_compact
                .contains("letentries=Self::journaled_entries_snapshot_for_fold(&self.backend);")
            && !index_store.contains("fn journaled_entries_snapshot("),
        "journaled index traversal must stream through ordered_overlay and keep snapshot materialization named fold-only",
    );

    assert!(
        schema_store.contains("visit_ordered_overlay(")
            && !schema_store.contains("journaled_snapshots"),
        "journaled schema traversal must keep latest-snapshot reads streaming through ordered_overlay without materialized snapshot maps",
    );

    let session_sql_compact = compact_source(&session_sql);
    assert!(
        session_sql_compact.contains("compile_sql_query_with_execution_context::<E>(sql)")
            && session_sql_compact
                .contains("execute_compiled_sql_context_with_phase_attribution::<E>(&compiled)")
            && session_sql.contains("SqlCompiledCommandExecutionContext"),
        "cold SQL query execution must carry accepted schema context from compile into plan lookup instead of rebuilding it inside the same query call",
    );
    assert!(
        session_sql_cache.contains("accepted_schema_catalog_context_for_query::<E>()")
            && !session_sql_cache.contains("ensure_accepted_schema_snapshot::<E>()"),
        "SQL query cache-key construction must load accepted runtime schema directly instead of rerunning generated reconciliation on every query call",
    );

    for (label, source) in [
        ("executor", executor),
        ("query", query),
        ("session query", session_query),
        ("session sql", session_sql),
    ] {
        assert!(
            !source.contains("journaled_entries_snapshot")
                && !source.contains("journaled_entries_snapshot_for_fold")
                && !source.contains("journaled_snapshots")
                && !source.contains("latest_raw_snapshots_by_entity"),
            "{label} hot paths must not call journaled snapshot materialization helpers",
        );
    }
}

#[test]
fn journaled_read_hot_paths_stay_off_recovery_fold_diagnostics_and_authority_rebuilds() {
    let read_hot_paths = [
        (
            "executor stream",
            read_rust_sources_under("src/db/executor/stream"),
        ),
        (
            "executor scan",
            read_rust_sources_under("src/db/executor/scan"),
        ),
        (
            "executor pipeline",
            read_rust_sources_under("src/db/executor/pipeline"),
        ),
        (
            "executor runtime context",
            read_rust_sources_under("src/db/executor/runtime_context"),
        ),
        ("query", read_rust_sources_under("src/db/query")),
        (
            "session query",
            read_rust_sources_under("src/db/session/query"),
        ),
        ("session sql", read_rust_sources_under("src/db/session/sql")),
    ];

    for (label, source) in read_hot_paths {
        for forbidden in [
            "storage_report",
            "StoreDiagnostics",
            "fold_journaled",
            "recover_journaled",
            "read_committed_batches_after",
            "journaled_entries_snapshot",
            "journaled_entries_snapshot_for_fold",
            "journaled_snapshots",
            "latest_raw_snapshots_by_entity",
        ] {
            assert!(
                !source.contains(forbidden),
                "{label} read hot path must not call {forbidden}",
            );
        }
    }

    let session_sql_compile_cache =
        compact_source(&read_source("src/db/session/sql/compile_cache.rs"));
    let cache_hit = session_sql_compile_cache
        .find("ifletSome(compiled)=cached")
        .expect("SQL compile cache should retain an explicit cache-hit branch");
    let accepted_authority = session_sql_compile_cache
        .find("letauthority=catalog.accepted_entity_authority_for::<E>()")
        .expect("SQL compile cache miss should construct accepted authority from cached schema");
    assert!(
        cache_hit < accepted_authority,
        "SQL compiled-command cache hits must return before accepted authority construction",
    );
    assert!(
        session_sql_compile_cache.contains("returnOk((compiled,SqlCacheAttribution::sql_compiled_command_cache_hit(),attribution.finish(),None,));"),
        "SQL compiled-command cache hits should carry no rebuilt accepted authority",
    );
}

#[test]
fn accepted_catalog_context_reaches_filterless_query_plan_cache_before_schema_projection() {
    let session_mod = read_source("src/db/session/mod.rs");
    let session_query_cache = read_source("src/db/session/query/cache.rs");
    let session_query_cache_compact = compact_source(&session_query_cache);
    let schema_store = read_source("src/db/schema/store.rs");
    let schema_store_compact = compact_source(&schema_store);
    let cached_shared_query_plan_for_entity = session_query_cache_compact
        .split("pub(incrate::db::session)fncached_shared_query_plan_for_entity<E>")
        .nth(1)
        .expect("session query cache should keep typed shared-plan cache helper");
    assert!(
        session_mod.contains("struct AcceptedSchemaCatalogContext")
            && session_mod.contains("fn accepted_schema_info_for<E>")
            && session_mod.contains("fn accepted_schema_catalog_context_for_query<E>")
            && session_query_cache.contains("accepted_schema_catalog_context_for_query::<E>()")
            && read_source("src/db/session/sql/cache.rs")
                .contains("accepted_schema_catalog_context_for_query::<E>()"),
        "query paths should name accepted catalog context instead of passing raw snapshot/fingerprint pairs everywhere",
    );

    let catalog_context = cached_shared_query_plan_for_entity
        .find("accepted_catalog_snapshot_selection_for_query::<E>()")
        .expect("typed query cache should create accepted catalog identity selection");
    let cache_hit = cached_shared_query_plan_for_entity
        .find("try_cached_filterless_query_plan_for_entity_path(")
        .expect("typed query cache should try eligible cache hits by catalog identity");
    let authority_projection = cached_shared_query_plan_for_entity
        .find("accepted_schema_catalog_context_from_selection::<E>")
        .expect("cache misses should decode the selected accepted snapshot");
    assert!(
        catalog_context < cache_hit && cache_hit < authority_projection,
        "eligible filterless query-plan cache hits must return before accepted snapshot decode, authority, and SchemaInfo projection",
    );
    assert!(
        cached_shared_query_plan_for_entity.contains("letidentity=selection.identity();")
            && cached_shared_query_plan_for_entity.contains(
                "debug_assert_eq!(identity.fingerprint_method_version(),crate::db::schema::accepted_schema_cache_fingerprint_method_version(),);",
            )
            && cached_shared_query_plan_for_entity.contains(
                "letschema_identity=SchemaCacheIdentity::new(identity.accepted_schema_version(),identity.fingerprint_method_version(),identity.accepted_schema_fingerprint(),);",
            )
            && cached_shared_query_plan_for_entity.contains(
                "try_cached_filterless_query_plan_for_entity_path(E::PATH,schema_identity,visibility,query.structural(),)",
            ),
        "eligible filterless query-plan cache hits must use method-qualified accepted catalog identity/header facts directly",
    );
    let identity_hit_prefix = &cached_shared_query_plan_for_entity[..cache_hit];
    for forbidden in [
        "SchemaInfo",
        "AcceptedRowLayoutRuntimeContract",
        "decode_verified",
        "ensure_accepted_schema_snapshot",
        "reconcile_runtime_schemas",
        "schema_admission_rejection",
        "SchemaAdmissionIdentityComparison",
        "accepted_schema_admission_fingerprint",
        "latest_raw_snapshots_by_entity",
        "storage_report",
        "integrity_report",
        "show_entities",
        "show_stores",
    ] {
        assert!(
            !identity_hit_prefix.contains(forbidden),
            "eligible filterless query-plan cache-hit prefix must not call {forbidden}",
        );
    }
    assert!(
        !cached_shared_query_plan_for_entity.contains("accepted_entity_authority::<E>()")
            && cached_shared_query_plan_for_entity.contains(
                "cached_shared_query_plan_for_accepted_authority_with_schema_fingerprint_and_visibility(",
            ),
        "typed query cache should use the catalog snapshot/fingerprint and reuse resolved visibility on misses",
    );
    assert_latest_catalog_identity_is_header_only(&schema_store, &schema_store_compact);
    let with_query_visible_indexes = session_query_cache_compact
        .split("pub(incrate::db::session)fnwith_query_visible_indexes<E,T>")
        .nth(1)
        .expect("session query cache should keep visible-index helper");
    assert!(
        with_query_visible_indexes.contains("accepted_schema_catalog_context_for_query::<E>()")
            && with_query_visible_indexes.contains("accepted_schema_info_for::<E>()")
            && !with_query_visible_indexes.contains("ensure_accepted_schema_snapshot::<E>()"),
        "visible-index diagnostics should derive accepted SchemaInfo from catalog context, not reload a raw snapshot",
    );
}

fn assert_latest_catalog_identity_is_header_only(schema_store: &str, schema_store_compact: &str) {
    let latest_catalog_identity = schema_store_compact
        .split("pub(incrate::db)fnlatest_catalog_identity(")
        .nth(1)
        .expect("schema store should expose latest catalog identity")
        .split("///Returnrawschema-storefootprintfactsforoneentity.")
        .next()
        .expect("latest catalog identity section should be bounded");

    assert!(
        schema_store.contains("pub(in crate::db) fn latest_catalog_identity(")
            && schema_store_compact.contains("self.latest_raw_snapshot_entry(entity)")
            && schema_store_compact.contains("raw_snapshot.accepted_schema_fingerprint()?")
            && !latest_catalog_identity.contains("latest_raw_snapshots_by_entity")
            && !latest_catalog_identity.contains("decode_persisted_snapshot"),
        "accepted catalog identity lookup must use one-entity raw snapshot selection without full decode or all-entity metadata",
    );
}

#[test]
fn journal_format_boundary_preserves_row_index_schema_codecs() {
    let commit_marker = read_source("src/db/commit/marker.rs");
    let schema_codec = read_source("src/db/schema/codec.rs");

    assert!(
        commit_marker
            .contains("pub(in crate::db) const COMMIT_MARKER_FORMAT_VERSION_CURRENT: u8 = 2")
            && commit_marker.contains("CommitIndexOp")
            && commit_marker.contains("Not persisted in commit markers"),
        "0.174.2 should hard-cut commit markers for embedded journal batches without making index records durable marker truth",
    );
    assert!(
        schema_codec.contains("const SCHEMA_SNAPSHOT_CODEC_VERSION: u32 = 6"),
        "0.172 must not change the accepted schema snapshot codec version",
    );

    let checked_roots = [
        "src/db/codec",
        "src/db/data/persisted_row/codec",
        "src/db/index/key/codec",
    ];
    let checked_files = ["src/db/schema/codec.rs"];
    let forbidden = [
        "JournalRecord",
        "JournalRecordEnvelope",
        "JournalCodec",
        "JournalReplay",
        "JournalFold",
        "Journaled",
        "CommittedJournal",
        "journal_record",
        "journal_format",
        "journal_replay",
        "replay_journal",
        "fold_journal",
    ];
    let mut sources = Vec::new();
    for root in checked_roots {
        sources.extend(rust_sources_under(root));
    }
    sources.extend(checked_files.iter().map(|path| {
        let mut absolute = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
        absolute.push(path);
        absolute
    }));
    sources.sort();

    let mut violations = Vec::new();
    for path in sources {
        let relative = relative_source_path(&path);
        if relative.ends_with("/tests.rs") || relative.contains("/tests/") {
            continue;
        }
        let source = fs::read_to_string(&path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", path.display()));
        let production_source = strip_cfg_test_items(&source);
        let symbols = forbidden
            .iter()
            .copied()
            .filter(|symbol| production_source.contains(symbol))
            .collect::<Vec<_>>();
        if !symbols.is_empty() {
            violations.push(format!("{relative} ({})", symbols.join(", ")));
        }
    }

    assert!(
        violations.is_empty(),
        "journaled codec work must stay out of existing row, index, and schema codec surfaces:\n{}",
        violations.join("\n"),
    );
}

#[test]
fn journaled_runtime_store_and_codec_slice_is_bounded_and_marker_bound() {
    let journal_codec = read_source("src/db/journal/codec.rs");
    let journal_store = read_source("src/db/journal/store.rs");
    let commit_marker = read_source("src/db/commit/marker.rs");
    let build_store = read_source("../icydb-build/src/db/store.rs");
    let registry = read_source("src/db/registry/registry.rs");
    let registry_handle = read_source("src/db/registry/handle.rs");
    let data_store = read_source("src/db/data/store.rs");
    let index_store = read_source("src/db/index/store.rs");
    let schema_store = read_source("src/db/schema/store.rs");
    let changelog = read_source("../../docs/changelog/0.174.md");

    assert!(
        journal_codec.contains("JOURNAL_BATCH_FORMAT_VERSION_CURRENT")
            && journal_codec.contains("MAX_JOURNAL_BATCH_BYTES")
            && journal_codec.contains("pub(in crate::db) struct JournalBatch")
            && journal_codec.contains("pub(in crate::db) enum JournalRecord")
            && journal_codec.contains("decode_journal_batch")
            && journal_codec.contains("serialize_incompatible_persisted_format"),
        "0.174.2 should introduce bounded/fallible journal batch codec",
    );
    assert!(
        journal_store.contains(
            "StableBTreeMap<JournalSequence, RawJournalBatch, VirtualMemory<DefaultMemoryImpl>>",
        ) && journal_store.contains("pub(in crate::db) fn append_batch")
            && journal_store.contains("pub(in crate::db) fn visit_batches_after")
            && journal_store.contains("sequence gap")
            && journal_store.contains("duplicate batch id"),
        "0.174.2 should add ordered append/read journal-tail storage",
    );
    assert!(
        commit_marker.contains("journal_batches: Vec<JournalBatch>")
            && commit_marker.contains("COMMIT_MARKER_FORMAT_VERSION_CURRENT: u8 = 2")
            && commit_marker.contains("commit marker journal batch count")
            && commit_marker.contains("batch.commit_marker_id() != marker.id"),
        "0.174.2 should bind embedded journal batches to the existing commit marker id",
    );
    assert!(
        build_store.contains("JournalTailStore :: init")
            && build_store.contains("register_journaled_store")
            && registry.contains("pub fn register_journaled_store")
            && registry_handle.contains("pub const fn journal_tail_store"),
        "0.174.2 should wire generated journal-tail storage through the registry handle",
    );
    for (label, source) in [
        ("data", data_store.as_str()),
        ("index", index_store.as_str()),
        ("schema", schema_store.as_str()),
    ] {
        assert!(
            source.contains("Backend::Journaled") && source.contains("init_journaled"),
            "{label} store wrappers should admit journaled live projections in the wrapper slice",
        );
    }
    assert!(
        changelog.contains("## 0.174.2")
            && changelog.contains("bounded/fallible journal batch codec")
            && changelog.contains("journal-tail storage")
            && changelog.contains("commit-marker-bound"),
        "0.174.2 changelog should record codec, tail storage, and marker-bound publication work",
    );
}
