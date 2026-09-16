//! Root and nested expansion limits apply before candidate publication and reload.

use crate::{
    db::schema::{
        AcceptedFieldKind, AcceptedSourceBindingCatalog, FieldId, FieldStorageDecode, LeafCodec,
        PersistedFieldSnapshot, PersistedNestedLeafSnapshot, PersistedSchemaSnapshot, ScalarCodec,
        SchemaFieldSlot, SchemaInsertDefault, SchemaRowLayout, SchemaVersion,
        composite_catalog::{
            AcceptedCompositeCatalog, AcceptedCompositeElement, AcceptedCompositeField,
            AcceptedCompositeShape, CompositeFieldId, CompositeTypeId,
        },
        empty_accepted_enum_catalog_for_tests,
        enum_catalog::publication::{
            ACCEPTED_SCHEMA_BUNDLE_CODEC_VERSION, ACCEPTED_SCHEMA_BUNDLE_MAGIC,
            AcceptedSchemaRevision, AcceptedSchemaRevisionBundle, BundleWriter,
            CandidateSchemaRevision, decode_accepted_schema_revision_bundle,
            encode_accepted_composite_catalog, encode_accepted_enum_catalog,
            encode_accepted_source_bindings, encode_persisted_schema_snapshot,
        },
    },
    error::ErrorClass,
    types::EntityTag,
};
use std::collections::BTreeMap;

// A zero count is a direct field; a positive count creates that many record leaves.
fn projection_bundle(levels: u32, field_leaves: &[usize]) -> AcceptedSchemaRevisionBundle {
    let enums = empty_accepted_enum_catalog_for_tests();
    let reference = |id| AcceptedFieldKind::Composite {
        type_id: CompositeTypeId::new(id).unwrap(),
    };
    let mut definitions = BTreeMap::new();
    for id in 1..=levels + 1 {
        let kind = if id == 1 {
            AcceptedFieldKind::Nat64
        } else {
            AcceptedFieldKind::Map {
                key: Box::new(reference(id - 1)),
                value: Box::new(reference(id - 1)),
            }
        };
        definitions.insert(
            CompositeTypeId::new(id).unwrap(),
            (
                format!("test::Type{id}"),
                AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(kind, false)),
            ),
        );
    }
    let expanded = reference(levels + 1);
    let mut query_fields = Vec::new();
    for (position, &leaf_count) in field_leaves.iter().enumerate() {
        if leaf_count == 0 {
            query_fields.push((expanded.clone(), Vec::new()));
            continue;
        }
        let record_id = levels + 2 + u32::try_from(position).unwrap();
        let mut members = Vec::new();
        let mut leaves = Vec::new();
        for leaf in 0..leaf_count {
            let name = format!("leaf{leaf}");
            members.push(AcceptedCompositeField::new(
                CompositeFieldId::new(u32::try_from(leaf + 1).unwrap()).unwrap(),
                name.clone(),
                AcceptedCompositeElement::new(expanded.clone(), false),
            ));
            leaves.push(PersistedNestedLeafSnapshot::new(
                vec![name],
                expanded.clone(),
                false,
            ));
        }
        definitions.insert(
            CompositeTypeId::new(record_id).unwrap(),
            (
                format!("test::Record{position}"),
                AcceptedCompositeShape::Record(members),
            ),
        );
        query_fields.push((reference(record_id), leaves));
    }
    let snapshot = projection_snapshot(query_fields);
    AcceptedSchemaRevisionBundle {
        revision: AcceptedSchemaRevision::INITIAL,
        store_path: "test::Store".into(),
        enum_catalog: enums.clone(),
        composite_catalog: AcceptedCompositeCatalog::from_initial_definitions(definitions, &enums)
            .unwrap(),
        source_bindings: AcceptedSourceBindingCatalog::initial(
            BTreeMap::from([(
                icydb_schema::EntitySourceKey::try_new("test::Projection").unwrap(),
                EntityTag::new(1),
            )]),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
            BTreeMap::new(),
        ),
        entity_snapshots: BTreeMap::from([(EntityTag::new(1), snapshot)]),
    }
}

fn projection_snapshot(
    query_fields: Vec<(AcceptedFieldKind, Vec<PersistedNestedLeafSnapshot>)>,
) -> PersistedSchemaSnapshot {
    let mut fields = vec![PersistedFieldSnapshot::new_initial(
        FieldId::new(1),
        "id".into(),
        SchemaFieldSlot::new(0),
        AcceptedFieldKind::Nat64,
        Vec::new(),
        false,
        SchemaInsertDefault::None,
        FieldStorageDecode::ByKind,
        LeafCodec::Scalar(ScalarCodec::Nat64),
    )];
    for (position, (kind, leaves)) in query_fields.into_iter().enumerate() {
        let (storage, codec) = if matches!(kind, AcceptedFieldKind::Nat64) {
            (
                FieldStorageDecode::ByKind,
                LeafCodec::Scalar(ScalarCodec::Nat64),
            )
        } else {
            (FieldStorageDecode::CatalogValue, LeafCodec::Structural)
        };
        fields.push(PersistedFieldSnapshot::new_initial(
            FieldId::new(u32::try_from(position + 2).unwrap()),
            format!("value{position}"),
            SchemaFieldSlot::new(u16::try_from(position + 1).unwrap()),
            kind,
            leaves,
            false,
            SchemaInsertDefault::None,
            storage,
            codec,
        ));
    }
    PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "test::Projection".into(),
        "Projection".into(),
        FieldId::new(1),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    )
}

// Components are individually valid; deliberately bypass bundle admission to
// prove persisted reconstruction rejects expansion before runtime projection.
fn component_bytes(bundle: &AcceptedSchemaRevisionBundle) -> Vec<u8> {
    let mut writer = BundleWriter::new();
    writer.push_bytes(ACCEPTED_SCHEMA_BUNDLE_MAGIC);
    writer.push_u16(ACCEPTED_SCHEMA_BUNDLE_CODEC_VERSION);
    writer.push_u64(bundle.revision().get());
    writer.push_string(bundle.store_path()).unwrap();
    for bytes in [
        encode_accepted_enum_catalog(&bundle.enum_catalog).unwrap(),
        encode_accepted_composite_catalog(&bundle.composite_catalog, &bundle.enum_catalog).unwrap(),
        encode_accepted_source_bindings(
            &bundle.source_bindings,
            &bundle.enum_catalog,
            &bundle.composite_catalog,
            &bundle.entity_snapshots,
        )
        .unwrap(),
    ] {
        writer.push_len_prefixed_bytes(&bytes).unwrap();
    }
    writer.push_len(bundle.entity_snapshots.len()).unwrap();
    for (tag, snapshot) in &bundle.entity_snapshots {
        writer.push_u64(tag.value());
        writer
            .push_len_prefixed_bytes(&encode_persisted_schema_snapshot(snapshot).unwrap())
            .unwrap();
    }
    writer.finish().unwrap()
}

#[test]
fn candidate_and_reload_admit_small_shared_types_and_reject_expansion() {
    for nested in [false, true] {
        assert_projection_admission(projection_bundle(8, &[usize::from(nested)]), true);
        assert_projection_admission(projection_bundle(30, &[usize::from(nested)]), false);
    }
}

fn assert_projection_admission(bundle: AcceptedSchemaRevisionBundle, admitted: bool) {
    let bytes = component_bytes(&bundle);
    let candidate = CandidateSchemaRevision::new(bundle.clone());
    let decoded = decode_accepted_schema_revision_bundle(&bytes);
    if admitted {
        let candidate = candidate.unwrap();
        assert_eq!(candidate.encoded_bundle(), bytes);
        assert_eq!(decoded.unwrap(), bundle);
        assert_eq!(
            CandidateSchemaRevision::from_encoded(bytes, candidate.encoded_root().to_vec())
                .unwrap()
                .bundle(),
            &bundle,
        );
    } else {
        assert_eq!(candidate.unwrap_err().class(), ErrorClass::Unsupported);
        assert_eq!(decoded.unwrap_err().class(), ErrorClass::Corruption);
    }
}

#[test]
fn entity_projection_allowance_accumulates_direct_nested_and_mixed_fields() {
    // Each depth-9 reference visits 2046 nodes. Two fit with the primary key
    // and record wrappers; three do not, even though every individual kind fits.
    for (fields, admitted) in [
        (vec![0, 0], true),
        (vec![0, 0, 0], false),
        (vec![2], true),
        (vec![3], false),
        (vec![1, 1], true),
        (vec![1, 1, 1], false),
        (vec![0, 1], true),
        (vec![0, 2], false),
        (vec![2, 0], false),
    ] {
        assert_projection_admission(projection_bundle(9, &fields), admitted);
    }
    // 4094 expanded visits, one opaque record and one primary key hit the
    // exact entity ceiling; a second direct field must not get a fresh allowance.
    assert_projection_admission(projection_bundle(10, &[1]), true);
    assert_projection_admission(projection_bundle(10, &[1, 0]), false);
}

#[test]
fn ordinary_wide_scalar_schema_stays_admitted() {
    let mut bundle = projection_bundle(0, &[]);
    // 255 not-null fields plus the primary-key constraint fill the existing
    // constraint-count limit without approaching query expansion's allowance.
    let snapshot = projection_snapshot(vec![(AcceptedFieldKind::Nat64, Vec::new()); 254]);
    bundle.entity_snapshots.insert(EntityTag::new(1), snapshot);
    assert_projection_admission(bundle, true);
}

#[test]
fn projection_allowance_resets_between_entities_not_between_fields() {
    let mut bundle = projection_bundle(10, &[1]);
    let first = &bundle.entity_snapshots[&EntityTag::new(1)];
    let second = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "test::Second".into(),
        "Second".into(),
        FieldId::new(1),
        first.row_layout().clone(),
        first.fields().to_vec(),
    );
    bundle.entity_snapshots.insert(EntityTag::new(2), second);
    bundle.source_bindings = AcceptedSourceBindingCatalog::initial(
        BTreeMap::from([
            (
                icydb_schema::EntitySourceKey::try_new("test::Projection").unwrap(),
                EntityTag::new(1),
            ),
            (
                icydb_schema::EntitySourceKey::try_new("test::Second").unwrap(),
                EntityTag::new(2),
            ),
        ]),
        BTreeMap::new(),
        BTreeMap::new(),
        BTreeMap::new(),
        BTreeMap::new(),
    );
    // Each entity exactly fits. Their sum exceeds one allowance, intentionally:
    // this contract is not a database-wide construction or instruction bound.
    assert_projection_admission(bundle, true);
}
