//! Persisted schema relation integrity checks.

use crate::db::schema::{
    AcceptedCompositeCatalog, AcceptedEnumCatalog, AcceptedFieldKind,
    AcceptedRelationValueContract, PersistedFieldSnapshot, PersistedRelationEdgeSnapshot,
    PersistedRelationPathStepSnapshot, PersistedRelationSourceSnapshot, SchemaRowLayout,
    classify_accepted_field_kind, composite_catalog::AcceptedCompositeShape,
    enum_catalog::AcceptedEnumVariantBody,
};

// Build the first deterministic accepted-relation integrity diagnostic.
// Relation edges are owned by the source entity snapshot; target compatibility
// is checked during schema reconciliation where both entity snapshots are
// available.
pub(in crate::db::schema) fn schema_snapshot_relation_integrity_detail(
    _subject: &str,
    row_layout: &SchemaRowLayout,
    fields: &[PersistedFieldSnapshot],
    relations: &[PersistedRelationEdgeSnapshot],
) -> Option<()> {
    for (relation_offset, relation) in relations.iter().enumerate() {
        if relation.name().is_empty() {
            return Some(());
        }

        if relation.target_path().is_empty() {
            return Some(());
        }

        for other in &relations[relation_offset + 1..] {
            if relation.id() == other.id() {
                return Some(());
            }

            if relation.name() == other.name() {
                return Some(());
            }
        }

        match relation.source() {
            PersistedRelationSourceSnapshot::Direct { field_ids } => {
                if field_ids.is_empty() {
                    return Some(());
                }
                for (field_offset, field_id) in field_ids.iter().enumerate() {
                    if field_ids[..field_offset].contains(field_id) {
                        return Some(());
                    }
                    let Some(field) = accepted_root_field(row_layout, fields, *field_id) else {
                        return Some(());
                    };
                    if classify_accepted_field_kind(field.kind()).is_composite() {
                        return Some(());
                    }
                }
            }
            PersistedRelationSourceSnapshot::Nested {
                root_field_id,
                steps,
            } => {
                if accepted_root_field(row_layout, fields, *root_field_id).is_none()
                    || steps.is_empty()
                    || steps.len() > icydb_schema::MAX_RELATION_PATH_STEPS
                {
                    return Some(());
                }
            }
        }
    }

    None
}

fn accepted_root_field<'a>(
    row_layout: &SchemaRowLayout,
    fields: &'a [PersistedFieldSnapshot],
    field_id: crate::db::schema::FieldId,
) -> Option<&'a PersistedFieldSnapshot> {
    let row_layout_slot = row_layout.slot_for_field(field_id)?;
    let field = fields.iter().find(|field| field.id() == field_id)?;
    (field.slot() == row_layout_slot).then_some(field)
}

pub(in crate::db::schema) fn accepted_relation_sources_match_catalogs<'a>(
    fields: &[PersistedFieldSnapshot],
    mut relations: impl Iterator<Item = &'a PersistedRelationEdgeSnapshot>,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> bool {
    relations.all(|relation| {
        matches!(
            relation.source(),
            PersistedRelationSourceSnapshot::Direct { .. }
        ) || accepted_relation_source_terminal(
            fields,
            relation.source(),
            enum_catalog,
            composite_catalog,
        )
        .is_some()
    })
}

/// Derive one nested relation terminal solely from its accepted root, stable
/// path identities, and accepted value catalogs.
pub(in crate::db) fn accepted_relation_source_terminal(
    fields: &[PersistedFieldSnapshot],
    source: &PersistedRelationSourceSnapshot,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Option<AcceptedRelationValueContract> {
    let PersistedRelationSourceSnapshot::Nested {
        root_field_id,
        steps,
    } = source
    else {
        return None;
    };
    if steps.is_empty() || steps.len() > icydb_schema::MAX_RELATION_PATH_STEPS {
        return None;
    }
    let root = fields.iter().find(|field| field.id() == *root_field_id)?;
    accepted_relation_path_terminal(
        AcceptedRelationValueContract::new(root.kind().clone(), root.nullable()),
        steps,
        enum_catalog,
        composite_catalog,
    )
}

pub(in crate::db) fn accepted_relation_path_terminal(
    mut current: AcceptedRelationValueContract,
    steps: &[PersistedRelationPathStepSnapshot],
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Option<AcceptedRelationValueContract> {
    if steps.is_empty() || steps.len() > icydb_schema::MAX_RELATION_PATH_STEPS {
        return None;
    }
    for step in steps {
        current = relation_step_output(current, step, enum_catalog, composite_catalog)?;
    }
    (!current.nullable()
        && current.kind().has_valid_local_shape()
        && classify_accepted_field_kind(current.kind()).is_relation_key_eligible())
    .then_some(current)
}

fn relation_step_output(
    current: AcceptedRelationValueContract,
    step: &PersistedRelationPathStepSnapshot,
    enum_catalog: &AcceptedEnumCatalog,
    composite_catalog: &AcceptedCompositeCatalog,
) -> Option<AcceptedRelationValueContract> {
    match step {
        PersistedRelationPathStepSnapshot::OptionalSome => current
            .nullable()
            .then(|| AcceptedRelationValueContract::new(current.kind().clone(), false)),
        PersistedRelationPathStepSnapshot::EnterNamed => {
            if current.nullable() {
                return None;
            }
            match current.kind() {
                AcceptedFieldKind::Enum { type_id } => enum_catalog
                    .enum_type(*type_id)
                    .map(|_| AcceptedRelationValueContract::new(current.kind().clone(), false)),
                AcceptedFieldKind::Composite { type_id } => {
                    match composite_catalog.composite_type(*type_id)?.shape() {
                        AcceptedCompositeShape::Record(_) => Some(
                            AcceptedRelationValueContract::new(current.kind().clone(), false),
                        ),
                        AcceptedCompositeShape::Newtype(inner) => {
                            Some(AcceptedRelationValueContract::new(
                                inner.kind().clone(),
                                inner.nullable(),
                            ))
                        }
                        AcceptedCompositeShape::Tuple(_) => None,
                    }
                }
                AcceptedFieldKind::List(_)
                | AcceptedFieldKind::Set(_)
                | AcceptedFieldKind::Map { .. } => Some(AcceptedRelationValueContract::new(
                    current.kind().clone(),
                    false,
                )),
                _ => None,
            }
        }
        PersistedRelationPathStepSnapshot::ListItems
        | PersistedRelationPathStepSnapshot::SetItems
        | PersistedRelationPathStepSnapshot::MapValues => {
            relation_collection_step_output(current, step)
        }
        PersistedRelationPathStepSnapshot::RecordMember {
            composite_type_id,
            member_id,
        } => {
            if current.nullable()
                || current.kind()
                    != &(AcceptedFieldKind::Composite {
                        type_id: *composite_type_id,
                    })
            {
                return None;
            }
            let AcceptedCompositeShape::Record(members) = composite_catalog
                .composite_type(*composite_type_id)?
                .shape()
            else {
                return None;
            };
            let contract = members
                .iter()
                .find(|member| member.id() == *member_id)?
                .contract();
            Some(AcceptedRelationValueContract::new(
                contract.kind().clone(),
                contract.nullable(),
            ))
        }
        PersistedRelationPathStepSnapshot::EnumVariantPayload {
            enum_type_id,
            variant_id,
        } => {
            if current.nullable()
                || current.kind()
                    != &(AcceptedFieldKind::Enum {
                        type_id: *enum_type_id,
                    })
            {
                return None;
            }
            let variant = enum_catalog
                .enum_type(*enum_type_id)?
                .variant(*variant_id)?;
            let AcceptedEnumVariantBody::Payload { contract } = variant.body() else {
                return None;
            };
            Some(AcceptedRelationValueContract::new(
                contract.kind().clone(),
                false,
            ))
        }
    }
}

fn relation_collection_step_output(
    current: AcceptedRelationValueContract,
    step: &PersistedRelationPathStepSnapshot,
) -> Option<AcceptedRelationValueContract> {
    if current.nullable() {
        return None;
    }
    let output = match (step, current.kind()) {
        (PersistedRelationPathStepSnapshot::ListItems, AcceptedFieldKind::List(item))
        | (PersistedRelationPathStepSnapshot::SetItems, AcceptedFieldKind::Set(item)) => {
            item.as_ref()
        }
        (PersistedRelationPathStepSnapshot::MapValues, AcceptedFieldKind::Map { value, .. }) => {
            value.as_ref()
        }
        _ => return None,
    };
    Some(AcceptedRelationValueContract::new(output.clone(), false))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::schema::{
        FieldId, FieldStorageDecode, LeafCodec, RelationId, ScalarCodec, SchemaFieldSlot,
        SchemaInsertDefault, empty_accepted_enum_catalog_for_tests,
    };

    #[test]
    fn nested_relation_terminal_is_derived_from_catalog_authority() {
        let fields = vec![PersistedFieldSnapshot::new_initial(
            FieldId::new(1),
            "target_id".to_string(),
            SchemaFieldSlot::new(0),
            AcceptedFieldKind::Nat64,
            Vec::new(),
            true,
            SchemaInsertDefault::None,
            FieldStorageDecode::ByKind,
            LeafCodec::Scalar(ScalarCodec::Nat64),
        )];
        let layout = SchemaRowLayout::initial(vec![(FieldId::new(1), SchemaFieldSlot::new(0))]);
        let valid = PersistedRelationEdgeSnapshot::new_nested(
            RelationId::new(1).expect("test relation identity should be non-zero"),
            "target_id".to_string(),
            "schema::integrity::Target".to_string(),
            FieldId::new(1),
            vec![PersistedRelationPathStepSnapshot::OptionalSome],
        );
        assert_eq!(
            schema_snapshot_relation_integrity_detail(
                "test",
                &layout,
                &fields,
                std::slice::from_ref(&valid),
            ),
            None,
        );
        let enum_catalog = empty_accepted_enum_catalog_for_tests();
        let composite_catalog = AcceptedCompositeCatalog::empty();
        let terminal = accepted_relation_source_terminal(
            &fields,
            valid.source(),
            &enum_catalog,
            &composite_catalog,
        )
        .expect("the optional step should derive its scalar terminal");
        assert_eq!(terminal.kind(), &AcceptedFieldKind::Nat64);
        assert!(!terminal.nullable());

        let malformed = PersistedRelationEdgeSnapshot::new_nested(
            RelationId::new(1).expect("test relation identity should be non-zero"),
            "target_id".to_string(),
            "schema::integrity::Target".to_string(),
            FieldId::new(1),
            vec![PersistedRelationPathStepSnapshot::EnterNamed],
        );
        assert!(
            accepted_relation_source_terminal(
                &fields,
                malformed.source(),
                &enum_catalog,
                &composite_catalog,
            )
            .is_none(),
        );
    }
}
