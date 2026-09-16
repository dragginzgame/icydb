use super::{
    AcceptedCompositeCatalog, AcceptedCompositeElement, AcceptedCompositeShape,
    AcceptedCompositeType, AcceptedFieldKind, CompositeCodec, CompositeTypeId,
};
use std::collections::BTreeMap;

fn composite(id: u32) -> AcceptedFieldKind {
    AcceptedFieldKind::Composite {
        type_id: CompositeTypeId::new(id).unwrap(),
    }
}

fn newtype(kind: AcceptedFieldKind) -> AcceptedCompositeShape {
    AcceptedCompositeShape::Newtype(AcceptedCompositeElement::new(kind, false))
}

// Build graphs directly so missing definitions and wrapper cycles reach the
// resolver's rejection boundary without requiring catalog admission to accept them.
fn catalog(shapes: Vec<AcceptedCompositeShape>) -> AcceptedCompositeCatalog {
    let by_id = shapes
        .into_iter()
        .enumerate()
        .map(|(index, shape)| {
            let id = CompositeTypeId::new(u32::try_from(index + 1).unwrap()).unwrap();
            (
                id,
                AcceptedCompositeType {
                    path: format!("tests::Type{}", id.get()),
                    codec: CompositeCodec::StructuralV1,
                    shape,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let id_by_path = by_id
        .iter()
        .map(|(id, ty)| (ty.path.clone(), *id))
        .collect();
    AcceptedCompositeCatalog { by_id, id_by_path }
}

#[test]
fn resolution_borrows_direct_input_and_catalog_collection_payloads() {
    let payload = AcceptedFieldKind::Map {
        key: Box::new(AcceptedFieldKind::Text { max_len: Some(64) }),
        value: Box::new(AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64))),
    };
    let catalog = catalog(vec![newtype(composite(2)), newtype(payload.clone())]);
    for direct in [AcceptedFieldKind::Nat64, payload] {
        assert!(std::ptr::eq(
            catalog.resolve_newtype_value_kind(&direct).unwrap(),
            &raw const direct,
        ));
    }
    let root = composite(1);
    let resolved = catalog.resolve_newtype_value_kind(&root).unwrap();
    let AcceptedCompositeShape::Newtype(inner) = catalog
        .composite_type(CompositeTypeId::new(2).unwrap())
        .unwrap()
        .shape()
    else {
        panic!("fixture must contain a newtype");
    };
    assert!(std::ptr::eq(resolved, inner.kind()));
    assert!(matches!(resolved, AcceptedFieldKind::Map { .. }));
}

#[test]
fn resolution_rejects_structural_missing_and_cyclic_wrappers() {
    let catalog = catalog(vec![
        newtype(composite(2)),
        newtype(composite(1)),
        AcceptedCompositeShape::Record(Vec::new()),
        AcceptedCompositeShape::Tuple(Vec::new()),
        newtype(composite(3)),
        newtype(composite(99)),
        newtype(composite(7)),
    ]);
    for id in [1, 2, 3, 4, 5, 6, 7, 99] {
        assert!(catalog.resolve_newtype_value_kind(&composite(id)).is_none());
    }
}

#[test]
fn query_projection_preserves_whole_tree_fallback() {
    use crate::db::schema::query_field_kind_from_persisted_kind;

    let catalog = catalog(vec![
        newtype(AcceptedFieldKind::Text { max_len: Some(64) }),
        AcceptedCompositeShape::Record(Vec::new()),
    ]);
    let input = AcceptedFieldKind::Map {
        key: Box::new(composite(1)),
        value: Box::new(composite(2)),
    };
    // The unresolved structural child leaves the entire original shape intact,
    // including the otherwise resolvable newtype sibling.
    assert_eq!(
        query_field_kind_from_persisted_kind(&input, &catalog),
        input
    );
}

fn assert_query_type(
    catalog: &AcceptedCompositeCatalog,
    kind: &AcceptedFieldKind,
    expected: crate::db::schema::FieldType,
) {
    use crate::db::schema::{
        field_type_from_persisted_kind, query_field_kind_from_persisted_kind,
        query_field_type_from_persisted_kind,
    };

    let direct = query_field_type_from_persisted_kind(kind, catalog);
    assert_eq!(direct, expected);
    // Both maintained output representations must agree on query meaning.
    assert_eq!(
        direct,
        field_type_from_persisted_kind(&query_field_kind_from_persisted_kind(kind, catalog)),
    );
}

#[test]
fn direct_query_types_preserve_newtypes_collections_and_whole_tree_fallback() {
    use crate::db::schema::FieldType;
    use icydb_schema::ScalarKind;

    let catalog = catalog(vec![
        newtype(AcceptedFieldKind::Text { max_len: Some(64) }),
        newtype(AcceptedFieldKind::List(Box::new(composite(1)))),
        AcceptedCompositeShape::Record(Vec::new()),
        AcceptedCompositeShape::Tuple(Vec::new()),
        newtype(composite(5)),
        newtype(AcceptedFieldKind::List(Box::new(composite(3)))),
        newtype(composite(99)),
        newtype(composite(1)),
    ]);
    let text = FieldType::Scalar(ScalarKind::Text);
    for (kind, expected) in [
        (AcceptedFieldKind::Nat8, FieldType::Scalar(ScalarKind::Nat)),
        (AcceptedFieldKind::U256, FieldType::Scalar(ScalarKind::U256)),
        (composite(1), text.clone()),
        (composite(2), FieldType::List(Box::new(text.clone()))),
        (
            AcceptedFieldKind::Set(Box::new(composite(8))),
            FieldType::Set(Box::new(text.clone())),
        ),
        (
            AcceptedFieldKind::Map {
                key: Box::new(composite(1)),
                value: Box::new(composite(8)),
            },
            FieldType::Map {
                key: Box::new(text.clone()),
                value: Box::new(text),
            },
        ),
        (
            AcceptedFieldKind::Map {
                key: Box::new(composite(1)),
                value: Box::new(composite(3)),
            },
            FieldType::Map {
                key: Box::new(FieldType::Composite),
                value: Box::new(FieldType::Composite),
            },
        ),
        (
            AcceptedFieldKind::List(Box::new(composite(3))),
            FieldType::List(Box::new(FieldType::Composite)),
        ),
    ] {
        assert_query_type(&catalog, &kind, expected);
    }
    // In particular, the newtype around a list of records must stay Composite,
    // rather than leaking a partially unwrapped, queryable list.
    for id in [3, 4, 5, 6, 7, 99] {
        assert_query_type(&catalog, &composite(id), FieldType::Composite);
    }
}

#[test]
fn direct_query_type_drops_relation_metadata_only_from_the_type_output() {
    use crate::{
        db::schema::{FieldType, query_field_kind_from_persisted_kind},
        types::EntityTag,
    };
    use icydb_schema::ScalarKind;

    let catalog = catalog(vec![newtype(AcceptedFieldKind::Text { max_len: Some(64) })]);
    let relation = |key| AcceptedFieldKind::Relation {
        target_path: "tests::Target".into(),
        target_entity_name: "Target".into(),
        target_entity_tag: EntityTag::new(7),
        target_store_path: "tests::Store".into(),
        key_kind: Box::new(key),
    };
    let input = relation(composite(1));
    assert_query_type(&catalog, &input, FieldType::Scalar(ScalarKind::Text));
    assert_eq!(
        query_field_kind_from_persisted_kind(&input, &catalog),
        relation(AcceptedFieldKind::Text { max_len: Some(64) }),
    );
}

#[test]
fn direct_query_type_preserves_the_exact_shared_depth_boundary() {
    use crate::db::schema::{FieldType, MAX_ACCEPTED_RECURSIVE_DEPTH};
    use icydb_schema::ScalarKind;

    let catalog = catalog(vec![newtype(AcceptedFieldKind::Text { max_len: Some(64) })]);
    for lists in [
        0,
        MAX_ACCEPTED_RECURSIVE_DEPTH - 2,
        MAX_ACCEPTED_RECURSIVE_DEPTH - 1,
        MAX_ACCEPTED_RECURSIVE_DEPTH,
    ] {
        let mut kind = composite(1);
        let mut expected = if lists < MAX_ACCEPTED_RECURSIVE_DEPTH - 1 {
            FieldType::Scalar(ScalarKind::Text)
        } else {
            FieldType::Composite
        };
        for _ in 0..lists {
            kind = AcceptedFieldKind::List(Box::new(kind));
            expected = FieldType::List(Box::new(expected));
        }
        assert_query_type(&catalog, &kind, expected);
    }
}
