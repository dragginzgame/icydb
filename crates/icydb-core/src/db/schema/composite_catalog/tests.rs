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

#[test]
fn query_projection_preserves_reused_newtype_members() {
    use super::{decode_accepted_composite_catalog, encode_accepted_composite_catalog};
    use crate::db::schema::{FieldType, empty_accepted_enum_catalog_for_tests};
    use icydb_schema::ScalarKind;

    // A compact, acyclic catalog can describe a much larger expanded tree.
    // Keep this fixture small: depth admission alone is not an expansion bound.
    let mut shapes = vec![newtype(AcceptedFieldKind::Nat64)];
    let mut expected = FieldType::Scalar(ScalarKind::Nat);
    let enums = empty_accepted_enum_catalog_for_tests();
    for level in 0..=8 {
        let accepted = catalog(shapes.clone());
        let bytes = encode_accepted_composite_catalog(&accepted, &enums).unwrap();
        let decoded = decode_accepted_composite_catalog(&bytes, &enums).unwrap();
        assert_eq!(decoded, accepted);
        assert_query_type(&decoded, &composite(level + 1), expected.clone());

        if level == 8 {
            assert_eq!(shapes.len(), 9);
            assert!(bytes.len() < 1024);
            let mut pending = vec![&expected];
            let mut nodes = 0;
            while let Some(ty) = pending.pop() {
                nodes += 1;
                match ty {
                    FieldType::Map { key, value } => {
                        pending.push(key);
                        pending.push(value);
                    }
                    FieldType::Scalar(ScalarKind::Nat) => {}
                    _ => panic!("fixture must contain only maps and natural numbers"),
                }
            }
            assert_eq!(nodes, 511);
            break;
        }

        shapes.push(newtype(AcceptedFieldKind::Map {
            key: Box::new(composite(level + 1)),
            value: Box::new(composite(level + 1)),
        }));
        expected = FieldType::Map {
            key: Box::new(expected.clone()),
            value: Box::new(expected),
        };
    }
}

fn assert_query_type(
    catalog: &AcceptedCompositeCatalog,
    kind: &AcceptedFieldKind,
    expected: crate::db::schema::FieldType,
) {
    use crate::db::schema::{
        field_type_from_persisted_kind, query_field_is_queryable,
        query_field_kind_from_persisted_kind, query_field_type_from_persisted_kind,
        validate_query_projections,
    };

    validate_query_projections([kind], catalog).unwrap();
    let direct = query_field_type_from_persisted_kind(kind, catalog);
    assert_eq!(direct, expected);
    assert_eq!(
        query_field_is_queryable(kind, catalog),
        expected.is_queryable()
    );
    // Both maintained output representations must agree on query meaning.
    assert_eq!(
        direct,
        field_type_from_persisted_kind(&query_field_kind_from_persisted_kind(kind, catalog)),
    );
}

#[test]
fn projection_admission_bounds_expanded_nodes_and_relation_text() {
    use crate::{
        db::schema::{FieldType, MAX_SCHEMA_SNAPSHOT_BYTES, validate_query_projections},
        error::ErrorClass,
        types::EntityTag,
    };

    // The policy is cross-target, not size_of-based. Its fixed node allowance
    // covers the inline representations constructed by the maintained outputs.
    let node_bytes = 128;
    assert!(size_of::<AcceptedFieldKind>() <= node_bytes);
    assert!(size_of::<FieldType>() <= node_bytes);
    let empty = catalog(Vec::new());
    let mut tree = AcceptedFieldKind::Nat64;
    for _ in 0..11 {
        tree = AcceptedFieldKind::Map {
            key: Box::new(tree.clone()),
            value: Box::new(tree),
        };
    }
    // 4095 map/scalar nodes plus the outer list exactly fill the allowance.
    let boundary = AcceptedFieldKind::List(Box::new(tree));
    assert!(validate_query_projections([&boundary], &empty).is_ok());
    let error = validate_query_projections([&AcceptedFieldKind::Set(Box::new(boundary))], &empty)
        .unwrap_err();
    assert_eq!(error.class(), ErrorClass::Unsupported);

    // Both relation and key are visited; all three copied labels count.
    let text_bytes = MAX_SCHEMA_SNAPSHOT_BYTES as usize - 2 * node_bytes;
    for extra in [0, 1] {
        let relation = AcceptedFieldKind::Relation {
            target_path: "p".repeat(text_bytes - 2 + extra),
            target_entity_name: "E".into(),
            target_entity_tag: EntityTag::new(1),
            target_store_path: "S".into(),
            key_kind: Box::new(AcceptedFieldKind::Nat64),
        };
        let admitted = validate_query_projections([&relation], &empty);
        if extra == 0 {
            admitted.unwrap();
        } else {
            assert_eq!(admitted.unwrap_err().class(), ErrorClass::Unsupported);
        }
    }
}

#[test]
fn projection_admission_rejects_expansion_before_semantic_fallback() {
    use crate::{db::schema::validate_query_projections, error::ErrorClass};

    let mut shapes = vec![newtype(AcceptedFieldKind::Nat64)];
    for id in 1..=30 {
        shapes.push(newtype(AcceptedFieldKind::Map {
            key: Box::new(composite(id)),
            value: Box::new(composite(id)),
        }));
    }
    shapes.push(AcceptedCompositeShape::Record(Vec::new()));
    let catalog = catalog(shapes);
    let huge = composite(31);
    let record = composite(32);

    // Never construct this expanded tree: it would have over a billion nodes.
    for kind in [
        huge.clone(),
        AcceptedFieldKind::Map {
            key: Box::new(huge.clone()),
            value: Box::new(record.clone()),
        },
    ] {
        assert_eq!(
            validate_query_projections([&kind], &catalog)
                .unwrap_err()
                .class(),
            ErrorClass::Unsupported,
        );
    }
    // An earlier semantic fallback does not visit the later branch in either
    // admission or construction. The original compact root remains unchanged.
    let fallback = AcceptedFieldKind::Map {
        key: Box::new(record),
        value: Box::new(huge),
    };
    validate_query_projections([&fallback], &catalog).unwrap();
    assert_eq!(
        crate::db::schema::query_field_kind_from_persisted_kind(&fallback, &catalog),
        fallback,
    );
}

#[test]
fn projection_admission_shares_exact_allowance_and_continues_after_fallback() {
    use crate::{db::schema::validate_query_projections, error::ErrorClass};

    let catalog = catalog(vec![AcceptedCompositeShape::Record(Vec::new())]);
    let scalar = AcceptedFieldKind::Nat64;
    let opaque = composite(1);
    // 4096 visits exactly fill 512 KiB. A semantic fallback still consumes its
    // visit and must not stop admission of the remaining fields in the entity.
    for first in [&scalar, &opaque] {
        for count in [4095, 4096, 4097] {
            let fields = std::iter::once(first).chain(std::iter::repeat_n(&scalar, count - 1));
            let result = validate_query_projections(fields, &catalog);
            if count <= 4096 {
                result.unwrap();
            } else {
                assert_eq!(result.unwrap_err().class(), ErrorClass::Unsupported);
            }
        }
    }
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

#[test]
fn queryability_preserves_relation_and_newtype_whole_tree_fallback() {
    use crate::{db::schema::FieldType, types::EntityTag};
    use icydb_schema::ScalarKind;

    let relation = |key| AcceptedFieldKind::Relation {
        target_path: "tests::Target".into(),
        target_entity_name: "Target".into(),
        target_entity_tag: EntityTag::new(7),
        target_store_path: "tests::Store".into(),
        key_kind: Box::new(key),
    };
    let list_of_records = relation(AcceptedFieldKind::List(Box::new(composite(1))));
    let catalog = catalog(vec![
        AcceptedCompositeShape::Record(Vec::new()),
        newtype(list_of_records.clone()),
        newtype(AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64))),
    ]);

    // An explicit outer list stays queryable on fallback, but an unresolved
    // newtype around that same list must not leak a partially projected shape.
    assert_query_type(
        &catalog,
        &list_of_records,
        FieldType::List(Box::new(FieldType::Composite)),
    );
    assert_query_type(&catalog, &composite(2), FieldType::Composite);
    assert_query_type(&catalog, &relation(composite(2)), FieldType::Composite);
    assert_query_type(
        &catalog,
        &relation(composite(3)),
        FieldType::List(Box::new(FieldType::Scalar(ScalarKind::Nat))),
    );
}
