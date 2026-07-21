use super::*;
use crate::model::field::{CompositeElementModel, CompositeFieldModel};

static ALPHA_FIELDS: [CompositeFieldModel; 2] = [
    CompositeFieldModel::generated("zeta", FieldKind::Nat64, false),
    CompositeFieldModel::generated("alpha", FieldKind::Text { max_len: Some(32) }, true),
];
static ALPHA_SHAPE: CompositeShapeModel = CompositeShapeModel::Record(&ALPHA_FIELDS);
static ALPHA_KIND: FieldKind = FieldKind::Composite {
    path: "tests::Alpha",
    codec: CompositeCodec::StructuralV1,
    shape: &ALPHA_SHAPE,
};
static BETA_FIELDS: [CompositeFieldModel; 2] = [
    CompositeFieldModel::generated("alpha", FieldKind::Text { max_len: Some(32) }, true),
    CompositeFieldModel::generated("zeta", FieldKind::Nat64, false),
];
static BETA_SHAPE: CompositeShapeModel = CompositeShapeModel::Record(&BETA_FIELDS);
static BETA_KIND: FieldKind = FieldKind::Composite {
    path: "tests::Beta",
    codec: CompositeCodec::StructuralV1,
    shape: &BETA_SHAPE,
};
static TUPLE_ELEMENTS: [CompositeElementModel; 2] = [
    CompositeElementModel::generated(ALPHA_KIND, false),
    CompositeElementModel::generated(FieldKind::Bool, true),
];
static TUPLE_SHAPE: CompositeShapeModel = CompositeShapeModel::Tuple(&TUPLE_ELEMENTS);
static TUPLE_KIND: FieldKind = FieldKind::Composite {
    path: "tests::Tuple",
    codec: CompositeCodec::StructuralV1,
    shape: &TUPLE_SHAPE,
};
static NEWTYPE_SHAPE: CompositeShapeModel =
    CompositeShapeModel::Newtype(CompositeElementModel::generated(FieldKind::Nat64, false));
static NEWTYPE_KIND: FieldKind = FieldKind::Composite {
    path: "tests::Identifier",
    codec: CompositeCodec::StructuralV1,
    shape: &NEWTYPE_SHAPE,
};
static CHANGED_ALPHA_FIELDS: [CompositeFieldModel; 2] = [
    CompositeFieldModel::generated("alpha", FieldKind::Text { max_len: Some(64) }, true),
    CompositeFieldModel::generated("zeta", FieldKind::Nat64, false),
];
static CHANGED_ALPHA_SHAPE: CompositeShapeModel =
    CompositeShapeModel::Record(&CHANGED_ALPHA_FIELDS);
static CHANGED_ALPHA_KIND: FieldKind = FieldKind::Composite {
    path: "tests::Alpha",
    codec: CompositeCodec::StructuralV1,
    shape: &CHANGED_ALPHA_SHAPE,
};
static AARDVARK_SHAPE: CompositeShapeModel =
    CompositeShapeModel::Newtype(CompositeElementModel::generated(FieldKind::Bool, false));
static AARDVARK_KIND: FieldKind = FieldKind::Composite {
    path: "tests::Aardvark",
    codec: CompositeCodec::StructuralV1,
    shape: &AARDVARK_SHAPE,
};

fn catalogs_for(kinds: &[FieldKind]) -> (AcceptedEnumCatalog, AcceptedCompositeCatalog) {
    build_initial_accepted_catalogs_from_kinds_for_tests(kinds)
        .expect("exact test catalogs should build")
}

#[test]
fn accepted_composite_catalog_is_nominal_canonical_and_deterministic() {
    let (enum_catalog, first) = catalogs_for(&[TUPLE_KIND, BETA_KIND, NEWTYPE_KIND, ALPHA_KIND]);
    let (_, second) = catalogs_for(&[ALPHA_KIND, NEWTYPE_KIND, BETA_KIND, TUPLE_KIND]);

    assert_eq!(first, second);
    assert!(first.validate(&enum_catalog));
    assert_eq!(
        first.type_id("tests::Alpha").map(CompositeTypeId::get),
        Some(1)
    );
    assert_eq!(
        first.type_id("tests::Beta").map(CompositeTypeId::get),
        Some(2)
    );
    assert_ne!(first.type_id("tests::Alpha"), first.type_id("tests::Beta"));

    let alpha = first
        .type_id("tests::Alpha")
        .and_then(|type_id| first.composite_type(type_id))
        .expect("alpha definition should exist");
    let AcceptedCompositeShape::Record(fields) = alpha.shape() else {
        panic!("alpha should remain a record");
    };
    assert_eq!(fields[0].name(), "alpha");
    assert!(fields[0].contract().nullable());
    assert_eq!(fields[1].name(), "zeta");
    assert!(!fields[1].contract().nullable());
}

#[test]
fn accepted_composite_catalog_current_wire_round_trips_exact_shapes() {
    let (enum_catalog, catalog) = catalogs_for(&[TUPLE_KIND, NEWTYPE_KIND]);
    let bytes = encode_accepted_composite_catalog(&catalog, &enum_catalog)
        .expect("accepted composite catalog should encode");
    let decoded = decode_accepted_composite_catalog(&bytes, &enum_catalog)
        .expect("current accepted composite catalog should decode");

    assert_eq!(decoded, catalog);
}

#[test]
fn accepted_composite_catalog_rejects_generated_shape_drift_at_same_nominal_path() {
    let (enum_catalog, catalog) = catalogs_for(&[ALPHA_KIND]);
    let type_id = catalog
        .type_id("tests::Alpha")
        .expect("alpha type identity should exist");

    assert!(!catalog.matches_generated_composite(
        &enum_catalog,
        type_id,
        "tests::Alpha",
        CompositeCodec::StructuralV1,
        &CHANGED_ALPHA_SHAPE,
    ));
}

#[test]
fn accepted_composite_catalog_reconciliation_retains_matching_contracts_and_additions() {
    let (_, accepted) = catalogs_for(&[ALPHA_KIND]);
    let (_, candidate) = catalogs_for(&[ALPHA_KIND, BETA_KIND]);

    let reconciled = reconcile_composite_catalog_candidate(&accepted, candidate.clone())
        .expect("matching accepted composite contracts should reconcile");

    assert_eq!(reconciled, candidate);
}

#[test]
fn accepted_composite_catalog_reconciliation_rejects_shape_drift() {
    let (_, accepted) = catalogs_for(&[ALPHA_KIND]);
    let (_, candidate) = catalogs_for(&[CHANGED_ALPHA_KIND]);

    assert_eq!(
        reconcile_composite_catalog_candidate(&accepted, candidate),
        Err(CompositeCatalogBuildError::ExistingTypeContractChanged {
            path: "tests::Alpha".to_string(),
        }),
    );
}

#[test]
fn accepted_composite_catalog_reconciliation_rejects_identity_drift() {
    let (_, accepted) = catalogs_for(&[ALPHA_KIND]);
    let (_, candidate) = catalogs_for(&[AARDVARK_KIND, ALPHA_KIND]);

    assert_eq!(
        reconcile_composite_catalog_candidate(&accepted, candidate),
        Err(CompositeCatalogBuildError::ExistingTypeIdentityChanged {
            path: "tests::Alpha".to_string(),
        }),
    );
}

fn nested_list_kind(mut kind: FieldKind, depth: usize) -> FieldKind {
    for _ in 0..depth {
        kind = FieldKind::List(Box::leak(Box::new(kind)));
    }
    kind
}

#[test]
fn accepted_composite_catalog_uses_the_shared_recursive_depth_boundary() {
    // The record itself occupies one recursive level beyond its enclosing
    // collections, and its scalar fields occupy the final admitted level.
    let allowed = nested_list_kind(ALPHA_KIND, MAX_ACCEPTED_RECURSIVE_DEPTH - 2);
    let excessive = nested_list_kind(ALPHA_KIND, MAX_ACCEPTED_RECURSIVE_DEPTH - 1);

    assert!(build_initial_accepted_catalogs_from_kinds_for_tests(&[allowed]).is_ok());
    assert!(build_initial_accepted_catalogs_from_kinds_for_tests(&[excessive]).is_err());
}

#[test]
fn accepted_composite_catalog_rejects_recursive_decoded_graphs() {
    let enum_catalog =
        super::super::enum_catalog::build_initial_accepted_enum_catalog_from_kinds_for_tests(&[])
            .expect("empty enum catalog should build");
    let type_id = CompositeTypeId::new(1).expect("one is non-zero");
    let recursive = AcceptedCompositeCatalog {
        by_id: BTreeMap::from([(
            type_id,
            AcceptedCompositeType {
                path: "tests::Recursive".to_string(),
                codec: CompositeCodec::StructuralV1,
                shape: AcceptedCompositeShape::Newtype(AcceptedCompositeElement {
                    kind: AcceptedFieldKind::Composite { type_id },
                    nullable: false,
                }),
            },
        )]),
        id_by_path: BTreeMap::from([("tests::Recursive".to_string(), type_id)]),
    };

    assert!(!recursive.validate(&enum_catalog));
}
