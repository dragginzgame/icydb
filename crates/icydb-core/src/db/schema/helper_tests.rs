//! Test-only helpers for the parent module.

use super::*;

// Keep optional scalar record members under the real catalog admission owner
// so reader/session qualification need not introduce a newtype wrapper.
pub(in crate::db) fn build_record_composite_catalog_for_tests(
    record_path: String,
    member_name: String,
    leaf_kind: AcceptedFieldKind,
    nullable: bool,
    enum_catalog: &AcceptedEnumCatalog,
) -> (AcceptedCompositeCatalog, CompositeTypeId) {
    build_record_members_catalog_for_tests(
        record_path,
        vec![(member_name, leaf_kind, nullable)],
        enum_catalog,
    )
}

// Multi-member reader fixtures use the same catalog admission as single-member records.
pub(in crate::db) fn build_record_members_catalog_for_tests(
    record_path: String,
    members: Vec<(String, AcceptedFieldKind, bool)>,
    enum_catalog: &AcceptedEnumCatalog,
) -> (AcceptedCompositeCatalog, CompositeTypeId) {
    let record_type = CompositeTypeId::new(1).unwrap();
    let fields = members
        .into_iter()
        .enumerate()
        .map(|(index, (name, kind, nullable))| {
            composite_catalog::AcceptedCompositeField::new(
                CompositeFieldId::new(u32::try_from(index + 1).unwrap()).unwrap(),
                name,
                composite_catalog::AcceptedCompositeElement::new(kind, nullable),
            )
        })
        .collect();
    let catalog = AcceptedCompositeCatalog::from_initial_definitions(
        std::collections::BTreeMap::from([(
            record_type,
            (
                record_path,
                composite_catalog::AcceptedCompositeShape::Record(fields),
            ),
        )]),
        enum_catalog,
    )
    .expect("test record catalog should build");
    (catalog, record_type)
}

pub(in crate::db) fn build_record_newtype_composite_catalog_for_tests(
    record_path: String,
    member_name: String,
    newtype_path: String,
    leaf_kind: AcceptedFieldKind,
    enum_catalog: &AcceptedEnumCatalog,
) -> Result<
    (
        AcceptedCompositeCatalog,
        CompositeTypeId,
        CompositeTypeId,
        CompositeFieldId,
    ),
    crate::error::InternalError,
> {
    let record_type =
        CompositeTypeId::new(1).expect("test record type identity should be non-zero");
    let newtype_type = CompositeTypeId::new(2).expect("test newtype identity should be non-zero");
    let member_id = CompositeFieldId::new(1).expect("test member identity should be non-zero");
    let catalog = AcceptedCompositeCatalog::from_initial_definitions(
        std::collections::BTreeMap::from([
            (
                record_type,
                (
                    record_path,
                    composite_catalog::AcceptedCompositeShape::Record(vec![
                        composite_catalog::AcceptedCompositeField::new(
                            member_id,
                            member_name,
                            composite_catalog::AcceptedCompositeElement::new(
                                AcceptedFieldKind::Composite {
                                    type_id: newtype_type,
                                },
                                false,
                            ),
                        ),
                    ]),
                ),
            ),
            (
                newtype_type,
                (
                    newtype_path,
                    composite_catalog::AcceptedCompositeShape::Newtype(
                        composite_catalog::AcceptedCompositeElement::new(leaf_kind, false),
                    ),
                ),
            ),
        ]),
        enum_catalog,
    )
    .map_err(|_| crate::error::InternalError::store_invariant())?;
    Ok((catalog, record_type, newtype_type, member_id))
}

pub(in crate::db) fn validate_accepted_enum_catalog_format_for_tests(
    bytes: &[u8],
) -> Result<(), crate::error::InternalError> {
    enum_catalog::decode_accepted_enum_catalog(bytes).map(drop)
}

pub(in crate::db) fn validate_accepted_schema_bundle_format_for_tests(
    bytes: &[u8],
) -> Result<(), crate::error::InternalError> {
    enum_catalog::decode_accepted_schema_revision_bundle(bytes).map(drop)
}
