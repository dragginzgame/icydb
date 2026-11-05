use super::*;
use crate::{
    MAX_SCHEMA_MIGRATION_PLAN_BYTES, decode_schema_migration_plan, encode_schema_migration_plan,
};

fn entity(value: &str) -> EntitySourceKey {
    EntitySourceKey::try_new(value).expect("entity source should admit")
}

fn field(value: &str) -> FieldSourceKey {
    FieldSourceKey::try_new(value).expect("field source should admit")
}

fn transition(entity_name: &str, from_name: &str) -> EntityMigration {
    EntityMigration::try_new(
        entity(entity_name),
        DeclaredEntityVersion::try_new(1).expect("version should admit"),
        Some(entity(from_name)),
        vec![SchemaMigrationRename::Field {
            from: field("old_value"),
            to: field("value"),
        }],
        Vec::new(),
    )
    .expect("transition should admit")
}

#[test]
fn declared_entity_version_is_strictly_positive() {
    assert_eq!(
        DeclaredEntityVersion::try_new(0),
        Err(SchemaContractError::InvalidEntityVersion),
    );
    assert_eq!(
        DeclaredEntityVersion::try_new(1)
            .expect("version one should admit")
            .get(),
        1,
    );
}

#[test]
fn plan_canonicalization_is_declaration_order_independent() {
    let account = transition("Account", "User");
    let article = transition("Article", "Post");
    let first = SchemaMigrationPlan::try_new(vec![account.clone(), article.clone()])
        .expect("plan should admit");
    let reverse =
        SchemaMigrationPlan::try_new(vec![article, account]).expect("reverse plan should admit");

    assert_eq!(first, reverse);
    assert_eq!(first.digest(), reverse.digest());
    let encoded = encode_schema_migration_plan(&first).expect("plan should encode");
    assert_eq!(
        decode_schema_migration_plan(&encoded).expect("plan should decode"),
        first,
    );
}

#[test]
fn every_current_migration_operation_roundtrips_exactly() {
    let transition = EntityMigration::try_new(
        entity("Account"),
        DeclaredEntityVersion::try_new(3).expect("version should admit"),
        Some(entity("User")),
        vec![
            SchemaMigrationRename::Field {
                from: field("old_field"),
                to: field("new_field"),
            },
            SchemaMigrationRename::NamedType {
                from: TypeSourceKey::try_new("OldType").expect("type should admit"),
                to: TypeSourceKey::try_new("NewType").expect("type should admit"),
            },
            SchemaMigrationRename::EnumVariant {
                named_type: TypeSourceKey::try_new("OldEnum").expect("type should admit"),
                from: TypeSourceKey::try_new("OldVariant").expect("variant should admit"),
                to: TypeSourceKey::try_new("NewVariant").expect("variant should admit"),
            },
            SchemaMigrationRename::RecordField {
                named_type: TypeSourceKey::try_new("OldRecord").expect("type should admit"),
                from: field("old_member"),
                to: field("new_member"),
            },
            SchemaMigrationRename::Relation {
                from: RelationSourceKey::try_new("old_relation").expect("relation should admit"),
                to: RelationSourceKey::try_new("new_relation").expect("relation should admit"),
            },
            SchemaMigrationRename::Constraint {
                from: ConstraintSourceKey::try_new("old_constraint")
                    .expect("constraint should admit"),
                to: ConstraintSourceKey::try_new("new_constraint")
                    .expect("constraint should admit"),
            },
            SchemaMigrationRename::Rule {
                named_type: TypeSourceKey::try_new("OldRuleOwner").expect("type should admit"),
                from: RuleSourceKey::try_new("old_rule").expect("rule should admit"),
                to: RuleSourceKey::try_new("new_rule").expect("rule should admit"),
            },
        ],
        vec![
            SchemaMigrationTransform::Fill {
                to: field("filled"),
                literal: ScalarLiteral::Nat(1),
            },
            SchemaMigrationTransform::Copy {
                from: field("copy_source"),
                to: field("copied"),
            },
            SchemaMigrationTransform::CheckedCast {
                from: field("cast_source"),
                to: field("casted"),
                target: ScalarType::Nat64,
            },
            SchemaMigrationTransform::Coalesce {
                from: field("nullable_source"),
                to: field("coalesced"),
                literal: ScalarLiteral::Nat(0),
            },
        ],
    )
    .expect("transition should admit");
    let plan = SchemaMigrationPlan::try_new(vec![transition]).expect("plan should admit");
    let encoded = encode_schema_migration_plan(&plan).expect("plan should encode");

    assert_eq!(
        decode_schema_migration_plan(&encoded).expect("plan should decode"),
        plan,
    );
}

#[test]
fn plan_digest_has_one_fixed_current_vector() {
    let plan = SchemaMigrationPlan::try_new(vec![transition("Account", "User")])
        .expect("plan should admit");
    assert_eq!(
        plan.digest().to_bytes(),
        [
            185, 94, 228, 94, 125, 249, 118, 171, 39, 218, 171, 230, 105, 177, 195, 14, 41, 14, 39,
            78, 248, 107, 177, 13, 16, 115, 37, 15, 145, 40, 134, 207,
        ],
    );
}

#[test]
fn dependency_only_companion_roundtrips_exactly() {
    let companion = EntityMigration::try_new(
        entity("Holder"),
        DeclaredEntityVersion::try_new(1).unwrap(),
        None,
        Vec::new(),
        Vec::new(),
    )
    .unwrap();
    let plan =
        SchemaMigrationPlan::try_new(vec![transition("CatalogItem", "Item"), companion]).unwrap();
    let encoded = crate::encode_schema_migration_plan(&plan).unwrap();
    assert_eq!(crate::decode_schema_migration_plan(&encoded).unwrap(), plan);
}

#[test]
fn duplicate_targets_predecessors_and_transform_targets_reject() {
    let account = transition("Account", "User");
    assert_eq!(
        SchemaMigrationPlan::try_new(vec![account.clone(), account]),
        Err(SchemaContractError::DuplicateMigrationTarget),
    );
    assert_eq!(
        SchemaMigrationPlan::try_new(vec![
            transition("Account", "User"),
            transition("Profile", "User"),
        ]),
        Err(SchemaContractError::DuplicateMigrationSource),
    );
    assert_eq!(
        EntityMigration::try_new(
            entity("Account"),
            DeclaredEntityVersion::try_new(1).expect("version should admit"),
            None,
            Vec::new(),
            vec![
                SchemaMigrationTransform::Fill {
                    to: field("value"),
                    literal: ScalarLiteral::Nat(1),
                },
                SchemaMigrationTransform::Copy {
                    from: field("old_value"),
                    to: field("value"),
                },
            ],
        ),
        Err(SchemaContractError::DuplicateMigrationTarget),
    );
}

#[test]
fn wire_plan_restoration_keeps_nested_constructor_admission() {
    let valid = transition("Account", "User");
    let mut duplicate = valid.clone();
    duplicate.renames.push(duplicate.renames[0].clone());
    let mut same_name = valid.clone();
    same_name.from_name = Some(same_name.entity.clone());
    let mut invalid_cast = valid;
    invalid_cast
        .transforms
        .push(SchemaMigrationTransform::CheckedCast {
            from: field("old_value"),
            to: field("value"),
            target: ScalarType::Float64,
        });
    for transition in [duplicate, same_name, invalid_cast] {
        let bytes = crate::wire::encode_one((*b"ICWM\x01", (1_u16, vec![transition]))).unwrap();
        assert!(decode_schema_migration_plan(&bytes).is_err());
    }
}

#[test]
fn unsupported_program_versions_and_oversized_transport_fail_closed() {
    let plan = SchemaMigrationPlan::try_new(vec![transition("Account", "User")])
        .expect("plan should admit");
    let mut bytes = encode_schema_migration_plan(&plan).expect("plan should encode");
    bytes[5] = 2;
    assert_eq!(
        decode_schema_migration_plan(&bytes),
        Err(SchemaContractError::UnsupportedMigrationProgramVersion {
            found: 2,
            supported: 1,
        }),
    );
    assert!(matches!(
        decode_schema_migration_plan(&vec![0; MAX_SCHEMA_MIGRATION_PLAN_BYTES + 1]),
        Err(SchemaContractError::EncodedTooLarge { .. }),
    ));
}
