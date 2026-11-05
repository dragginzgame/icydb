use icydb_schema::{
    ConstraintFragmentKind, ConstraintSourceKey, Decimal, FieldSourceKey, FieldType,
    MAX_PROPOSAL_LITERAL_BYTES, NamedTypeFragment, RuleSourceKey, ScalarLiteral, ScalarType,
    SourceRuleOperation, TypeSourceKey,
};

use super::{
    FragmentLoweringError, Schema, lower_blob_default, lower_field_rules, lower_nested_relations,
    lower_relation, lower_scalar_default,
};
use crate::{
    node::{
        Arg, Args, Canister, Def, Entity, Enum, EnumVariant, Field, FieldList, Item, ItemTarget,
        Newtype, Normalizer, PrimaryKey, PrimaryKeySource, Record, RuleNumber, SchemaNode,
        SourceRule, SourceRuleAuthoringOperation, Store, StoreHeapConfig, Type, TypeNormalizer,
        TypeValidator, Validator, Value,
    },
    types::{Cardinality, Primitive},
};

#[test]
fn relation_finish_preserves_source_and_ordered_target_keys() {
    use icydb_schema::{RelationDeleteAction, RelationPathStepFragment, RelationSourceFragment};

    for keys in [&["id"][..], &["tags", "id"][..]] {
        let mut schema = Schema::new();
        schema.insert_node(SchemaNode::Entity(Entity::new(
            Def::new("test", "Target"),
            "test::Store",
            1,
            PrimaryKey::new(keys, PrimaryKeySource::External),
            &[],
            &[],
            &[],
            FieldList::new(&ENTITY_FIELDS),
            EMPTY_TYPE.clone(),
        )));
        let direct = RelationSourceFragment::direct(
            keys.iter()
                .map(|key| FieldSourceKey::try_new(*key).unwrap())
                .collect(),
        );
        let mut sources = vec![direct];
        if keys.len() == 1 {
            sources.push(RelationSourceFragment::Nested {
                root: FieldSourceKey::try_new("root").unwrap(),
                steps: vec![RelationPathStepFragment::OptionalSome],
            });
        }
        for source in sources {
            let relation =
                lower_relation(&schema, "test::Target", "owner", || Ok(source.clone())).unwrap();
            assert_eq!(relation.source(), &source);
            assert_eq!(relation.target_entity().as_str(), "Target");
            assert_eq!(
                relation
                    .target_fields()
                    .iter()
                    .map(FieldSourceKey::as_str)
                    .collect::<Vec<_>>(),
                keys
            );
            assert_eq!(relation.on_delete(), RelationDeleteAction::Restrict);
        }
        assert!(matches!(
            lower_relation(&schema, "test::Target", "owner", || Ok(
                RelationSourceFragment::direct(vec![])
            )),
            Err(FragmentLoweringError::Contract(_)),
        ));
    }
}

#[test]
fn relation_target_failure_precedes_source_lowering() {
    let result = lower_relation(&Schema::new(), "test::Missing", "owner", || {
        panic!("invalid target must reject before source lowering")
    });
    assert!(
        matches!(result, Err(FragmentLoweringError::InvalidReference(path)) if path == "test::Missing")
    );
}

#[test]
fn blob_defaults_and_migration_fills_share_exact_bytes_and_refusals() {
    for text in ["", "00", "deadbeef", "DeAdBeEf", "ff00"] {
        let default = lower_blob_default(text).unwrap();
        let fill = crate::node::migration_literal_from_text("blob", text).unwrap();
        assert_eq!(default, fill);
    }
    assert_eq!(
        lower_blob_default("deadbeef"),
        Some(ScalarLiteral::Blob(icydb_schema::Blob::from(vec![
            0xde, 0xad, 0xbe, 0xef
        ])))
    );
    for text in ["a", "zz", "+1", "0x00", " 00", "é", "aé0"] {
        assert!(lower_blob_default(text).is_none());
        assert_eq!(
            crate::node::migration_literal_from_text("blob", text),
            Err(icydb_schema::SchemaContractError::InvalidLiteral)
        );
    }
    let maximum = "ff".repeat(MAX_PROPOSAL_LITERAL_BYTES);
    assert_eq!(
        lower_blob_default(&maximum),
        Some(crate::node::migration_literal_from_text("blob", &maximum).unwrap())
    );
    let oversized = format!("{maximum}00");
    assert!(lower_blob_default(&oversized).is_none());
    assert_eq!(
        crate::node::migration_literal_from_text("blob", &oversized),
        Err(icydb_schema::SchemaContractError::InvalidLiteral)
    );
    let subaccount = "aB".repeat(32);
    assert!(crate::node::migration_literal_from_text("subaccount", &subaccount).is_ok());
    for text in [
        format!("0x{subaccount}"),
        "00".repeat(31),
        "00".repeat(33),
        "gg".repeat(32),
        "+1".repeat(32),
    ] {
        assert_eq!(
            crate::node::migration_literal_from_text("subaccount", &text),
            Err(icydb_schema::SchemaContractError::InvalidLiteral)
        );
    }
}

#[test]
fn blob_default_lowering_enforces_the_proposal_literal_bound() {
    let maximum = "ab".repeat(MAX_PROPOSAL_LITERAL_BYTES);
    let oversized = "ab".repeat(MAX_PROPOSAL_LITERAL_BYTES + 1);

    assert_eq!(
        lower_blob_default(&maximum).and_then(|literal| match literal {
            ScalarLiteral::Blob(value) => Some(value.len()),
            _ => None,
        }),
        Some(MAX_PROPOSAL_LITERAL_BYTES),
    );
    assert_eq!(lower_blob_default(&oversized), None);
}

#[test]
fn duration_default_lowering_rejects_suffixed_overflow() {
    let item = Item::new(
        ItemTarget::Primitive(Primitive::Duration),
        None,
        None,
        None,
        &[],
        &[],
        false,
    );

    assert_eq!(
        lower_scalar_default(
            Primitive::Duration,
            &item,
            &Arg::String("18446744073709551615ms"),
        ),
        Some(ScalarLiteral::Duration(icydb_schema::Duration::MAX)),
    );
    assert_eq!(
        lower_scalar_default(
            Primitive::Duration,
            &item,
            &Arg::String("18446744073709552s"),
        ),
        None,
    );
}

static EMPTY_TYPE: Type = Type::new(&[], &[], &[]);
static APPLICATION_FIELDS: [Field; 1] = [Field::new(
    "id",
    Value::new(
        Cardinality::One,
        Item::new(
            ItemTarget::Primitive(Primitive::Nat64),
            None,
            None,
            None,
            &[],
            &[],
            false,
        ),
    ),
    None,
    None,
    None,
)];
static APPLICATION_NORMALIZERS_A: [TypeNormalizer; 1] =
    [TypeNormalizer::new("test::NormalizeA", Args(&[]))];
static APPLICATION_NORMALIZERS_B: [TypeNormalizer; 1] =
    [TypeNormalizer::new("test::NormalizeB", Args(&[]))];
static APPLICATION_VALIDATORS_A: [TypeValidator; 1] =
    [TypeValidator::new("test::ValidateA", Args(&[]))];
static APPLICATION_VALIDATORS_B: [TypeValidator; 1] =
    [TypeValidator::new("test::ValidateB", Args(&[]))];
static NUMERIC_RULES: [SourceRule; 1] = [SourceRule::new(
    "range",
    SourceRuleAuthoringOperation::NumericRangeInclusive {
        min: RuleNumber::Integer("0"),
        max: RuleNumber::Integer("360"),
    },
)];
static NUMERIC_RULE_TYPE: Type = Type::new(&[], &[], &NUMERIC_RULES);
static LENGTH_RULES: [SourceRule; 1] = [SourceRule::new(
    "length",
    SourceRuleAuthoringOperation::LengthRangeInclusive {
        min: RuleNumber::Integer("2"),
        max: RuleNumber::Integer("40"),
    },
)];
static LENGTH_RULE_TYPE: Type = Type::new(&[], &[], &LENGTH_RULES);
static NAT_EXACT_RULES: [SourceRule; 2] = [
    SourceRule::new(
        "maximum",
        SourceRuleAuthoringOperation::NumericMaximumInclusive {
            value: RuleNumber::Integer("100"),
        },
    ),
    SourceRule::new(
        "step",
        SourceRuleAuthoringOperation::MultipleOf {
            divisor: RuleNumber::Integer("5"),
        },
    ),
];
static NAT_EXACT_RULE_TYPE: Type = Type::new(&[], &[], &NAT_EXACT_RULES);
static DECIMAL_EXACT_RULES: [SourceRule; 1] = [SourceRule::new(
    "step",
    SourceRuleAuthoringOperation::MultipleOf {
        divisor: RuleNumber::Decimal("0.25"),
    },
)];
static DECIMAL_EXACT_RULE_TYPE: Type = Type::new(&[], &[], &DECIMAL_EXACT_RULES);
static INEXACT_DECIMAL_RULES: [SourceRule; 1] = [SourceRule::new(
    "step",
    SourceRuleAuthoringOperation::MultipleOf {
        divisor: RuleNumber::Decimal("0.251"),
    },
)];
static INEXACT_DECIMAL_RULE_TYPE: Type = Type::new(&[], &[], &INEXACT_DECIMAL_RULES);
static NESTED_RULE_FIELDS: [Field; 1] = [Field::new(
    "degrees",
    Value::new(
        Cardinality::One,
        Item::new(
            ItemTarget::Is("test::Degrees"),
            None,
            None,
            None,
            &[],
            &[],
            false,
        ),
    ),
    None,
    None,
    None,
)];
static STATUS_VARIANTS: [EnumVariant; 2] = [
    EnumVariant::new("Active", None),
    EnumVariant::new(
        "Retries",
        Some(Value::new(
            Cardinality::Many,
            Item::new(
                ItemTarget::Primitive(Primitive::Nat16),
                None,
                None,
                None,
                &[],
                &[],
                false,
            ),
        )),
    ),
];
static RECURSIVE_RECORD_FIELDS: [Field; 1] = [Field::new(
    "next",
    Value::new(
        Cardinality::Opt,
        Item::new(
            ItemTarget::Is("test::Recursive"),
            None,
            None,
            None,
            &[],
            &[],
            false,
        ),
    ),
    None,
    None,
    None,
)];
static RECURSIVE_ENTITY_FIELDS: [Field; 1] = [Field::new(
    "root",
    Value::new(
        Cardinality::One,
        Item::new(
            ItemTarget::Is("test::Recursive"),
            None,
            None,
            None,
            &[],
            &[],
            false,
        ),
    ),
    None,
    None,
    None,
)];

#[test]
fn recursive_structural_type_without_a_finite_relation_path_is_ignored() {
    let mut schema = Schema::new();
    schema.insert_node(SchemaNode::Record(Record::new(
        Def::new("test", "Recursive"),
        "Recursive",
        FieldList::new(&RECURSIVE_RECORD_FIELDS),
        EMPTY_TYPE.clone(),
    )));
    let entity = Entity::new(
        Def::new("test", "RecursiveOwner"),
        "test::Store",
        1,
        PrimaryKey::new(&["root"], PrimaryKeySource::External),
        &[],
        &[],
        &[],
        FieldList::new(&RECURSIVE_ENTITY_FIELDS),
        EMPTY_TYPE.clone(),
    );

    assert!(
        lower_nested_relations(&schema, &entity, &RECURSIVE_ENTITY_FIELDS[0])
            .expect("recursive structural types without finite relations should lower")
            .is_empty(),
    );
}

#[test]
fn recursive_relation_sources_reject_in_both_sibling_orders() {
    for relation_first in [false, true] {
        let relation = Field::new(
            "owner",
            Value::new(
                Cardinality::One,
                Item::new(
                    ItemTarget::Primitive(Primitive::Ulid),
                    Some("test::User"),
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
            None,
            None,
        );
        let next = RECURSIVE_RECORD_FIELDS[0].clone();
        let fields = if relation_first {
            vec![relation, next]
        } else {
            vec![next, relation]
        };
        let mut schema = Schema::new();
        schema.insert_node(SchemaNode::Record(Record::new(
            Def::new("test", "Recursive"),
            "Recursive",
            FieldList::new(Box::leak(fields.into_boxed_slice())),
            EMPTY_TYPE.clone(),
        )));
        let entity = Entity::new(
            Def::new("test", "RecursiveOwner"),
            "test::Store",
            1,
            PrimaryKey::new(&["root"], PrimaryKeySource::External),
            &[],
            &[],
            &[],
            FieldList::new(&RECURSIVE_ENTITY_FIELDS),
            EMPTY_TYPE.clone(),
        );
        assert!(matches!(
            lower_nested_relations(&schema, &entity, &RECURSIVE_ENTITY_FIELDS[0]),
            Err(FragmentLoweringError::UnsupportedCardinality(_))
        ));
    }
}

fn application_behavior_fragment(
    normalizers: &'static [TypeNormalizer],
    validators: &'static [TypeValidator],
    normalizer_name: &'static str,
    validator_name: &'static str,
) -> icydb_schema::SchemaFragment {
    let mut schema = Schema::new();
    schema.insert_node(SchemaNode::Canister(Canister::new(
        Def::new("test", "Canister"),
        "test",
        None,
    )));
    schema.insert_node(SchemaNode::Store(Store::new_heap(
        Def::new("test", "Store"),
        "test::Canister",
        StoreHeapConfig::new(),
    )));
    schema.insert_node(SchemaNode::Normalizer(Normalizer::new(Def::new(
        "test",
        normalizer_name,
    ))));
    schema.insert_node(SchemaNode::Validator(Validator::new(Def::new(
        "test",
        validator_name,
    ))));
    schema.insert_node(SchemaNode::Entity(Entity::new(
        Def::new("test", "ApplicationOnly"),
        "test::Store",
        1,
        PrimaryKey::new(&["id"], PrimaryKeySource::External),
        &[],
        &[],
        &[],
        FieldList::new(&APPLICATION_FIELDS),
        Type::new(normalizers, validators, &[]),
    )));
    schema.seal().expect("application-only fixture should seal");
    schema
        .schema_fragment_for_canister("test::Canister")
        .expect("application-only fixture should lower")
}

#[test]
fn validator_and_normalizer_edits_do_not_change_database_fragment() {
    let before = application_behavior_fragment(
        &APPLICATION_NORMALIZERS_A,
        &APPLICATION_VALIDATORS_A,
        "NormalizeA",
        "ValidateA",
    );
    let after = application_behavior_fragment(
        &APPLICATION_NORMALIZERS_B,
        &APPLICATION_VALIDATORS_B,
        "NormalizeB",
        "ValidateB",
    );

    assert_eq!(before, after);
}

#[test]
fn durable_rules_nested_below_structural_fields_lower_to_nominal_targets() {
    let mut schema = Schema::new();
    schema.insert_node(SchemaNode::Newtype(Newtype::new(
        Def::new("test", "Degrees"),
        "Degrees",
        Item::new(
            ItemTarget::Primitive(Primitive::Nat16),
            None,
            None,
            None,
            &[],
            &[],
            false,
        ),
        None,
        NUMERIC_RULE_TYPE.clone(),
    )));
    schema.insert_node(SchemaNode::Record(Record::new(
        Def::new("test", "Nested"),
        "Nested",
        FieldList::new(&NESTED_RULE_FIELDS),
        EMPTY_TYPE.clone(),
    )));

    let outer = Field::new(
        "nested",
        Value::new(
            Cardinality::One,
            Item::new(
                ItemTarget::Is("test::Nested"),
                None,
                None,
                None,
                &[],
                &[],
                false,
            ),
        ),
        None,
        None,
        None,
    );
    let constraints = lower_field_rules(&schema, &outer).expect("nested durable rule should lower");
    assert_eq!(constraints.len(), 1);
    let ConstraintFragmentKind::TargetedRule(rule) = constraints[0].kind() else {
        panic!("nested durable rule should use the targeted-rule contract")
    };
    assert_eq!(rule.root().as_str(), "nested");
    assert_eq!(rule.target_type().as_str(), "Degrees");
    assert!(matches!(
        rule.operation(),
        SourceRuleOperation::NumericRangeInclusive { .. }
    ));
}

#[test]
fn exact_maximum_and_multiple_of_lower_without_float_reconstruction() {
    let mut schema = Schema::new();
    for (name, primitive, scale, rules) in [
        (
            "Counter",
            Primitive::Nat64,
            None,
            NAT_EXACT_RULE_TYPE.clone(),
        ),
        (
            "PriceStep",
            Primitive::Decimal,
            Some(2),
            DECIMAL_EXACT_RULE_TYPE.clone(),
        ),
    ] {
        schema.insert_node(SchemaNode::Newtype(Newtype::new(
            Def::new("test", name),
            name,
            Item::new(
                ItemTarget::Primitive(primitive),
                None,
                scale,
                None,
                &[],
                &[],
                false,
            ),
            None,
            rules,
        )));
    }

    let field = |name| {
        Field::new(
            name,
            Value::new(
                Cardinality::One,
                Item::new(
                    ItemTarget::Is(if name == "counter" {
                        "test::Counter"
                    } else {
                        "test::PriceStep"
                    }),
                    None,
                    None,
                    None,
                    &[],
                    &[],
                    false,
                ),
            ),
            None,
            None,
            None,
        )
    };
    let counter =
        lower_field_rules(&schema, &field("counter")).expect("exact integer rules should lower");
    assert!(matches!(
        counter[0].kind(),
        ConstraintFragmentKind::TargetedRule(rule)
            if matches!(
                rule.operation(),
                SourceRuleOperation::NumericMaximumInclusive {
                    value: ScalarLiteral::Nat(100)
                }
            )
    ));
    assert!(matches!(
        counter[1].kind(),
        ConstraintFragmentKind::TargetedRule(rule)
            if matches!(
                rule.operation(),
                SourceRuleOperation::MultipleOf {
                    divisor: ScalarLiteral::Nat(5)
                }
            )
    ));

    let decimal =
        lower_field_rules(&schema, &field("price")).expect("exact decimal multiple should lower");
    assert!(matches!(
        decimal[0].kind(),
        ConstraintFragmentKind::TargetedRule(rule)
            if matches!(
                rule.operation(),
                SourceRuleOperation::MultipleOf {
                    divisor: ScalarLiteral::Decimal(value)
                } if *value == Decimal::new(25, 2)
            )
    ));
}

#[test]
fn inexact_decimal_rule_operand_rejects_before_proposal_composition() {
    let mut schema = Schema::new();
    schema.insert_node(SchemaNode::Newtype(Newtype::new(
        Def::new("test", "InexactStep"),
        "InexactStep",
        Item::new(
            ItemTarget::Primitive(Primitive::Decimal),
            None,
            Some(2),
            None,
            &[],
            &[],
            false,
        ),
        None,
        INEXACT_DECIMAL_RULE_TYPE.clone(),
    )));
    let field = Field::new(
        "price",
        Value::new(
            Cardinality::One,
            Item::new(
                ItemTarget::Is("test::InexactStep"),
                None,
                None,
                None,
                &[],
                &[],
                false,
            ),
        ),
        None,
        None,
        None,
    );
    assert!(lower_field_rules(&schema, &field).is_err());
}

static ENTITY_FIELDS: [Field; 5] = [
    Field::new(
        "id",
        Value::new(
            Cardinality::One,
            Item::new(
                ItemTarget::Primitive(Primitive::Nat64),
                None,
                None,
                None,
                &[],
                &[],
                false,
            ),
        ),
        None,
        None,
        None,
    ),
    Field::new(
        "tags",
        Value::new(
            Cardinality::Many,
            Item::new(
                ItemTarget::Primitive(Primitive::Text),
                None,
                None,
                Some(32),
                &[],
                &[],
                false,
            ),
        ),
        None,
        None,
        None,
    ),
    Field::new(
        "status",
        Value::new(
            Cardinality::One,
            Item::new(
                ItemTarget::Is("test::Status"),
                None,
                None,
                None,
                &[],
                &[],
                false,
            ),
        ),
        Some(crate::node::Arg::ConstPath("test::Status::Active")),
        None,
        None,
    ),
    Field::new(
        "degrees",
        Value::new(
            Cardinality::One,
            Item::new(
                ItemTarget::Is("test::Degrees"),
                None,
                None,
                None,
                &[],
                &[],
                false,
            ),
        ),
        None,
        None,
        None,
    ),
    Field::new(
        "label",
        Value::new(
            Cardinality::One,
            Item::new(
                ItemTarget::Is("test::Label"),
                None,
                None,
                None,
                &[],
                &[],
                false,
            ),
        ),
        None,
        None,
        None,
    ),
];

#[test]
#[expect(
    clippy::too_many_lines,
    reason = "one graph fixture proves the complete field, type, relation, and durable-rule closure"
)]
fn sealed_canister_graph_emits_store_free_database_closure() {
    let mut schema = Schema::new();
    schema.insert_node(SchemaNode::Canister(Canister::new(
        Def::new("test", "Canister"),
        "test",
        None,
    )));
    schema.insert_node(SchemaNode::Store(Store::new_heap(
        Def::new("test", "Store"),
        "test::Canister",
        StoreHeapConfig::new(),
    )));
    schema.insert_node(SchemaNode::Enum(Enum::new(
        Def::new("test", "Status"),
        "Status",
        &STATUS_VARIANTS,
        EMPTY_TYPE.clone(),
    )));
    schema.insert_node(SchemaNode::Newtype(Newtype::new(
        Def::new("test", "Degrees"),
        "Degrees",
        Item::new(
            ItemTarget::Primitive(Primitive::Nat16),
            None,
            None,
            None,
            &[],
            &[],
            false,
        ),
        None,
        NUMERIC_RULE_TYPE.clone(),
    )));
    schema.insert_node(SchemaNode::Newtype(Newtype::new(
        Def::new("test", "Label"),
        "Label",
        Item::new(
            ItemTarget::Primitive(Primitive::Text),
            None,
            None,
            None,
            &[],
            &[],
            false,
        ),
        None,
        LENGTH_RULE_TYPE.clone(),
    )));
    schema.insert_node(SchemaNode::Entity(Entity::new(
        Def::new("test", "Task"),
        "test::Store",
        1,
        PrimaryKey::new(&["id"], PrimaryKeySource::External),
        &[],
        &[],
        &[],
        FieldList::new(&ENTITY_FIELDS),
        EMPTY_TYPE.clone(),
    )));
    schema.seal().expect("fixture graph should seal");

    let fragment = schema
        .schema_fragment_for_canister("test::Canister")
        .expect("sealed database closure should lower");

    assert_eq!(fragment.entities().len(), 1);
    assert_eq!(fragment.types().len(), 3);
    let fields = fragment.entities()[0].fields();
    assert!(matches!(
        fields
            .iter()
            .find(|field| field.name().as_str() == "tags")
            .map(icydb_schema::FieldFragment::field_type),
        Some(FieldType::List(item))
            if matches!(item.as_ref(), FieldType::Scalar(ScalarType::Text { max_len: Some(32) }))
    ));
    assert!(matches!(
        fields
            .iter()
            .find(|field| field.name().as_str() == "status")
            .map(icydb_schema::FieldFragment::insert_policy),
        Some(icydb_schema::FieldInsertPolicy::Default(
            icydb_schema::ScalarLiteral::EnumUnit { .. }
        ))
    ));
    let NamedTypeFragment::Enum(status) = fragment
        .types()
        .iter()
        .find(|fragment| matches!(fragment, NamedTypeFragment::Enum(_)))
        .expect("reachable status type should remain an enum")
    else {
        panic!("reachable status type should remain an enum")
    };
    assert!(matches!(
        status
            .variants()
            .iter()
            .find(|variant| variant.name().as_str() == "Retries")
            .and_then(|variant| variant.payload()),
        Some(FieldType::List(item))
            if matches!(item.as_ref(), FieldType::Scalar(ScalarType::Nat16))
    ));

    let constraints = fragment.entities()[0].constraints();
    assert_eq!(constraints.len(), 2);
    let degrees_source = ConstraintSourceKey::for_targeted_field_rule(
        &FieldSourceKey::try_new("degrees").expect("field name"),
        &TypeSourceKey::try_new("Degrees").expect("type name"),
        &RuleSourceKey::try_new("range").expect("rule name"),
    );
    let degrees = constraints
        .iter()
        .find(|constraint| constraint.source_key() == &degrees_source)
        .expect("numeric rule should become one field-owned constraint");
    let ConstraintFragmentKind::TargetedRule(degrees) = degrees.kind() else {
        panic!("numeric rule should use the targeted-rule contract")
    };
    assert_eq!(degrees.root().as_str(), "degrees");
    assert_eq!(degrees.target_type().as_str(), "Degrees");
    assert!(matches!(
        degrees.operation(),
        SourceRuleOperation::NumericRangeInclusive { .. }
    ));
    let label = constraints
        .iter()
        .find(|constraint| constraint.source_key() != &degrees_source)
        .expect("length rule should become one field-owned constraint");
    let ConstraintFragmentKind::TargetedRule(label) = label.kind() else {
        panic!("length rule should use the targeted-rule contract")
    };
    assert_eq!(label.target_type().as_str(), "Label");
    assert!(matches!(
        label.operation(),
        SourceRuleOperation::LengthRangeInclusive { min: 2, max: 40 }
    ));
}
