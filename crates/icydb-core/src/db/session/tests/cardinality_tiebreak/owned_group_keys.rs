//! Generic hash grouping reuses canonical owned keys across nonadjacent rows.

use super::*;
use crate::{
    db::{count, sum},
    types::Decimal,
    value::Value,
};

fn initialize_group_schema(kind: AcceptedFieldKind) {
    scalar_page_limits::initialize_payload_schema(
        vec![
            field(1, "id", 0, AcceptedFieldKind::Nat64),
            field(2, "category", 1, AcceptedFieldKind::Nat64),
            field(3, "operand", 2, kind),
            field(4, "label", 3, AcceptedFieldKind::Text { max_len: None }),
        ],
        vec![PersistedIndexSnapshot::new(
            SchemaIndexId::new(1).unwrap(),
            1,
            "category_idx".into(),
            STORE_PATH.into(),
            false,
            PersistedIndexKeySnapshot::FieldPath(vec![PersistedIndexFieldPathSnapshot::new(
                FieldId::new(2),
                SchemaFieldSlot::new(1),
                vec!["category".into()],
                AcceptedFieldKind::Nat64,
                false,
            )]),
            None,
        )],
    );
}

fn assert_owned_group_results(values: &[(InputValue, OutputValue)]) {
    let session = new_request_session(&crate::db::RequestExecutionRoot::__new_runtime_root());
    for id in 1_u64..=5 {
        let key_index = usize::try_from((id - 1) % u64::try_from(values.len()).unwrap()).unwrap();
        session
            .execute_trusted_dynamic_insert_batch(
                ENTITY_NAME,
                vec![DynamicStructuralPatch::new(vec![
                    ("id".into(), DynamicWriteCell::Value(InputValue::nat64(id))),
                    (
                        "category".into(),
                        DynamicWriteCell::Value(InputValue::nat64(3)),
                    ),
                    (
                        "operand".into(),
                        DynamicWriteCell::Value(values[key_index].0.clone()),
                    ),
                    (
                        "label".into(),
                        DynamicWriteCell::Value(InputValue::text("row".into())),
                    ),
                ])],
            )
            .unwrap();
    }
    for multi_field in [false, true] {
        let mut base = DynamicQuery::new(ENTITY_NAME)
            .filter(FieldRef::new("category").eq(InputValue::nat64(3)))
            .group_by("operand")
            .aggregate(count())
            .grouped_limits(8, 64 * 1024);
        if multi_field {
            base = base.group_by("category");
        }
        // SUM selects the generic bundle; COUNT alone is the maintained
        // dedicated-fold control. An unrelated index admits the public lane.
        let generic = base.clone().aggregate(sum("id"));
        for _ in 0..2 {
            let counts = session
                .execute_trusted_dynamic_grouped_query(&base)
                .unwrap();
            let grouped = session
                .execute_trusted_dynamic_grouped_query(&generic)
                .unwrap();
            assert_eq!(counts.rows.len(), values.len());
            assert_eq!(grouped.rows.len(), values.len());
            for (key_index, (_, output)) in values.iter().enumerate() {
                let mut key = vec![output.clone()];
                if multi_field {
                    key.push(OutputValue::nat64(3));
                }
                let ids = (1_u64..=5)
                    .filter(|id| {
                        usize::try_from((id - 1) % u64::try_from(values.len()).unwrap()).unwrap()
                            == key_index
                    })
                    .collect::<Vec<_>>();
                let count = OutputValue::nat64(u64::try_from(ids.len()).unwrap());
                let sum = OutputValue::decimal(Decimal::from(ids.into_iter().sum::<u64>()));
                assert_eq!(
                    counts
                        .rows
                        .iter()
                        .find(|row| row.group_key() == key)
                        .unwrap()
                        .aggregate_values(),
                    std::slice::from_ref(&count)
                );
                assert_eq!(
                    grouped
                        .rows
                        .iter()
                        .find(|row| row.group_key() == key)
                        .unwrap()
                        .aggregate_values(),
                    &[count, sum]
                );
            }
            // Limits count distinct groups, rather than the five input rows.
            let tight = generic
                .clone()
                .grouped_limits(u32::try_from(values.len()).unwrap(), 64 * 1024);
            assert_eq!(
                session
                    .execute_trusted_dynamic_grouped_query(&tight)
                    .unwrap()
                    .rows,
                grouped.rows
            );
            assert_eq!(
                session
                    .execute_public_dynamic_grouped_query(&tight)
                    .unwrap()
                    .rows,
                grouped.rows
            );
        }
    }
}

fn assert_group_kind(kind: AcceptedFieldKind, values: Vec<Value>) {
    initialize_group_schema(kind);
    let values = values
        .into_iter()
        .map(|value| (InputValue::from(value.clone()), OutputValue::from(value)))
        .collect::<Vec<_>>();
    assert_owned_group_results(&values);
}

#[test]
fn owned_group_unit_reuses_one_group() {
    assert_group_kind(AcceptedFieldKind::Unit, vec![Value::Unit]);
}

#[test]
fn owned_group_lists_reuse_nonadjacent_groups() {
    assert_group_kind(
        AcceptedFieldKind::List(Box::new(AcceptedFieldKind::Nat64)),
        [7, 9]
            .map(|key| Value::List(vec![Value::Nat64(key)]))
            .to_vec(),
    );
}

#[test]
fn owned_group_sets_reuse_nonadjacent_groups() {
    assert_group_kind(
        AcceptedFieldKind::Set(Box::new(AcceptedFieldKind::Nat64)),
        [7, 9]
            .map(|key| Value::List(vec![Value::Nat64(key)]))
            .to_vec(),
    );
}

#[test]
fn owned_group_maps_reuse_nonadjacent_groups() {
    assert_group_kind(
        AcceptedFieldKind::Map {
            key: Box::new(AcceptedFieldKind::Text { max_len: None }),
            value: Box::new(AcceptedFieldKind::Nat64),
        },
        [7, 9]
            .map(|key| Value::Map(vec![(Value::Text("value".into()), Value::Nat64(key))]))
            .to_vec(),
    );
}

#[test]
fn owned_group_enum_uses_accepted_catalog_identity() {
    use crate::{
        db::schema::{TestEnumDefinition, TestEnumVariant, build_accepted_enum_catalog_for_tests},
        value::{PublicEnumValue, PublicValue},
    };
    const ENUM_PATH: &str = "db::session::tests::cardinality_tiebreak::GroupStatus";
    let enums = build_accepted_enum_catalog_for_tests(&[TestEnumDefinition::new(
        ENUM_PATH,
        vec![
            TestEnumVariant::unit("Paid"),
            TestEnumVariant::unit("Pending"),
        ],
    )])
    .unwrap();
    let kind = AcceptedFieldKind::Enum {
        type_id: enums.type_id(ENUM_PATH).unwrap(),
    };
    hybrid_components::initialize_component_schema(kind, enums);
    let values = ["Paid", "Pending"].map(|name| {
        (
            InputValue::enum_value(name, Some(ENUM_PATH)),
            OutputValue::from_public(PublicValue::Enum(PublicEnumValue::new(
                name,
                Some(ENUM_PATH),
            ))),
        )
    });
    assert_owned_group_results(&values);
}

#[test]
fn owned_group_change_preserves_borrowed_scalar_groups() {
    assert_group_kind(
        AcceptedFieldKind::Text { max_len: None },
        ["first", "second"]
            .map(|key| Value::Text(key.into()))
            .to_vec(),
    );
}
