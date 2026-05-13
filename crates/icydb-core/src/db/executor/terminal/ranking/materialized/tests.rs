use crate::{
    db::executor::{
        aggregate::field::FieldSlot,
        terminal::ranking::materialized::{
            RankedFieldDirection, apply_ranked_take_window, compare_ranked_keys_and_values,
        },
    },
    model::field::FieldKind,
    value::Value,
};
use std::cmp::Ordering;

fn nat_field_slot() -> FieldSlot {
    FieldSlot {
        index: 0,
        kind: FieldKind::Nat,
    }
}

#[test]
fn compare_ranked_keys_and_values_desc_uses_value_then_key_order() {
    let ordering = compare_ranked_keys_and_values(
        "score",
        nat_field_slot(),
        &2_u64,
        &Value::Nat(9),
        &1_u64,
        &Value::Nat(7),
        RankedFieldDirection::Descending,
    )
    .expect("comparison");
    assert_eq!(ordering, Ordering::Less);

    let tie_break_ordering = compare_ranked_keys_and_values(
        "score",
        nat_field_slot(),
        &1_u64,
        &Value::Nat(7),
        &2_u64,
        &Value::Nat(7),
        RankedFieldDirection::Descending,
    )
    .expect("comparison");
    assert_eq!(tie_break_ordering, Ordering::Less);
}

#[test]
fn apply_ranked_take_window_keeps_smallest_bottom_k_in_final_order() {
    let mut ranked_rows = vec![
        ((4_u64, ()), Value::Nat(40)),
        ((2_u64, ()), Value::Nat(20)),
        ((3_u64, ()), Value::Nat(30)),
        ((1_u64, ()), Value::Nat(10)),
    ];

    apply_ranked_take_window(
        "score",
        nat_field_slot(),
        &mut ranked_rows,
        2,
        RankedFieldDirection::Ascending,
    )
    .expect("bounded ranking");

    assert_eq!(
        ranked_rows,
        vec![((1_u64, ()), Value::Nat(10)), ((2_u64, ()), Value::Nat(20))],
    );
}
