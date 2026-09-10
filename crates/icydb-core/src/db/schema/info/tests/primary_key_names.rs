//! Accepted primary-key names are shared by detached schemas and finalized plans.

use super::{newtype_query_schema, scalar_field};
use crate::{
    db::{
        MissingRowPolicy,
        query::{plan::AccessPlannedQuery, preparation::with_preparation_work},
        schema::{
            AcceptedFieldKind, AcceptedSchemaSnapshot, FieldId, PersistedSchemaSnapshot,
            ScalarCodec, SchemaInfo, SchemaRowLayout, SchemaVersion,
        },
    },
    retained::RetainedBytes,
};
use std::rc::Rc;

fn schema(names: &[&str], key_ids: &[u32]) -> SchemaInfo {
    let fields = names
        .iter()
        .enumerate()
        .map(|(index, name)| {
            scalar_field(
                u32::try_from(index).unwrap() + 1,
                u16::try_from(index).unwrap(),
                name,
                AcceptedFieldKind::Nat64,
                ScalarCodec::Nat64,
            )
        })
        .collect::<Vec<_>>();
    let snapshot = PersistedSchemaSnapshot::new(
        SchemaVersion::initial(),
        "tests::PrimaryKeyNames".into(),
        "PrimaryKeyNames".into(),
        key_ids
            .iter()
            .copied()
            .map(FieldId::new)
            .collect::<Vec<_>>(),
        SchemaRowLayout::initial(
            fields
                .iter()
                .map(|field| (field.id(), field.slot()))
                .collect(),
        ),
        fields,
    );
    SchemaInfo::from_accepted_snapshot_and_catalog(
        &AcceptedSchemaSnapshot::new(snapshot),
        newtype_query_schema().value_catalog,
        true,
    )
}

fn finalize(plan: &mut AccessPlannedQuery, schema: &SchemaInfo) {
    with_preparation_work(|work| {
        let projection = plan.prepare_projection(schema, work)?;
        plan.finalize_static_execution_planning_contract_with_schema(schema, projection, work)
    })
    .unwrap();
}

#[test]
fn accepted_primary_key_names_share_backing_across_schema_and_plan_clones() {
    for key_ids in [vec![1], vec![2, 1]] {
        let schema = schema(&["id", "account"], &key_ids);
        let source = schema.shared_primary_key_names();
        let cloned_schema = schema.clone();
        let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
        finalize(&mut plan, &schema);
        let cloned_plan = plan.clone();
        for candidate in [&plan, &cloned_plan] {
            assert!(std::ptr::eq(
                candidate.primary_key_names().unwrap(),
                source.as_ref()
            ));
        }
        for names in [
            cloned_schema.shared_primary_key_names(),
            plan.static_execution_planning_contract
                .as_ref()
                .unwrap()
                .primary_key_names
                .clone(),
            cloned_plan
                .static_execution_planning_contract
                .as_ref()
                .unwrap()
                .primary_key_names
                .clone(),
        ] {
            assert!(Rc::ptr_eq(&source, &names));
            for (accepted, retained) in source.iter().zip(names.iter()) {
                assert_eq!(accepted.as_ptr(), retained.as_ptr());
            }
        }
        let expected = key_ids
            .iter()
            .map(|id| if *id == 1 { "id" } else { "account" })
            .collect::<Vec<_>>();
        drop(source);
        drop(schema);
        drop(cloned_schema);
        drop(plan);
        assert_eq!(cloned_plan.primary_key_names().unwrap(), expected);
    }
}

#[test]
fn refinalization_uses_current_key_names_without_changing_detached_plans() {
    let old_schema = schema(&["id", "account"], &[2, 1]);
    let current_schema = schema(&["id_now", "account_now"], &[2, 1]);
    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    finalize(&mut plan, &old_schema);
    let previous = plan.clone();
    finalize(&mut plan, &current_schema);
    let names = &plan
        .static_execution_planning_contract
        .as_ref()
        .unwrap()
        .primary_key_names;
    assert!(Rc::ptr_eq(
        names,
        &current_schema.shared_primary_key_names()
    ));
    assert!(!Rc::ptr_eq(names, &old_schema.shared_primary_key_names()));
    assert_eq!(plan.primary_key_names().unwrap(), ["account_now", "id_now"]);
    assert_eq!(previous.primary_key_names().unwrap(), ["account", "id"]);
}

#[test]
fn retained_primary_key_names_include_shared_headers_and_string_capacity() {
    let schema = schema(&["id", "账户"], &[2, 1]);
    let names = schema.shared_primary_key_names();
    let expected = size_of::<Rc<[String]>>()
        + 2 * size_of::<usize>()
        + size_of_val(names.as_ref())
        + names.iter().map(String::capacity).sum::<usize>();
    for resident in [names.clone(), names] {
        assert_eq!(RetainedBytes::measure(&resident, expected), Some(expected));
        assert_eq!(RetainedBytes::measure(&resident, expected - 1), None);
    }
    // The enclosing contract must also account for the shared field, not only
    // its inline pointer. Replacing it with an empty list isolates the payload.
    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    finalize(&mut plan, &schema);
    let contract = plan.static_execution_planning_contract.as_mut().unwrap();
    let full = RetainedBytes::measure(contract, usize::MAX).unwrap();
    // Measure the same contract: cloning unrelated vectors may shrink their
    // capacities and would confound the isolated key-name contribution.
    let saved = std::mem::replace(&mut contract.primary_key_names, Rc::from([]));
    let empty_bytes = RetainedBytes::measure(contract, usize::MAX).unwrap();
    assert_eq!(
        full - empty_bytes,
        expected - size_of::<Rc<[String]>>() - 2 * size_of::<usize>()
    );
    contract.primary_key_names = saved;
    assert_eq!(RetainedBytes::measure(contract, full - 1), None);
}
