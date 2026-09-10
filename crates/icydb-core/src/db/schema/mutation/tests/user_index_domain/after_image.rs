//! Accepted mutation planning admits after-images before its unchanged-key shortcut.

use super::*;
use crate::{
    db::{
        data::{DecodedDataStoreKey, RawRow},
        index::{
            IndexMutationPlan, IndexPlanReadView, IndexReadContract,
            plan_index_mutation_for_slot_reader_structural,
        },
    },
    error::InternalError,
};
use icydb_diagnostic_code::{DiagnosticExecutionBudgetResource as Resource, DiagnosticFactTag};
use std::{borrow::Cow, cell::Cell, ops::Bound};

struct ObservedNameRow {
    value: Value,
    visits: Cell<usize>,
    reject: bool,
}

impl ObservedNameRow {
    fn new(name: &str, reject: bool) -> Self {
        Self {
            value: Value::Text(name.to_string()),
            visits: Cell::new(0),
            reject,
        }
    }
}

impl CanonicalSlotReader for ObservedNameRow {
    fn field_name(&self, slot: usize) -> Result<&str, InternalError> {
        assert_eq!(slot, 1);
        Ok("name")
    }

    fn field_leaf_codec(&self, _slot: usize) -> Result<LeafCodec, InternalError> {
        Ok(LeafCodec::Scalar(ScalarCodec::Text))
    }

    fn required_value_by_contract(&self, slot: usize) -> Result<Value, InternalError> {
        self.required_value_by_contract_cow(slot)
            .map(Cow::into_owned)
    }

    fn required_value_by_contract_cow(&self, slot: usize) -> Result<Cow<'_, Value>, InternalError> {
        assert_eq!(slot, 1);
        self.visits.set(self.visits.get() + 1);
        if self.reject {
            return Err(InternalError::relation_budget_exceeded(
                Resource::TemporaryBytes,
                0,
                1,
            ));
        }
        Ok(Cow::Borrowed(&self.value))
    }
}

impl SlotReader for ObservedNameRow {
    fn get_bytes(&self, slot: usize) -> Option<&[u8]> {
        assert_eq!(slot, 1);
        let Value::Text(value) = &self.value else {
            panic!("fixture contains one text field");
        };
        Some(value.as_bytes())
    }

    fn get_scalar(&self, slot: usize) -> Result<Option<ScalarSlotValueRef<'_>>, InternalError> {
        assert_eq!(slot, 1);
        let Value::Text(value) = &self.value else {
            panic!("fixture contains one text field");
        };
        Ok(Some(ScalarSlotValueRef::Value(ScalarValueRef::Text(value))))
    }

    fn get_value(&mut self, _slot: usize) -> Result<Option<Value>, InternalError> {
        panic!("key derivation must use the accepted value reader");
    }
}

// Unchanged inputs must avoid every physical index/row lookup, including unique
// probes. Any accidental read rejects the test's otherwise valid plan.
struct NoIndexReads;

impl IndexPlanReadView for NoIndexReads {
    fn read_primary_row(
        &self,
        _key: &DecodedDataStoreKey,
    ) -> Result<Option<RawRow>, InternalError> {
        Err(InternalError::store_invariant())
    }

    fn has_primary_row_override(&self, _key: &DecodedDataStoreKey) -> Result<bool, InternalError> {
        Err(InternalError::store_invariant())
    }

    fn read_index_entry(
        &self,
        _index: IndexReadContract<'_>,
        _key: &RawIndexStoreKey,
    ) -> Result<Option<IndexEntryValue>, InternalError> {
        Err(InternalError::store_invariant())
    }

    fn read_index_keys_in_raw_range(
        &self,
        _index: IndexReadContract<'_>,
        _bounds: (&Bound<RawIndexStoreKey>, &Bound<RawIndexStoreKey>),
        _limit: usize,
    ) -> Result<Vec<PrimaryKeyValue>, InternalError> {
        Err(InternalError::store_invariant())
    }
}

fn after_image_schema(index: PersistedIndexSnapshot) -> (SchemaInfo, StructuralRowContract) {
    let snapshot = snapshot_with_indexes(&base_snapshot(), vec![index]);
    let row_contract = accepted_row_contract(&snapshot);
    let accepted = AcceptedSchemaSnapshot::try_new(snapshot).expect("fixture should admit");
    let catalog = AcceptedValueCatalogHandle::new_for_tests(
        empty_accepted_enum_catalog_for_tests(),
        AcceptedCompositeCatalog::empty(),
        AcceptedSchemaRevision::INITIAL,
    );
    (
        SchemaInfo::from_accepted_snapshot_and_catalog(&accepted, catalog, true),
        row_contract,
    )
}

fn plan_name_update(
    schema: &SchemaInfo,
    contract: &StructuralRowContract,
    old: &mut ObservedNameRow,
    new: &mut ObservedNameRow,
) -> Result<IndexMutationPlan, InternalError> {
    let primary_key = PrimaryKeyValue::Scalar(PrimaryKeyComponent::Ulid(crate::types::Ulid::MIN));
    match plan_index_mutation_for_slot_reader_structural(
        EntityTag::new(7),
        [0; 16],
        None,
        schema,
        &NoIndexReads,
        contract,
        Some(&primary_key),
        Some(old),
        Some(&primary_key),
        Some(new),
    ) {
        Ok(plan) => Ok(plan),
        Err(error) => Err(error.into_internal_error()),
    }
}

#[test]
fn unchanged_field_and_expression_inputs_admit_after_image_without_index_reads() {
    for index in [
        domain_field_index(1, "by_name", true),
        domain_expression_index(1, "by_lower_name", true, None),
    ] {
        let (schema, contract) = after_image_schema(index);
        let mut old = ObservedNameRow::new("Ada", false);
        let mut new = ObservedNameRow::new("Ada", false);
        let plan = plan_name_update(&schema, &contract, &mut old, &mut new)
            .expect("unchanged membership should admit");
        assert!(plan.groups.is_empty());
        assert_eq!(
            old.visits.get(),
            0,
            "unchanged before-image needs no re-encoding"
        );
        assert_eq!(
            new.visits.get(),
            1,
            "after-image admission must not be skipped or repeated"
        );
    }
}

#[test]
fn unchanged_field_and_expression_inputs_preserve_after_image_rejection() {
    for index in [
        domain_field_index(1, "by_name", false),
        domain_expression_index(1, "by_lower_name", false, None),
    ] {
        let (schema, contract) = after_image_schema(index);
        let mut old = ObservedNameRow::new("Ada", false);
        let mut new = ObservedNameRow::new("Ada", true);
        let error = plan_name_update(&schema, &contract, &mut old, &mut new)
            .expect_err("after-image failure must reject");
        assert!(error.diagnostic_facts().contains(&(
            DiagnosticFactTag::BudgetResource,
            Resource::TemporaryBytes.raw(),
        )));
        assert_eq!(old.visits.get(), 0);
        assert_eq!(new.visits.get(), 1);
    }
}

#[test]
fn changed_expression_input_reuses_its_admitted_after_image_key() {
    let (schema, contract) =
        after_image_schema(domain_expression_index(1, "by_lower_name", true, None));
    let mut old = ObservedNameRow::new("Ada", false);
    let mut new = ObservedNameRow::new("ADA", false);
    let plan = plan_name_update(&schema, &contract, &mut old, &mut new)
        .expect("equal derived keys need no index reads");
    assert!(plan.groups.is_empty());
    assert_eq!(old.visits.get(), 1);
    assert_eq!(
        new.visits.get(),
        1,
        "admitted after-image must be reused in the delta check"
    );
}

#[test]
fn false_index_membership_does_not_force_after_image_component_encoding() {
    let (schema, contract) = after_image_schema(domain_expression_index(
        1,
        "by_lower_name",
        true,
        Some("name = 'Grace'".to_string()),
    ));
    let mut old = ObservedNameRow::new("Ada", false);
    let mut new = ObservedNameRow::new("Ada", true);
    let plan = plan_name_update(&schema, &contract, &mut old, &mut new)
        .expect("nonmember rows need no component");
    assert!(plan.groups.is_empty());
    assert_eq!(old.visits.get(), 0);
    assert_eq!(new.visits.get(), 0);
}
