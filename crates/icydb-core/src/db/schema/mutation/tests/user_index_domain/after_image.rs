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
        SchemaInfo::from_accepted_snapshot_and_catalog(&accepted, catalog),
        row_contract,
    )
}

fn plan_name_update(
    schema: &SchemaInfo,
    contract: &StructuralRowContract,
    old: &ObservedNameRow,
    new: &ObservedNameRow,
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
fn malformed_accepted_predicates_reject_query_mutation_and_inspection_plans() {
    use crate::db::{index::AcceptedIndexInspectionPlan, query::plan::VisibleIndexes};
    use icydb_diagnostic_code::DiagnosticCode;

    let malformed = Some("name = 'unterminated".to_string());
    let field_index = PersistedIndexSnapshot::new(
        SchemaIndexId::new(1).unwrap(),
        1,
        "by_name".into(),
        STORE_PATH.into(),
        false,
        PersistedIndexKeySnapshot::FieldPath(vec![name_key_path()]),
        malformed.clone(),
    );
    for index in [
        field_index,
        domain_expression_index(1, "by_lower_name", false, malformed),
    ] {
        let snapshot = snapshot_with_indexes(&base_snapshot(), vec![index]);
        // Acceptance rejects this metadata. Inject it explicitly to retain the
        // query/mutation/inspection corruption defenses without weakening setup.
        let accepted = AcceptedSchemaSnapshot::new(snapshot);
        let contract = accepted_row_contract(&base_snapshot());
        let schema = SchemaInfo::from_accepted_snapshot_and_catalog(
            &accepted,
            contract.accepted_value_catalog_handle().clone(),
        );
        let old = ObservedNameRow::new("Ada", false);
        let new = ObservedNameRow::new("Ada", false);
        for error in [
            VisibleIndexes::accepted_schema_visible(&schema).unwrap_err(),
            plan_name_update(&schema, &contract, &old, &new).unwrap_err(),
            AcceptedIndexInspectionPlan::compile(
                &accepted,
                contract.accepted_value_catalog_handle().clone(),
                &contract,
                &crate::db::executor::budget::MaintenanceConstructionBudget::new(),
            )
            .unwrap_err(),
        ] {
            assert_eq!(error.diagnostic().code(), DiagnosticCode::StoreCorruption);
            assert_eq!(error.diagnostic().detail(), None);
        }
        assert_eq!(old.visits.get(), 0);
        assert_eq!(new.visits.get(), 0);
    }
}

#[test]
fn index_inspection_construction_uses_cumulative_caller_admission() {
    use crate::db::{
        QueryError, RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        index::AcceptedIndexInspectionPlan,
        query::preparation::PreparationWork,
    };
    use icydb_diagnostic_code::DiagnosticExecutionLane as Lane;

    let snapshot = snapshot_with_indexes(
        &base_snapshot(),
        vec![
            domain_field_index(1, "by_name", false),
            domain_expression_index(2, "by_lower_name", false, Some("name = 'Ada'".into())),
        ],
    );
    let row_contract = accepted_row_contract(&snapshot);
    let accepted = AcceptedSchemaSnapshot::try_new(snapshot).unwrap();
    let compile = |work: &PreparationWork<'_>| {
        AcceptedIndexInspectionPlan::compile(
            &accepted,
            row_contract.accepted_value_catalog_handle().clone(),
            &row_contract,
            work,
        )
        .map_err(QueryError::execute)
    };
    let witnesses = |plan: &AcceptedIndexInspectionPlan| {
        ["Ada", "Grace"].map(|name| {
            let row = ObservedNameRow::new(name, false);
            (0..plan.len())
                .map(|ordinal| {
                    plan.project(
                        ordinal,
                        EntityTag::new(7),
                        &PrimaryKeyValue::Scalar(PrimaryKeyComponent::Ulid(
                            crate::types::Ulid::MIN,
                        )),
                        &row,
                    )
                    .unwrap()
                })
                .collect::<Vec<_>>()
        })
    };
    let limits = HardExecutionBudget::uniform_for_tests(
        16_000_000,
        HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
    );
    let baseline = RequestExecutionRoot::new_for_tests(limits);
    let expected = PreparationWork::run(&baseline.scope(), Lane::Diagnostic, compile).unwrap();
    assert_eq!(expected.len(), 2);
    let expected_witnesses = witnesses(&expected);
    assert!(expected_witnesses[0].iter().all(Option::is_some));
    assert!(expected_witnesses[1][0].is_some());
    assert!(expected_witnesses[1][1].is_none());

    // Admission counters establish exact/cumulative rejection, not performance.
    for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
        let exact = baseline.observed(resource);
        assert!(exact > 0);
        for limit in [exact - 1, exact] {
            let root =
                RequestExecutionRoot::new_for_tests(limits.with_limit_for_tests(resource, limit));
            let result = PreparationWork::run(&root.scope(), Lane::Diagnostic, compile);
            if limit == exact {
                assert_eq!(witnesses(&result.unwrap()), expected_witnesses);
            } else {
                let error = result.unwrap_err();
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                );
                assert!(
                    error
                        .diagnostic_facts()
                        .contains(&(DiagnosticFactTag::Limit, limit))
                );
            }
        }
        let root =
            RequestExecutionRoot::new_for_tests(limits.with_limit_for_tests(resource, exact));
        let error = PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            compile(work)?;
            compile(work)
        })
        .unwrap_err();
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
        );
    }
}

#[test]
fn index_inspection_source_admission_precedes_parsing() {
    use crate::db::{
        executor::budget::MaintenanceConstructionBudget, index::AcceptedIndexInspectionPlan,
    };

    let snapshot = snapshot_with_indexes(
        &base_snapshot(),
        vec![domain_expression_index(
            1,
            "by_lower_name",
            false,
            Some("name = 'unterminated".into()),
        )],
    );
    let contract = accepted_row_contract(&base_snapshot());
    // Explicitly inject invalid authority to distinguish source admission from
    // the maintained payload-free corruption defense covered separately.
    let accepted = AcceptedSchemaSnapshot::new(snapshot);
    let work =
        MaintenanceConstructionBudget::with_limit_for_tests(Resource::PredicateExpressionSteps, 1);
    let compile = || {
        AcceptedIndexInspectionPlan::compile(
            &accepted,
            contract.accepted_value_catalog_handle().clone(),
            &contract,
            &work,
        )
    };
    let error = compile().unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::PredicateExpressionSteps.raw(),
    )));
    assert_eq!(compile().unwrap_err().diagnostic(), error.diagnostic());
}

#[test]
fn unchanged_field_and_expression_inputs_admit_after_image_without_index_reads() {
    for index in [
        domain_field_index(1, "by_name", true),
        domain_expression_index(1, "by_lower_name", true, None),
    ] {
        let (schema, contract) = after_image_schema(index);
        let old = ObservedNameRow::new("Ada", false);
        let new = ObservedNameRow::new("Ada", false);
        let plan = plan_name_update(&schema, &contract, &old, &new)
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
        let old = ObservedNameRow::new("Ada", false);
        let new = ObservedNameRow::new("Ada", true);
        let error = plan_name_update(&schema, &contract, &old, &new)
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
    let old = ObservedNameRow::new("Ada", false);
    let new = ObservedNameRow::new("ADA", false);
    let plan = plan_name_update(&schema, &contract, &old, &new)
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
    let old = ObservedNameRow::new("Ada", false);
    let new = ObservedNameRow::new("Ada", true);
    let plan =
        plan_name_update(&schema, &contract, &old, &new).expect("nonmember rows need no component");
    assert!(plan.groups.is_empty());
    assert_eq!(old.visits.get(), 0);
    assert_eq!(new.visits.get(), 0);
}
