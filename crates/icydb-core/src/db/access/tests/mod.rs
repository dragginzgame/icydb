//! Module: db::access::tests
//! Covers source-tree hygiene and regression checks for the access subsystem.

mod canonical;

use std::{collections::BTreeSet, fs, ops::Bound, path::Path};

use crate::{
    db::{
        access::{
            AccessPathKind, AccessPlan, SemanticIndexAccessContract, SemanticIndexKeyItemsRef,
            SemanticIndexRangeSpec, lower_access,
        },
        index::{
            EncodedValue, IndexId, IndexKeyKind, build_index_prefix_bounds_for_encoded_components,
        },
        test_support::source_guard::{
            collect_rust_sources, relative_rust_source_path, runtime_source_without_test_items,
        },
    },
    model::index::{IndexKeyItemsRef, IndexModel},
    types::EntityTag,
    value::Value,
};

const CAPABILITY_TEST_INDEX_FIELDS: [&str; 2] = ["rank", "name"];
const CAPABILITY_TEST_INDEX: IndexModel = IndexModel::generated(
    "access::tests::capability_idx_rank_name",
    "access::tests::Store",
    &CAPABILITY_TEST_INDEX_FIELDS,
    false,
);

#[test]
fn access_shape_facts_preserve_pure_index_range_details() {
    let spec = SemanticIndexRangeSpec::new(
        CAPABILITY_TEST_INDEX,
        vec![0, 1],
        vec![Value::Nat64(7)],
        Bound::Included(Value::Text("a".to_string())),
        Bound::Excluded(Value::Text("z".to_string())),
    );
    let plan: AccessPlan<Value> = AccessPlan::index_range(spec);
    let shape_facts = plan.shape_facts();
    let path = shape_facts
        .single_path_facts()
        .expect("index-range test plan should remain a single access path");
    let range_details = shape_facts
        .single_path_index_range_details()
        .expect("index-range test plan should expose index range details");

    assert_eq!(path.kind(), AccessPathKind::IndexRange);
    assert_eq!(range_details.name(), CAPABILITY_TEST_INDEX.name());
    assert_eq!(range_details.slot_arity(), 1);
    assert_eq!(
        path.index_key_items_for_slot_map()
            .expect("index range should expose slot-map key items")
            .key_items(),
        SemanticIndexKeyItemsRef::Static(IndexKeyItemsRef::Fields(
            &CAPABILITY_TEST_INDEX_FIELDS[..]
        ))
    );
    assert_eq!(path.index_prefix_spec_count(), 0);
    assert!(path.consumes_index_range_spec());
    assert!(shape_facts.all_paths_support_reverse_traversal());
    assert!(shape_facts.has_single_path_index_range_access_path());
    assert_eq!(shape_facts.first_index_range_details(), Some(range_details));
}

#[test]
fn access_shape_facts_keep_branch_tree_closeout_families_distinct() {
    let multi_lookup: AccessPlan<Value> = AccessPlan::index_multi_lookup_from_contract(
        SemanticIndexAccessContract::model_only_from_generated_index(CAPABILITY_TEST_INDEX),
        vec![Value::Nat64(1), Value::Nat64(2)],
    );
    let multi_lookup_facts = multi_lookup.shape_facts();
    let multi_lookup_path = multi_lookup_facts
        .single_path_facts()
        .expect("multi-lookup should remain a single access path");
    let multi_lookup_index = multi_lookup_facts
        .single_path_index_prefix_details()
        .expect("multi-lookup should expose leading-prefix index details");

    assert_eq!(multi_lookup_path.kind(), AccessPathKind::IndexMultiLookup);
    assert_eq!(
        multi_lookup_path.index_prefix_spec_count(),
        2,
        "multi-lookup consumes one exact prefix spec per lookup value",
    );
    assert_eq!(
        multi_lookup_index.slot_arity(),
        1,
        "multi-lookup prefixes target the leading index slot only",
    );
    assert!(multi_lookup_facts.has_selected_index_access_path());

    let branch_set: AccessPlan<Value> = AccessPlan::index_branch_set_from_contract(
        SemanticIndexAccessContract::model_only_from_generated_index(CAPABILITY_TEST_INDEX),
        vec![Value::Nat64(7)],
        vec![
            Value::Text("draft".to_string()),
            Value::Text("review".to_string()),
        ],
    );
    let branch_set_facts = branch_set.shape_facts();
    let branch_set_path = branch_set_facts
        .single_path_facts()
        .expect("branch-set should remain a single access path");
    let branch_set_index = branch_set_facts
        .single_path_index_prefix_details()
        .expect("branch-set should expose composite-prefix index details");

    assert_eq!(branch_set_path.kind(), AccessPathKind::IndexBranchSet);
    assert_eq!(
        branch_set_path.index_prefix_spec_count(),
        2,
        "branch-set consumes one exact prefix spec per branch value",
    );
    assert_eq!(
        branch_set_index.slot_arity(),
        2,
        "branch-set prefixes include fixed leading slots plus the branch slot",
    );
    assert!(branch_set_facts.has_selected_index_access_path());

    let union: AccessPlan<Value> = AccessPlan::union(vec![
        AccessPlan::index_prefix_from_contract(
            SemanticIndexAccessContract::model_only_from_generated_index(CAPABILITY_TEST_INDEX),
            vec![Value::Nat64(1)],
        ),
        AccessPlan::index_prefix_from_contract(
            SemanticIndexAccessContract::model_only_from_generated_index(CAPABILITY_TEST_INDEX),
            vec![Value::Nat64(2)],
        ),
    ]);
    let union_facts = union.shape_facts();

    assert!(union_facts.is_composite());
    assert!(union_facts.single_path_facts().is_none());
    assert!(
        !union_facts.has_selected_index_access_path(),
        "general set-union access must not masquerade as one selected index path",
    );

    let intersection: AccessPlan<Value> = AccessPlan::intersection(vec![
        AccessPlan::index_prefix_from_contract(
            SemanticIndexAccessContract::model_only_from_generated_index(CAPABILITY_TEST_INDEX),
            vec![Value::Nat64(1)],
        ),
        AccessPlan::by_key(Value::Text("primary".to_string())),
    ]);
    let intersection_facts = intersection.shape_facts();

    assert!(intersection_facts.is_composite());
    assert!(intersection_facts.single_path_facts().is_none());
    assert!(
        !intersection_facts.has_selected_index_access_path(),
        "general set-intersection access must not masquerade as one selected index path",
    );
}

#[test]
fn lower_access_multi_lookup_matches_individual_prefix_bounds() {
    let index = SemanticIndexAccessContract::model_only_from_generated_index(CAPABILITY_TEST_INDEX);
    let values = vec![Value::Nat64(1), Value::Nat64(2), Value::Nat64(3)];
    let multi_lookup: AccessPlan<Value> =
        AccessPlan::index_multi_lookup_from_contract(index.clone(), values.clone());
    let lowered_multi_lookup =
        lower_access(EntityTag::new(0xA11C), &multi_lookup).expect("multi-lookup should lower");
    let expected_prefix_specs = values
        .iter()
        .map(|value| {
            let prefix: AccessPlan<Value> =
                AccessPlan::index_prefix_from_contract(index.clone(), vec![value.clone()]);
            lower_access(EntityTag::new(0xA11C), &prefix)
                .expect("single prefix should lower")
                .index_prefix_specs()
                .first()
                .expect("single prefix should emit one lowered spec")
                .clone()
        })
        .collect::<Vec<_>>();

    assert_eq!(
        lowered_multi_lookup.index_prefix_specs(),
        expected_prefix_specs.as_slice(),
        "batched multi-lookup lowering must preserve the exact per-prefix bounds contract",
    );
}

#[test]
fn lower_access_index_prefix_matches_canonical_prefix_bounds() {
    let index = SemanticIndexAccessContract::model_only_from_generated_index(CAPABILITY_TEST_INDEX);
    let values = vec![Value::Nat64(7), Value::Text("alpha".to_string())];
    let plan: AccessPlan<Value> =
        AccessPlan::index_prefix_from_contract(index.clone(), values.clone());
    let lowered = lower_access(EntityTag::new(0xA11C), &plan).expect("prefix should lower");
    let spec = lowered
        .index_prefix_specs()
        .first()
        .expect("index prefix should emit one lowered spec");
    let encoded_values =
        EncodedValue::try_encode_all(values.as_slice()).expect("prefix values should encode");
    let expected = build_index_prefix_bounds_for_encoded_components(
        &IndexId::new(EntityTag::new(0xA11C), index.ordinal()),
        IndexKeyKind::User,
        index.key_arity(),
        encoded_values.as_slice(),
    )
    .expect("canonical prefix bounds should build");

    assert_eq!(spec.lower(), &expected.0);
    assert_eq!(spec.upper(), &expected.1);
}

// Detect raw `AccessPath::` tokens while excluding prefixed identifiers
// such as `ExplainAccessPath::`.
fn contains_raw_access_path_token(source: &str) -> bool {
    let needle = "AccessPath::";
    let mut offset = 0usize;
    while let Some(found) = source[offset..].find(needle) {
        let absolute = offset + found;
        let previous = if absolute == 0 {
            None
        } else {
            source[..absolute].chars().next_back()
        };
        let preceded_by_identifier =
            previous.is_some_and(|ch| ch.is_ascii_alphanumeric() || ch == '_');
        if !preceded_by_identifier {
            return true;
        }

        offset = absolute + needle.len();
    }

    false
}

#[test]
fn runtime_raw_access_path_references_stay_within_access_boundary() {
    let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/db");
    let mut sources = Vec::new();
    collect_rust_sources(source_root.as_path(), &mut sources);
    sources.sort();

    let allowed: BTreeSet<&str> = BTreeSet::from([
        "src/db/access/canonical.rs",
        "src/db/access/execution_contract/mod.rs",
        "src/db/access/lowering.rs",
        "src/db/access/path.rs",
        "src/db/access/plan.rs",
    ]);

    let mut violations = Vec::new();
    for source_path in sources {
        if source_path
            .components()
            .any(|part| part.as_os_str() == "tests")
            || source_path
                .file_name()
                .is_some_and(|name| name == "tests.rs")
        {
            continue;
        }

        let source = fs::read_to_string(&source_path)
            .unwrap_or_else(|err| panic!("failed to read {}: {err}", source_path.display()));
        let runtime_source = runtime_source_without_test_items(source.as_str());
        if !contains_raw_access_path_token(runtime_source.as_str()) {
            continue;
        }

        let relative =
            relative_rust_source_path(Path::new(env!("CARGO_MANIFEST_DIR")), source_path.as_path());

        if !allowed.contains(relative.as_str()) {
            violations.push(relative);
        }
    }

    assert!(
        violations.is_empty(),
        "runtime AccessPath variant references must stay access-boundary-local; violations: {}",
        violations.join(", "),
    );
}
