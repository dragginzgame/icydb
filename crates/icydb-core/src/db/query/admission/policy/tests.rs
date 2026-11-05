use super::QueryAdmissionPolicy;
use crate::db::query::admission::input::{
    MAX_QUERY_INPUT_BYTES, MAX_QUERY_INPUT_DEPTH, MAX_QUERY_INPUT_NODES,
};
use std::{collections::BTreeMap, num::NonZeroU32};

#[test]
fn documentation_resource_limits_match_compiled_owners() {
    // Read checkout data at test time; library builds do not embed docs.
    // Only keys and numbers are contractual, not table order or prose.
    let document = std::fs::read_to_string(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/../../docs/contracts/RESOURCE_MODEL.md"
    ))
    .expect("repository resource model");
    let section = document
        .split_once("<!-- icydb-read-resource-limits:start -->")
        .expect("resource data start")
        .1
        .split_once("<!-- icydb-read-resource-limits:end -->")
        .expect("resource data end")
        .0;
    let mut documented = BTreeMap::new();
    for line in section.lines() {
        let mut cells = line.split('|').skip(1).map(str::trim);
        let Some(key) = cells
            .next()
            .and_then(|cell| cell.strip_prefix('`'))
            .and_then(|cell| cell.strip_suffix('`'))
        else {
            continue;
        };
        let value = cells
            .next()
            .expect("numeric data cell")
            .parse::<u64>()
            .expect("numeric resource limit");
        assert!(documented.insert(key, value).is_none(), "duplicate limit");
    }
    let policy = QueryAdmissionPolicy::default_bounded_read();
    let value = |limit: Option<NonZeroU32>| u64::from(limit.expect("bounded policy").get());
    let expected = BTreeMap::from([
        ("max_returned_rows", value(policy.max_returned_rows)),
        (
            "max_primary_key_input_terms",
            value(policy.max_primary_key_input_terms),
        ),
        (
            "max_primary_key_input_bytes",
            value(policy.max_primary_key_input_bytes),
        ),
        ("max_groups", value(policy.grouped.max_groups())),
        ("max_group_bytes", value(policy.grouped.max_group_bytes())),
        (
            "max_input_depth",
            u64::try_from(MAX_QUERY_INPUT_DEPTH).expect("input depth"),
        ),
        (
            "max_input_nodes",
            u64::try_from(MAX_QUERY_INPUT_NODES).expect("input nodes"),
        ),
        (
            "max_input_bytes",
            u64::try_from(MAX_QUERY_INPUT_BYTES).expect("input bytes"),
        ),
    ]);
    assert_eq!(documented, expected);
}

#[test]
fn public_read_keeps_bounded_access_requirements() {
    let policy = QueryAdmissionPolicy::public_read(NonZeroU32::MIN);

    assert!(policy.require_limit());
    assert!(policy.require_index());
    assert!(!policy.allow_full_scan());
    assert!(!policy.allow_materialized_sort());
}

#[cfg(feature = "sql")]
#[test]
fn diagnostic_explain_keeps_non_executing_access_permissions() {
    let policy = QueryAdmissionPolicy::diagnostic_explain();

    assert!(!policy.require_limit());
    assert!(!policy.require_index());
    assert!(policy.allow_full_scan());
    assert!(policy.allow_materialized_sort());
}
