//! Module: u256_measurement_contract
//! Responsibility: freeze the bounded 0.248 Ethereum measurement workload.
//! Does not own: production U256 semantics, schema admission, or encoding.
//! Boundary: checked-in measurement fixture -> Patch 1 evidence tooling.

use std::collections::BTreeSet;

use icydb::types::NatBig;
use serde::Deserialize;

const FIXTURE: &str = include_str!("../fixtures/u256/ethereum-workload-v1.json");
const PROFILE: &str = "icydb-u256-ethereum/0.248/v1";
const ROW_COUNT: u32 = 2_048;
const REQUIRED_QUERY_IDS: &[&str] = &[
    "unique_token_equality",
    "indexed_balance_range",
    "descending_balance_page",
    "checked_projection_arithmetic",
    "tuple_distinct",
    "grouped_checked_sum",
    "global_extrema",
    "global_checked_sum",
];
const REQUIRED_SAMPLE_IDS: &[&str] = &[
    "zero",
    "one",
    "balance_64",
    "value_128",
    "token_id_full_width",
    "u256_max",
];

#[derive(Deserialize)]
struct Workload {
    format_version: u32,
    profile: String,
    row_count: u32,
    controls: Controls,
    entity: Entity,
    samples: Vec<Sample>,
    fields: Vec<Field>,
    queries: Vec<Query>,
}

#[derive(Deserialize)]
struct Controls {
    nat_big_max_bytes: u32,
    nat_big_adapter_max_bytes: u32,
    native_u256_bits: u32,
}

#[derive(Deserialize)]
struct Entity {
    name: String,
    primary_key: SchemaField,
    control_fields: Vec<ControlField>,
}

#[derive(Deserialize)]
struct SchemaField {
    name: String,
    r#type: String,
}

#[derive(Deserialize)]
struct ControlField {
    name: String,
    r#type: String,
    round_robin_cardinality: u32,
}

#[derive(Deserialize)]
struct Sample {
    id: String,
    decimal: String,
}

#[derive(Deserialize)]
struct Field {
    name: String,
    r#type: String,
    indexed: bool,
    unique: bool,
    distribution: Vec<Frequency>,
}

#[derive(Deserialize)]
struct Frequency {
    sample: String,
    count: u32,
    #[serde(default)]
    sequence_step: Option<String>,
}

#[derive(Deserialize)]
struct Query {
    id: String,
    sql: String,
}

#[test]
fn ethereum_u256_measurement_fixture_is_complete_and_bounded() {
    let workload: Workload =
        serde_json::from_str(FIXTURE).expect("U256 measurement fixture should decode");

    assert_eq!(workload.format_version, 1);
    assert_eq!(workload.profile, PROFILE);
    assert_eq!(workload.row_count, ROW_COUNT);
    assert_eq!(workload.controls.nat_big_max_bytes, 37);
    assert_eq!(workload.controls.nat_big_adapter_max_bytes, 37);
    assert_eq!(workload.controls.native_u256_bits, 256);
    assert_eq!(workload.entity.name, "EthereumPosition");
    assert_eq!(workload.entity.primary_key.name, "id");
    assert_eq!(workload.entity.primary_key.r#type, "Ulid");
    assert_eq!(workload.entity.control_fields.len(), 1);
    let owner = &workload.entity.control_fields[0];
    assert_eq!(owner.name, "owner");
    assert_eq!(owner.r#type, "Principal");
    assert_eq!(owner.round_robin_cardinality, 64);
    assert_eq!(workload.row_count % owner.round_robin_cardinality, 0);

    let sample_ids = unique_ids(workload.samples.iter().map(|sample| sample.id.as_str()));
    assert_eq!(sample_ids, REQUIRED_SAMPLE_IDS.iter().copied().collect());
    for sample in &workload.samples {
        let value = sample
            .decimal
            .parse::<NatBig>()
            .expect("measurement sample should be an unsigned decimal");
        assert!(
            value.to_leb128().len() <= workload.controls.nat_big_max_bytes as usize,
            "sample {} exceeds the complete constrained NatBig control",
            sample.id
        );
    }
    let maximum = workload
        .samples
        .iter()
        .find(|sample| sample.id == "u256_max")
        .expect("fixture should carry U256::MAX")
        .decimal
        .parse::<NatBig>()
        .expect("U256::MAX should parse");
    assert_eq!(maximum.to_leb128().len(), 37);

    let mut field_ids = BTreeSet::new();
    for field in &workload.fields {
        assert!(field_ids.insert(field.name.as_str()), "duplicate field");
        assert_eq!(field.r#type, "U256");
        assert_eq!(
            field
                .distribution
                .iter()
                .map(|frequency| frequency.count)
                .sum::<u32>(),
            workload.row_count,
            "field {} must freeze one value per row",
            field.name
        );
        for frequency in &field.distribution {
            assert!(sample_ids.contains(frequency.sample.as_str()));
        }
    }
    assert_eq!(
        field_ids,
        ["allowance", "balance", "token_id", "total_supply"].into()
    );
    let token_id = workload
        .fields
        .iter()
        .find(|field| field.name == "token_id")
        .expect("fixture should carry token_id");
    let balance = workload
        .fields
        .iter()
        .find(|field| field.name == "balance")
        .expect("fixture should carry balance");
    assert!(token_id.indexed && token_id.unique);
    assert!(balance.indexed && !balance.unique);
    assert_eq!(token_id.distribution.len(), 1);
    assert_eq!(token_id.distribution[0].sequence_step.as_deref(), Some("1"));
    let token_base = workload
        .samples
        .iter()
        .find(|sample| sample.id == token_id.distribution[0].sample)
        .expect("token sequence base should exist")
        .decimal
        .parse::<NatBig>()
        .expect("token sequence base should parse");
    let token_last = token_base + NatBig::from(u64::from(workload.row_count - 1));
    assert!(token_last <= maximum);
    assert!(
        workload
            .fields
            .iter()
            .filter(|field| field.name != "token_id")
            .flat_map(|field| &field.distribution)
            .all(|frequency| frequency.sequence_step.is_none())
    );

    let query_ids = unique_ids(workload.queries.iter().map(|query| query.id.as_str()));
    assert_eq!(query_ids, REQUIRED_QUERY_IDS.iter().copied().collect());
    assert!(workload.queries.iter().all(|query| !query.sql.is_empty()));
}

fn unique_ids<'a>(ids: impl Iterator<Item = &'a str>) -> BTreeSet<&'a str> {
    let ids: Vec<_> = ids.collect();
    let unique: BTreeSet<_> = ids.iter().copied().collect();
    assert_eq!(unique.len(), ids.len(), "measurement IDs must be unique");
    unique
}
