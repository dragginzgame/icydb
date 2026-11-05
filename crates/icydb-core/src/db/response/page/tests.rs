use super::*;

#[derive(serde::Serialize)]
struct FrozenScalarPageWorkWire {
    envelope_identity: u64,
    entries_visited: u64,
    result_rows: u32,
}

#[derive(serde::Serialize)]
struct FrozenLiveQueryPageOutputWire {
    entity: String,
    columns: Vec<String>,
    rows: Vec<Vec<OutputValue>>,
    row_count: u32,
    continuation: Option<String>,
    work: FrozenScalarPageWorkWire,
}

#[derive(serde::Serialize)]
struct FrozenReadSetStoreIdentityWire([u8; 32]);

#[derive(serde::Serialize)]
struct FrozenReadSetStoreRevisionWire {
    store: FrozenReadSetStoreIdentityWire,
    data_revision: u64,
    access_state_revision: u64,
}

#[derive(serde::Serialize)]
struct FrozenReadSetRevisionProofWire {
    database_incarnation: [u8; 16],
    accepted_root_revision: u64,
    accepted_root_fingerprint_method: u8,
    accepted_root_fingerprint: [u8; 32],
    stores: Vec<FrozenReadSetStoreRevisionWire>,
}

#[derive(serde::Serialize)]
struct FrozenExhaustiveQueryPageOutputWire {
    entity: String,
    columns: Vec<String>,
    rows: Vec<Vec<OutputValue>>,
    row_count: u32,
    continuation: Option<String>,
    work: FrozenScalarPageWorkWire,
    proof: FrozenReadSetRevisionProofWire,
}

#[test]
fn live_query_page_output_preserves_its_initial_wire_record_shape() {
    let current = LiveQueryPageOutput {
        entity: "example".to_string(),
        columns: vec!["id".to_string()],
        rows: vec![vec![OutputValue::nat64(7)]],
        row_count: 1,
        continuation: Some("opaque".to_string()),
        work: ScalarPageWork {
            envelope_identity: 11,
            entries_visited: 2,
            result_rows: 1,
        },
    };
    let frozen = FrozenLiveQueryPageOutputWire {
        entity: current.entity.clone(),
        columns: current.columns.clone(),
        rows: current.rows.clone(),
        row_count: current.row_count,
        continuation: current.continuation.clone(),
        work: FrozenScalarPageWorkWire {
            envelope_identity: current.work.envelope_identity,
            entries_visited: current.work.entries_visited,
            result_rows: current.work.result_rows,
        },
    };

    assert_eq!(current.len(), 1);
    assert!(!current.is_empty());

    assert_eq!(
        icydb_schema::wire::encode_one(&current).expect("current live page should encode"),
        icydb_schema::wire::encode_one(&frozen).expect("frozen live page should encode"),
    );
}

#[test]
fn exhaustive_query_page_output_freezes_its_initial_wire_record_shape() {
    let proof = ReadSetRevisionProof::from_parts(
        [1; 16],
        7,
        1,
        [2; 32],
        vec![crate::db::ReadSetStoreRevision::new(
            crate::db::ReadSetStoreIdentity::from_bytes([3; 32]),
            11,
            13,
        )],
    )
    .expect("bounded canonical proof should admit");
    let current = ExhaustiveQueryPageOutput {
        entity: "example".to_string(),
        columns: vec!["id".to_string()],
        rows: vec![vec![OutputValue::nat64(7)]],
        row_count: 1,
        continuation: Some("opaque".to_string()),
        work: ScalarPageWork {
            envelope_identity: 11,
            entries_visited: 2,
            result_rows: 1,
        },
        proof,
    };
    let frozen = FrozenExhaustiveQueryPageOutputWire {
        entity: current.entity.clone(),
        columns: current.columns.clone(),
        rows: current.rows.clone(),
        row_count: current.row_count,
        continuation: current.continuation.clone(),
        work: FrozenScalarPageWorkWire {
            envelope_identity: current.work.envelope_identity,
            entries_visited: current.work.entries_visited,
            result_rows: current.work.result_rows,
        },
        proof: FrozenReadSetRevisionProofWire {
            database_incarnation: current.proof.database_incarnation(),
            accepted_root_revision: current.proof.accepted_root_revision(),
            accepted_root_fingerprint_method: current.proof.accepted_root_fingerprint_method(),
            accepted_root_fingerprint: current.proof.accepted_root_fingerprint(),
            stores: current
                .proof
                .stores()
                .iter()
                .map(|store| FrozenReadSetStoreRevisionWire {
                    store: FrozenReadSetStoreIdentityWire(store.store().to_bytes()),
                    data_revision: store.data_revision(),
                    access_state_revision: store.access_state_revision(),
                })
                .collect(),
        },
    };

    assert_eq!(current.len(), 1);
    assert!(!current.is_empty());

    assert_eq!(
        icydb_schema::wire::encode_one(&current).expect("current exhaustive page should encode"),
        icydb_schema::wire::encode_one(&frozen).expect("frozen exhaustive page should encode"),
    );
}
