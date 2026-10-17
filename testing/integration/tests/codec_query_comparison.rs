//! Installed codec-only comparison; neither actor links the database or CDK.

use std::{env, fs, path::PathBuf};

use canister_audit_codec_wire_query::CountRows;
use ic_host_artifacts::wasm::{InspectionLimits, inspect};
use ic_testkit::pic::{InstallSpec, StandaloneCanisterFixture};
use ic_wire::{Method, decode_response, encode_request, encode_response};
use icydb_testing_build::{
    CanisterWasmProfile, build_fixture_variant_wasm_with_profile,
    canister_artifact::{MethodMode, inspect_wasm_methods},
    wasm_optimizer::{POST_LINK_PIPELINE_IDENTITY, WASM_OPT_FLAGS, pinned_wasm_optimizer},
};
use icydb_testing_codec_query::MAX_FRAME_BYTES;
use icydb_testing_integration::start_fixture_pocket_ic;
use serde_json::{Value, json};

fn artifact_facts(wasm: &[u8]) -> Value {
    let methods = inspect_wasm_methods(wasm).unwrap();
    assert_eq!(methods.len(), 1);
    let method = methods.first().unwrap();
    assert_eq!(method.name, CountRows::NAME);
    assert_eq!(method.mode, MethodMode::Query);
    let facts = inspect(
        wasm,
        InspectionLimits {
            module_bytes: wasm.len(),
            sections: wasm.len(),
            exports: 128,
            custom_sections: 128,
        },
    )
    .unwrap();
    assert!(!facts.exports.contains_key("get_candid_pointer"));
    json!({
        "raw_wasm_bytes": wasm.len(), "code_section_bytes": facts.code_section_bytes,
        "defined_functions": facts.defined_functions,
        "blake3": blake3::hash(wasm).to_hex().to_string(),
    })
}

fn install(label: &str, wasm: Vec<u8>) -> StandaloneCanisterFixture {
    // Neither actor exports init; no Candid argument encoder participates.
    StandaloneCanisterFixture::try_install(
        start_fixture_pocket_ic(),
        InstallSpec {
            wasm,
            init_bytes: Vec::new(),
            cycles: 100_000_000_000_000,
            install_sender: None,
            label: Some(label.into()),
        },
    )
    .unwrap()
}

fn query(fixture: &StandaloneCanisterFixture, request: Vec<u8>) -> Vec<u8> {
    fixture
        .pocket_ic()
        .query_call(
            fixture.canister_id(),
            candid::Principal::anonymous(),
            CountRows::NAME,
            request,
        )
        .expect("raw codec query should succeed")
}

fn rejected_requests(fixture: &StandaloneCanisterFixture, request: &[u8], other: Vec<u8>) -> usize {
    let mut invalid = (0..request.len())
        .map(|length| request[..length].to_vec())
        .collect::<Vec<_>>();
    let mut trailing = request.to_vec();
    trailing.push(0);
    invalid.extend([trailing, vec![0; MAX_FRAME_BYTES + 1], other]);
    let count = invalid.len();
    for bytes in invalid {
        assert!(
            fixture
                .pocket_ic()
                .query_call(
                    fixture.canister_id(),
                    candid::Principal::anonymous(),
                    CountRows::NAME,
                    bytes,
                )
                .is_err()
        );
    }
    count
}

fn verified_vectors(
    candid_actor: &StandaloneCanisterFixture,
    wire_actor: &StandaloneCanisterFixture,
) -> Vec<Value> {
    let mut vectors = Vec::new();
    for (key, expected) in [(0, 1_u32), (7, 2), (8, 0), (u64::MAX, 1)] {
        let candid_request = candid::encode_one(key).unwrap();
        let wire_request = encode_request::<CountRows>(&key).unwrap().into_bytes();
        let candid_reply = query(candid_actor, candid_request.clone());
        let wire_reply = query(wire_actor, wire_request.clone());
        assert_eq!(candid::decode_one::<u32>(&candid_reply).unwrap(), expected);
        assert_eq!(decode_response::<CountRows>(&wire_reply).unwrap(), expected);
        assert_eq!(candid_reply, candid::encode_one(expected).unwrap());
        assert_eq!(
            wire_reply,
            encode_response::<CountRows>(&expected).unwrap().as_bytes()
        );
        vectors.push(json!({"key": key, "rows": expected,
            "candid_request": candid_request, "candid_reply": candid_reply,
            "wire_request": wire_request, "wire_reply": wire_reply,
        }));
    }
    vectors
}

#[test]
fn codec_only_actors_preserve_counts_and_record_matched_wasm() {
    let directory = env::var_os("CARGO_TARGET_DIR")
        .map_or_else(
            || PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("../../target"),
            PathBuf::from,
        )
        .join("codec-only-comparison");
    fs::create_dir_all(&directory).unwrap();
    let candid = build_fixture_variant_wasm_with_profile(
        "canister_audit_codec_candid_query",
        "",
        "codec-candid-query",
        CanisterWasmProfile::WasmRelease,
    )
    .unwrap();
    let wire = build_fixture_variant_wasm_with_profile(
        "canister_audit_codec_wire_query",
        "",
        "codec-wire-query",
        CanisterWasmProfile::WasmRelease,
    )
    .unwrap();
    fs::write(directory.join("candid.wasm"), &candid).unwrap();
    fs::write(directory.join("wire.wasm"), &wire).unwrap();
    let candid_facts = artifact_facts(&candid);
    let wire_facts = artifact_facts(&wire);
    let candid_actor = install("codec-candid-query", candid);
    let wire_actor = install("codec-wire-query", wire);
    let vectors = verified_vectors(&candid_actor, &wire_actor);
    let candid_request = candid::encode_one(7_u64).unwrap();
    let wire_request = encode_request::<CountRows>(&7).unwrap().into_bytes();
    let candid_rejections = rejected_requests(&candid_actor, &candid_request, wire_request.clone());
    let wire_rejections = rejected_requests(&wire_actor, &wire_request, candid_request.clone());
    assert_eq!(
        candid::decode_one::<u32>(&query(&candid_actor, candid_request)).unwrap(),
        2
    );
    assert_eq!(
        decode_response::<CountRows>(&query(&wire_actor, wire_request)).unwrap(),
        2
    );
    let optimizer = pinned_wasm_optimizer().unwrap();
    let report = json!({"scope": "codec-only; no database or CDK",
        "profile": "wasm-release", "features": [],
        "pipeline": POST_LINK_PIPELINE_IDENTITY, "optimizer_flags": WASM_OPT_FLAGS,
        "optimizer_sha256": optimizer.identity().sha256.to_string(),
        "candid": candid_facts, "wire": wire_facts, "vectors": vectors,
        "candid_rejections": candid_rejections, "wire_rejections": wire_rejections,
        "cycles_delta": null, "instructions_delta": null,
    });
    fs::write(
        directory.join("report.json"),
        serde_json::to_vec_pretty(&report).unwrap(),
    )
    .unwrap();
    println!("codec-only comparison: {report}");
}
