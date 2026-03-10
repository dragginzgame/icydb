//! Module: db::query::fingerprint::tests
//! Responsibility: module-local ownership and contracts for db::query::fingerprint::tests.
//! Does not own: cross-module orchestration outside this module.
//! Boundary: exposes this module API while keeping implementation details internal.

use crate::db::query::fingerprint::{
    new_continuation_signature_hasher_v1, new_plan_fingerprint_hasher_v2,
};
use sha2::{Digest, Sha256};

#[test]
fn plan_fingerprint_hasher_profile_seed_matches_manual_contract() {
    let mut helper = new_plan_fingerprint_hasher_v2();
    helper.update(b"payload");

    let mut manual = Sha256::new();
    manual.update(b"planfp:v2");
    manual.update(b"payload");

    assert_eq!(helper.finalize(), manual.finalize());
}

#[test]
fn continuation_signature_hasher_profile_seed_matches_manual_contract() {
    let mut helper = new_continuation_signature_hasher_v1();
    helper.update(b"payload");

    let mut manual = Sha256::new();
    manual.update(b"contsig:v1");
    manual.update(b"payload");

    assert_eq!(helper.finalize(), manual.finalize());
}
