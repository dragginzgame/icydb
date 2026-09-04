//! Frozen 0.253 relation-cost limits and proving-fixture identities.
//!
//! This module owns measurement and fixture authority only. Production runtime
//! enforcement adopts these constants in the later implementation slices.

/// Published predecessor used for every 0.253 comparison.
pub const PREDECESSOR_TAG: &str = "v0.252.11";
/// Exact published predecessor source revision.
pub const PREDECESSOR_REVISION: &str = "d497f60738a7e1e103298e9817185b9998be3bb1";
/// Exact published predecessor Git tree.
pub const PREDECESSOR_TREE: &str = "390e70caed81940ca13fd01731930d8bb06b93e7";
/// Exact published predecessor lockfile digest.
pub const PREDECESSOR_LOCK_SHA256: &str =
    "b776ec36c5fffa437ca5710fe131a55c023d2a20f44f6592f91bfe36154eab16";
/// Rust compiler used for the frozen opening artifacts.
pub const PREDECESSOR_RUSTC: &str = "rustc 1.97.1 (8bab26f4f 2026-07-14)";

/// Maximum explicit accepted steps below one relation root field.
pub const MAX_NESTED_RELATION_PATH_STEPS: u64 = 64;
/// Maximum logical path-instruction evaluations for one old or new row image.
pub const MAX_NESTED_RELATION_IMAGE_TRAVERSAL_WORK: u64 = 349_440;
/// Maximum scalar terminal occurrences emitted by one old or new row image.
pub const MAX_NESTED_RELATION_IMAGE_RAW_REFERENCES: u64 = 5_460;
/// Maximum logical path-instruction evaluations across one atomic batch.
pub const MAX_RELATION_BATCH_TRAVERSAL_WORK: u64 = 349_440;
/// Maximum old/new scalar terminal occurrences across one atomic batch.
pub const MAX_RELATION_BATCH_RAW_REFERENCES: u64 = 5_460;
/// Maximum distinct final target checks across one atomic batch.
pub const MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS: u64 = 3_276;
/// Maximum coalesced physical reverse inserts and deletes across one atomic batch.
pub const MAX_RELATION_BATCH_REVERSE_DELTAS: u64 = 5_460;

/// Rows admitted by the existing one-reverse-delta commit-work boundary.
pub const DIRECT_INSERT_CALIBRATION_ROWS: u16 = 3_276;
/// Rows admitted by the existing two-reverse-delta replacement boundary.
pub const DIRECT_REPLACE_CALIBRATION_ROWS: u16 = 2_730;
/// IC update ceiling used by the calibration contract.
pub const IC_UPDATE_INSTRUCTION_LIMIT: u64 = 40_000_000_000;
/// Instructions reserved from every admitted maximum for surrounding work.
pub const REQUIRED_INSTRUCTION_HEADROOM: u64 = IC_UPDATE_INSTRUCTION_LIMIT / 4;
/// Largest measured stable-tree batch in the frozen calibration run.
pub const MEASURED_MAXIMUM_BATCH_INSTRUCTIONS: u64 = 4_928_211_587;

/// One maintained controlled actor in the nested-relation measurement matrix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationProvingActor {
    /// Maintained build-policy name.
    pub name: &'static str,
    /// Direct relations present in the opening schema.
    pub direct_relations: u8,
    /// Nested relations introduced when this actor becomes the candidate.
    pub planned_nested_relations: u8,
}

/// Complete approved proving-actor manifest.
pub const RELATION_PROVING_ACTORS: &[RelationProvingActor] = &[
    RelationProvingActor {
        name: "nested_relation_none",
        direct_relations: 0,
        planned_nested_relations: 0,
    },
    RelationProvingActor {
        name: "nested_relation_direct",
        direct_relations: 1,
        planned_nested_relations: 0,
    },
    RelationProvingActor {
        name: "nested_relation_shallow",
        direct_relations: 1,
        planned_nested_relations: 3,
    },
    RelationProvingActor {
        name: "nested_relation_repeated",
        direct_relations: 1,
        planned_nested_relations: 3,
    },
];

/// One exact current-form relation identity and accepted path-program fixture.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct RelationPathFixture {
    /// Proving actor that owns the entity-local relation identity.
    pub actor: &'static str,
    /// Monotonic entity-local relation identity expected after fresh creation.
    pub relation_id: u32,
    /// Human-readable rooted source path used only by fixture diagnostics.
    pub display_path: &'static str,
    /// Exact accepted path instructions after the root field.
    pub steps: &'static [&'static str],
}

const DIRECT_STEPS: &[&str] = &[];
const SHALLOW_CHOICE_STEPS: &[&str] = &[
    "OptionalSome",
    "EnterNamed(RelationCostWrapper)",
    "RecordMember(choice)",
    "EnterNamed(RelationCostChoice)",
    "EnumVariantPayload(Target)",
];
const SHALLOW_OPTIONAL_STEPS: &[&str] = &[
    "OptionalSome",
    "EnterNamed(RelationCostWrapper)",
    "RecordMember(optional_target_id)",
    "OptionalSome",
];
const SHALLOW_REQUIRED_STEPS: &[&str] = &[
    "OptionalSome",
    "EnterNamed(RelationCostWrapper)",
    "RecordMember(required_target_id)",
];
const REPEATED_LIST_STEPS: &[&str] = &[
    "OptionalSome",
    "EnterNamed(RelationCostTargetList)",
    "ListItems",
];
const REPEATED_MAP_STEPS: &[&str] = &[
    "OptionalSome",
    "EnterNamed(RelationCostTargetMap)",
    "MapValues",
];
const REPEATED_SET_STEPS: &[&str] = &[
    "OptionalSome",
    "EnterNamed(RelationCostTargetSet)",
    "SetItems",
];

/// Exact entity-local identities and path programs expected from fresh schemas.
pub const RELATION_PATH_FIXTURES: &[RelationPathFixture] = &[
    RelationPathFixture {
        actor: "nested_relation_shallow",
        relation_id: 1,
        display_path: "target_id",
        steps: DIRECT_STEPS,
    },
    RelationPathFixture {
        actor: "nested_relation_shallow",
        relation_id: 2,
        display_path: "wrapper.choice#Target",
        steps: SHALLOW_CHOICE_STEPS,
    },
    RelationPathFixture {
        actor: "nested_relation_shallow",
        relation_id: 3,
        display_path: "wrapper.optional_target_id",
        steps: SHALLOW_OPTIONAL_STEPS,
    },
    RelationPathFixture {
        actor: "nested_relation_shallow",
        relation_id: 4,
        display_path: "wrapper.required_target_id",
        steps: SHALLOW_REQUIRED_STEPS,
    },
    RelationPathFixture {
        actor: "nested_relation_repeated",
        relation_id: 1,
        display_path: "target_id",
        steps: DIRECT_STEPS,
    },
    RelationPathFixture {
        actor: "nested_relation_repeated",
        relation_id: 2,
        display_path: "target_list[]",
        steps: REPEATED_LIST_STEPS,
    },
    RelationPathFixture {
        actor: "nested_relation_repeated",
        relation_id: 3,
        display_path: "target_map{value}",
        steps: REPEATED_MAP_STEPS,
    },
    RelationPathFixture {
        actor: "nested_relation_repeated",
        relation_id: 4,
        display_path: "target_set{}",
        steps: REPEATED_SET_STEPS,
    },
];

/// Validate the frozen fixture and numerical contract.
///
/// # Errors
///
/// Returns a stable diagnostic when the manifest or its derivation drifts.
pub fn validate_nested_relation_contract() -> Result<(), &'static str> {
    if MAX_NESTED_RELATION_PATH_STEPS != 64
        || MAX_NESTED_RELATION_IMAGE_TRAVERSAL_WORK
            != MAX_NESTED_RELATION_PATH_STEPS * MAX_NESTED_RELATION_IMAGE_RAW_REFERENCES
        || MAX_RELATION_BATCH_TRAVERSAL_WORK
            != MAX_NESTED_RELATION_PATH_STEPS * MAX_RELATION_BATCH_RAW_REFERENCES
        || MAX_RELATION_BATCH_RAW_REFERENCES != u64::from(DIRECT_REPLACE_CALIBRATION_ROWS) * 2
        || MAX_RELATION_BATCH_REVERSE_DELTAS != u64::from(DIRECT_REPLACE_CALIBRATION_ROWS) * 2
    {
        return Err("nested relation limits drifted from the measured derivation");
    }
    if MEASURED_MAXIMUM_BATCH_INSTRUCTIONS
        > IC_UPDATE_INSTRUCTION_LIMIT - REQUIRED_INSTRUCTION_HEADROOM
    {
        return Err("nested relation calibration lost its required instruction headroom");
    }
    if RELATION_PROVING_ACTORS.len() != 4
        || RELATION_PROVING_ACTORS
            .iter()
            .enumerate()
            .any(|(index, actor)| {
                actor.name.is_empty()
                    || RELATION_PROVING_ACTORS[index + 1..]
                        .iter()
                        .any(|later| later.name == actor.name)
            })
    {
        return Err("nested relation proving-actor manifest is incomplete or duplicated");
    }
    if RELATION_PATH_FIXTURES.iter().any(|fixture| {
        fixture.relation_id == 0 || fixture.display_path.is_empty() || fixture.steps.len() > 64
    }) {
        return Err("nested relation identity/path fixture is invalid");
    }
    for actor in ["nested_relation_shallow", "nested_relation_repeated"] {
        let identities = RELATION_PATH_FIXTURES
            .iter()
            .filter(|fixture| fixture.actor == actor)
            .map(|fixture| fixture.relation_id)
            .collect::<Vec<_>>();
        if identities != [1, 2, 3, 4] {
            return Err("nested relation fixture identities must be canonical and monotonic");
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn frozen_nested_relation_contract_is_complete() {
        validate_nested_relation_contract().expect("frozen 0.253 contract should validate");
        for actor in RELATION_PROVING_ACTORS {
            assert!(crate::wasm_measurement::WASM_MEASUREMENT_SUBJECTS.contains(&actor.name));
            assert!(
                crate::canister_artifact::MAINTAINED_CANISTER_POLICIES
                    .iter()
                    .any(|policy| policy.canister == actor.name)
            );
        }
        assert_eq!(MAX_RELATION_BATCH_UNIQUE_TARGET_LOOKUPS, 3_276);
        assert_eq!(MAX_RELATION_BATCH_REVERSE_DELTAS, 5_460);
        assert_eq!(MAX_RELATION_BATCH_TRAVERSAL_WORK, 349_440);
        assert_eq!(REQUIRED_INSTRUCTION_HEADROOM, 10_000_000_000);
    }
}
