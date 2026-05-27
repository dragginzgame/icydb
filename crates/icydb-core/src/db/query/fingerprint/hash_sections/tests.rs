use super::profile::{CONTINUATION_STEPS, ExplainHashField, ExplainHashProfile, FINGERPRINT_STEPS};

#[test]
fn fingerprint_v1_profile_excludes_grouping_shape_field() {
    let has_grouping_shape = FINGERPRINT_STEPS
        .iter()
        .any(|step| step.field == ExplainHashField::GroupingShape);

    assert!(
        !has_grouping_shape,
        "Fingerprint must remain semantic and exclude grouped strategy/handoff metadata fields",
    );
}

#[test]
fn continuation_profile_includes_grouping_shape_field() {
    let has_grouping_shape = CONTINUATION_STEPS
        .iter()
        .any(|step| step.field == ExplainHashField::GroupingShape);

    assert!(
        has_grouping_shape,
        "Continuation profile must remain grouped-shape aware for resume compatibility",
    );
}

#[test]
fn fingerprint_v1_profile_projection_slot_is_stable() {
    let projection_slots = FINGERPRINT_STEPS
        .iter()
        .filter(|step| step.field == ExplainHashField::ProjectionSpec)
        .count();

    assert_eq!(
        projection_slots, 1,
        "Fingerprint must keep exactly one projection-semantic hash slot",
    );
}

#[test]
fn continuation_profile_declares_entity_path_contract_slot() {
    let spec = ExplainHashProfile::Continuation {
        entity_path: "tests::Entity",
    }
    .spec();

    assert!(
        spec.entity_path.is_some(),
        "Continuation profile must remain entity-path aware for cursor signature isolation",
    );
}
