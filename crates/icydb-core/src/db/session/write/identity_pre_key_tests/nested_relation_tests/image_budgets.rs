//! Large admitted relation images remain replaceable within reverse-delta limits.

use super::*;

#[test]
fn changed_high_fanout_images_remain_updatable_and_validate_final_targets() {
    let session = initialize_nested();
    for id in [1, 2] {
        session
            .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
                entity: ENTITY_NAME.into(),
                patch: dynamic_payload_patch(id),
            })
            .unwrap();
    }
    session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Insert {
            entity: SECOND_ENTITY_NAME.into(),
            patch: nested_patch(&vec![1; 1600]),
        })
        .unwrap();
    drive_journaled_recovery_to_completion(&session);

    // Both images hold 3,201 references (list/map duplicates and one set
    // member). Only six reverse deltas are needed to change all three fields.
    for (target, mutation) in [
        (
            2,
            DynamicMutation::Update {
                entity: SECOND_ENTITY_NAME.into(),
                key: InputValue::nat64(1),
                patch: nested_patch(&vec![2; 1600]),
            },
        ),
        (
            1,
            DynamicMutation::Replace {
                entity: SECOND_ENTITY_NAME.into(),
                key: InputValue::nat64(1),
                patch: nested_patch(&vec![1; 1600]),
            },
        ),
    ] {
        session.execute_trusted_dynamic_mutation(&mutation).unwrap();
        assert_nested_state(&session, &vec![target; 1600]);
        let before = source_rows(&session);
        let reverse = reverse_entries();
        assert_eq!(reverse.len(), 3);
        drive_journaled_recovery_to_completion(&session);
        assert_eq!(source_rows(&session), before);
        assert_eq!(reverse_entries(), reverse);
    }
    let before = source_rows(&session);
    let reverse = reverse_entries();
    let error = session
        .execute_trusted_dynamic_mutation(&DynamicMutation::Update {
            entity: SECOND_ENTITY_NAME.into(),
            key: InputValue::nat64(1),
            patch: nested_patch(&vec![3; 1600]),
        })
        .unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        icydb_diagnostic_code::DiagnosticFactTag::ConstraintKind,
        icydb_diagnostic_code::DiagnosticConstraintKind::Relation.raw(),
    )));
    assert_eq!(source_rows(&session), before);
    assert_eq!(reverse_entries(), reverse);
}
