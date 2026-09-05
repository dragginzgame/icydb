//! One shared operation harness for all controlled relation-cost actors.

use icydb::{
    db::{DbSession, StructuralMutation, StructuralPatch, WriteCell},
    traits::CanisterKind,
    value::InputValue,
};

const TARGET: &str = "RelationCostTarget";
const SOURCE: &str = "RelationCostSource";

fn row_patch(id: i32) -> StructuralPatch {
    StructuralPatch::new().field("id", WriteCell::Value(InputValue::from(id)))
}

fn source_patch(id: i32, target_id: i32) -> StructuralPatch {
    row_patch(id).field("target_id", WriteCell::Value(InputValue::from(target_id)))
}

fn shallow_wrapper(required: i32, optional: Option<i32>, choice: Option<i32>) -> InputValue {
    let choice = match choice {
        Some(target) => InputValue::loose_enum("Target")
            .with_enum_payload(InputValue::from(target))
            .unwrap_or_else(InputValue::null),
        None => InputValue::loose_enum("Absent"),
    };
    InputValue::map(vec![
        (
            InputValue::text("required_target_id".to_string()),
            InputValue::from(required),
        ),
        (
            InputValue::text("optional_target_id".to_string()),
            optional.map_or_else(InputValue::null, InputValue::from),
        ),
        (InputValue::text("choice".to_string()), choice),
    ])
}

fn shallow_source_patch(
    id: i32,
    direct: i32,
    required: i32,
    optional: Option<i32>,
    choice: Option<i32>,
) -> StructuralPatch {
    source_patch(id, direct).field(
        "wrapper",
        WriteCell::Value(shallow_wrapper(required, optional, choice)),
    )
}

fn repeated_source_patch(
    id: i32,
    direct: i32,
    list: &[i32],
    set: &[i32],
    map: &[(u32, i32)],
) -> StructuralPatch {
    source_patch(id, direct)
        .field(
            "target_list",
            WriteCell::Value(InputValue::list(
                list.iter().copied().map(InputValue::from).collect(),
            )),
        )
        .field(
            "target_set",
            WriteCell::Value(InputValue::list(
                set.iter().copied().map(InputValue::from).collect(),
            )),
        )
        .field(
            "target_map",
            WriteCell::Value(InputValue::map(
                map.iter()
                    .map(|(key, value)| {
                        (
                            InputValue::from(*key),
                            InputValue::list(vec![InputValue::from(*value)]),
                        )
                    })
                    .collect(),
            )),
        )
}

fn insert(entity: &str, patch: StructuralPatch) -> StructuralMutation {
    StructuralMutation::Insert {
        entity: entity.to_string(),
        patch,
    }
}

fn delete(entity: &str, id: i32) -> StructuralMutation {
    StructuralMutation::Delete {
        entity: entity.to_string(),
        key: InputValue::from(id),
    }
}

fn measured<T>(operation: impl FnOnce() -> T) -> (T, u64) {
    let start = ic_cdk::api::performance_counter(1);
    let result = operation();
    let instructions = ic_cdk::api::performance_counter(1).saturating_sub(start);
    (result, instructions)
}

/// Measure one data-driven seven-instruction-shape traversal loop.
#[must_use]
pub fn measure_traversal(path_steps: u8, references: u16) -> (u64, u64) {
    measured(|| {
        let mut checksum = 1_u64;
        for reference in 0..references {
            for step in 0..path_steps {
                let input = u64::from(reference)
                    .wrapping_mul(67)
                    .wrapping_add(u64::from(step));
                checksum = match step % 7 {
                    0 => checksum.wrapping_add(input),
                    1 => checksum.rotate_left(3) ^ input,
                    2 => checksum.wrapping_mul(31).wrapping_add(input),
                    3 => checksum.rotate_right(5).wrapping_add(input),
                    4 => checksum ^ input.wrapping_mul(17),
                    5 => checksum.wrapping_sub(input).rotate_left(7),
                    _ => checksum.wrapping_add(input ^ checksum.rotate_right(11)),
                };
            }
        }
        checksum
    })
}

fn batch_insert_targets(base: i32, count: u16) -> Vec<StructuralMutation> {
    (0..count)
        .map(|offset| insert(TARGET, row_patch(base + i32::from(offset))))
        .collect()
}

fn batch_insert_sources(base: i32, count: u16) -> Vec<StructuralMutation> {
    (0..count)
        .map(|offset| {
            let id = base + i32::from(offset);
            insert(SOURCE, source_patch(id, id))
        })
        .collect()
}

fn batch_delete(base: i32, count: u16, entity: &str) -> Vec<StructuralMutation> {
    (0..count)
        .map(|offset| delete(entity, base + i32::from(offset)))
        .collect()
}

fn batch_replace_source_targets(base: i32, count: u16) -> Vec<StructuralMutation> {
    (0..count)
        .map(|offset| {
            let id = base + i32::from(offset);
            StructuralMutation::Update {
                entity: SOURCE.to_string(),
                key: InputValue::from(id),
                patch: StructuralPatch::new().field(
                    "target_id",
                    WriteCell::Value(InputValue::from(id + 100_000)),
                ),
            }
        })
        .collect()
}

/// Measure one direct stable-tree calibration phase.
pub fn calibrate<C: CanisterKind>(
    session: &DbSession<C>,
    base: i32,
    count: u16,
    phase: u8,
) -> Result<u64, icydb::Error> {
    let (result, instructions) = measured(|| match phase {
        0 => session
            .execute_trusted_structural_mutation_batch(batch_insert_targets(base, count))
            .map(|_| ()),
        1 => session
            .execute_trusted_structural_mutation_batch(batch_insert_sources(base, count))
            .map(|_| ()),
        2 => session
            .execute_trusted_structural_mutation_batch(batch_replace_source_targets(base, count))
            .map(|_| ()),
        3 => session
            .execute_trusted_structural_mutation_batch(batch_delete(base, count, SOURCE))
            .map(|_| ()),
        4 => session
            .execute_trusted_structural_mutation_batch(batch_delete(base, count, TARGET))
            .map(|_| ()),
        5 => {
            let _miss = session
                .execute_trusted_structural_mutation(insert(SOURCE, source_patch(base, i32::MIN)));
            Ok(())
        }
        6 => {
            let _restricted = session.execute_trusted_structural_mutation(delete(TARGET, 0));
            Ok(())
        }
        _ => Ok(()),
    });
    result?;
    Ok(instructions)
}

/// Exercise direct insert, replace, delete, RESTRICT, and batch-overlay paths.
#[must_use]
pub fn exercise<C: CanisterKind>(session: &DbSession<C>, operation: u8) -> (bool, u64) {
    measured(|| match operation {
        0 => session
            .execute_trusted_structural_mutation_batch(vec![
                insert(TARGET, row_patch(10_000)),
                insert(TARGET, row_patch(10_001)),
            ])
            .is_ok(),
        1 => session
            .execute_trusted_structural_mutation(insert(SOURCE, source_patch(10_000, 10_000)))
            .is_ok(),
        2 => session
            .execute_trusted_structural_mutation(StructuralMutation::Update {
                entity: SOURCE.to_string(),
                key: InputValue::from(10_000_i32),
                patch: StructuralPatch::new()
                    .field("target_id", WriteCell::Value(InputValue::from(10_001_i32))),
            })
            .is_ok(),
        3 => session
            .execute_trusted_structural_mutation(delete(TARGET, 10_001))
            .is_err(),
        4 => session
            .execute_trusted_structural_mutation(delete(SOURCE, 10_000))
            .is_ok(),
        5 => session
            .execute_trusted_structural_mutation_batch(vec![
                insert(TARGET, row_patch(10_002)),
                insert(SOURCE, source_patch(10_002, 10_002)),
            ])
            .is_ok(),
        6 => session
            .execute_trusted_structural_mutation_batch(vec![
                delete(TARGET, 10_002),
                delete(SOURCE, 10_002),
            ])
            .is_ok(),
        _ => false,
    })
}

/// Exercise every single-valued nested source through ordinary and batch commits.
#[must_use]
pub fn exercise_shallow<C: CanisterKind>(session: &DbSession<C>, operation: u8) -> (bool, u64) {
    measured(|| match operation {
        0 => session
            .execute_trusted_structural_mutation_batch(vec![
                insert(TARGET, row_patch(20_000)),
                insert(TARGET, row_patch(20_001)),
                insert(TARGET, row_patch(20_002)),
                insert(
                    SOURCE,
                    shallow_source_patch(20_000, 20_000, 20_000, Some(20_001), Some(20_002)),
                ),
            ])
            .is_ok(),
        1 => [20_000, 20_001, 20_002].into_iter().all(|target| {
            session
                .execute_trusted_structural_mutation(delete(TARGET, target))
                .is_err()
        }),
        2 => {
            session
                .execute_trusted_structural_mutation_batch(vec![
                    insert(TARGET, row_patch(20_003)),
                    StructuralMutation::Update {
                        entity: SOURCE.to_string(),
                        key: InputValue::from(20_000_i32),
                        patch: shallow_source_patch(20_000, 20_003, 20_003, None, None),
                    },
                ])
                .is_ok()
                && [20_000, 20_001, 20_002].into_iter().all(|target| {
                    session
                        .execute_trusted_structural_mutation(delete(TARGET, target))
                        .is_ok()
                })
                && session
                    .execute_trusted_structural_mutation(delete(TARGET, 20_003))
                    .is_err()
        }
        3 => {
            session
                .execute_trusted_structural_mutation(delete(SOURCE, 20_000))
                .is_ok()
                && session
                    .execute_trusted_structural_mutation(delete(TARGET, 20_003))
                    .is_ok()
        }
        4 => session
            .execute_trusted_structural_mutation_batch(vec![
                insert(TARGET, row_patch(20_004)),
                insert(
                    SOURCE,
                    shallow_source_patch(20_004, 20_004, 20_004, Some(20_004), Some(20_004)),
                ),
            ])
            .is_ok(),
        5 => session
            .execute_trusted_structural_mutation_batch(vec![
                delete(TARGET, 20_004),
                delete(SOURCE, 20_004),
            ])
            .is_ok(),
        _ => false,
    })
}

/// Exercise every repeated nested source through ordinary and batch commits.
#[must_use]
pub fn exercise_repeated<C: CanisterKind>(session: &DbSession<C>, operation: u8) -> (bool, u64) {
    measured(|| match operation {
        0 => session
            .execute_trusted_structural_mutation_batch(vec![
                insert(TARGET, row_patch(30_000)),
                insert(TARGET, row_patch(30_001)),
                insert(TARGET, row_patch(30_002)),
                insert(TARGET, row_patch(30_003)),
                insert(
                    SOURCE,
                    repeated_source_patch(
                        30_000,
                        30_000,
                        &[30_000, 30_001, 30_000],
                        &[30_001, 30_002],
                        &[(1, 30_002), (2, 30_003)],
                    ),
                ),
            ])
            .is_ok(),
        1 => [30_000, 30_001, 30_002, 30_003].into_iter().all(|target| {
            session
                .execute_trusted_structural_mutation(delete(TARGET, target))
                .is_err()
        }),
        2 => {
            session
                .execute_trusted_structural_mutation_batch(vec![
                    insert(TARGET, row_patch(30_004)),
                    StructuralMutation::Update {
                        entity: SOURCE.to_string(),
                        key: InputValue::from(30_000_i32),
                        patch: repeated_source_patch(
                            30_000,
                            30_004,
                            &[30_004, 30_004],
                            &[30_004],
                            &[(1, 30_004), (2, 30_004)],
                        ),
                    },
                ])
                .is_ok()
                && [30_000, 30_001, 30_002, 30_003].into_iter().all(|target| {
                    session
                        .execute_trusted_structural_mutation(delete(TARGET, target))
                        .is_ok()
                })
                && session
                    .execute_trusted_structural_mutation(delete(TARGET, 30_004))
                    .is_err()
        }
        3 => {
            session
                .execute_trusted_structural_mutation(delete(SOURCE, 30_000))
                .is_ok()
                && session
                    .execute_trusted_structural_mutation(delete(TARGET, 30_004))
                    .is_ok()
        }
        4 => session
            .execute_trusted_structural_mutation_batch(vec![
                insert(TARGET, row_patch(30_005)),
                insert(
                    SOURCE,
                    repeated_source_patch(30_005, 30_005, &[30_005], &[30_005], &[(1, 30_005)]),
                ),
            ])
            .is_ok(),
        5 => session
            .execute_trusted_structural_mutation_batch(vec![
                delete(TARGET, 30_005),
                delete(SOURCE, 30_005),
            ])
            .is_ok(),
        _ => false,
    })
}

/// Commit one repeated row at the exact raw-occurrence boundary under test.
pub fn commit_repeated_boundary<C: CanisterKind>(
    session: &DbSession<C>,
    base: i32,
    references: u16,
) -> Result<(), icydb::Error> {
    let repeated = vec![base; usize::from(references)];
    session
        .execute_trusted_structural_mutation_batch(vec![
            insert(TARGET, row_patch(base)),
            insert(
                SOURCE,
                repeated_source_patch(base, base, repeated.as_slice(), &[], &[]),
            ),
        ])
        .map(|_| ())
}

/// Define the shared endpoint surface used by every proving actor.
#[macro_export]
macro_rules! define_relation_cost_measurement_actor {
    (@shared) => {
        ::icydb::start!();

        #[::ic_cdk::update]
        fn measure_relation_calibration(
            base: i32,
            count: u16,
            phase: u8,
        ) -> Result<u64, ::icydb::Error> {
            ::icydb::db::with_request_execution(|| {
                let session = db()?;
                $crate::harness::calibrate(&session, base, count, phase)
            })
        }

        #[::ic_cdk::query]
        fn measure_relation_traversal(path_steps: u8, references: u16) -> ((u64, u64),) {
            ($crate::harness::measure_traversal(path_steps, references),)
        }

        #[::ic_cdk::update]
        fn exercise_relation_flow(operation: u8) -> ((bool, u64),) {
            ::icydb::db::with_request_execution(|| {
                let Ok(session) = db() else {
                    return ((false, 0),);
                };
                ($crate::harness::exercise(&session, operation),)
            })
        }

    };

    () => {
        $crate::define_relation_cost_measurement_actor!(@shared);

        #[cfg(feature = "candid-export")]
        ::ic_cdk::export_candid!();
    };

    (shallow) => {
        $crate::define_relation_cost_measurement_actor!(@shared);

        #[::ic_cdk::update]
        fn exercise_shallow_relation_flow(operation: u8) -> ((bool, u64),) {
            ::icydb::db::with_request_execution(|| {
                let Ok(session) = db() else {
                    return ((false, 0),);
                };
                ($crate::harness::exercise_shallow(&session, operation),)
            })
        }

        #[cfg(feature = "candid-export")]
        ::ic_cdk::export_candid!();
    };

    (repeated) => {
        $crate::define_relation_cost_measurement_actor!(@shared);

        #[::ic_cdk::update]
        fn exercise_repeated_relation_flow(operation: u8) -> ((bool, u64),) {
            ::icydb::db::with_request_execution(|| {
                let Ok(session) = db() else {
                    return ((false, 0),);
                };
                ($crate::harness::exercise_repeated(&session, operation),)
            })
        }

        #[::ic_cdk::update]
        fn commit_repeated_relation_boundary(
            base: i32,
            references: u16,
        ) -> Result<(), ::icydb::Error> {
            ::icydb::db::with_request_execution(|| {
                let session = db()?;
                $crate::harness::commit_repeated_boundary(&session, base, references)
            })
        }

        #[cfg(feature = "candid-export")]
        ::ic_cdk::export_candid!();
    };
}
