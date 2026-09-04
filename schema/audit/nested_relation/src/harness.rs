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

/// Define the identical endpoint surface used by every proving actor.
#[macro_export]
macro_rules! define_relation_cost_measurement_actor {
    () => {
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

        #[cfg(feature = "candid-export")]
        ::ic_cdk::export_candid!();
    };
}
