//! Module: db::query::plan::primary_key_input_resource
//! Responsibility: planner-owned primary-key input resource summaries.
//! Does not own: read-admission policy, key-access selection, or execution.
//! Boundary: estimates pre-execution key-list work for exact-key admission.

use crate::{
    db::query::{construction::ConstructionBudget, plan::PrimaryKeyInputResourceSummary},
    error::InternalError,
    types::AccountStorageCodec,
    value::{Value, ValueEnum},
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;

/// Build resource facts for one raw model-level key value list.
pub(in crate::db::query) fn primary_key_input_resource_from_value_list(
    values: &[Value],
    budget: &dyn ConstructionBudget,
) -> Result<Option<PrimaryKeyInputResourceSummary>, InternalError> {
    if values.is_empty() {
        return Ok(None);
    }

    let estimated_payload_bytes = values.iter().try_fold(0u32, |total, value| {
        Ok::<_, InternalError>(total.saturating_add(estimate_value_payload_bytes(value, budget)?))
    })?;

    Ok(Some(PrimaryKeyInputResourceSummary::new(
        u32::try_from(values.len()).unwrap_or(u32::MAX),
        estimated_payload_bytes,
    )))
}

/// Visit payload structure under the caller's authority without encoding or copying.
pub(super) fn estimate_value_payload_bytes(
    value: &Value,
    budget: &dyn ConstructionBudget,
) -> Result<u32, InternalError> {
    budget.charge(Resource::PredicateExpressionSteps, 1)?;
    Ok(match value {
        Value::Account(_) => crate::types::Account::STORED_SIZE,
        Value::Blob(bytes) => byte_len_u32(bytes.len()),
        Value::Bool(_) => 1,
        Value::Date(_) | Value::Float32(_) => 4,
        Value::Decimal(_) => 20,
        Value::Duration(_)
        | Value::Float64(_)
        | Value::Int64(_)
        | Value::Nat64(_)
        | Value::Timestamp(_) => 8,
        Value::Enum(value) => estimate_enum_payload_bytes(value, budget)?,
        Value::Int128(_) | Value::Nat128(_) | Value::Ulid(_) => 16,
        Value::IntBig(value) => {
            // Signed length can scan trailing zero limbs at a sign boundary.
            // Admit that scan from bit metadata before asking for its length.
            budget.charge(
                Resource::PredicateExpressionSteps,
                value.magnitude_bits().div_ceil(32),
            )?;
            u32::try_from(value.leb128_len()).unwrap_or(u32::MAX)
        }
        Value::List(values) => values.iter().try_fold(0u32, |total, value| {
            Ok::<_, InternalError>(
                total.saturating_add(estimate_value_payload_bytes(value, budget)?),
            )
        })?,
        Value::Map(entries) => entries.iter().try_fold(0u32, |total, (key, value)| {
            Ok::<_, InternalError>(
                total
                    .saturating_add(estimate_value_payload_bytes(key, budget)?)
                    .saturating_add(estimate_value_payload_bytes(value, budget)?),
            )
        })?,
        Value::NatBig(value) => u32::try_from(value.leb128_len()).unwrap_or(u32::MAX),
        Value::Null | Value::Unit => 0,
        Value::Principal(value) => byte_len_u32(value.as_slice().len()),
        Value::Subaccount(_) | Value::U256(_) => 32,
        Value::Text(value) => byte_len_u32(value.len()),
    })
}

fn estimate_enum_payload_bytes(
    value: &ValueEnum,
    budget: &dyn ConstructionBudget,
) -> Result<u32, InternalError> {
    let mut bytes = 9_u32;
    if let Some(payload) = value.payload() {
        bytes = bytes.saturating_add(estimate_value_payload_bytes(payload, budget)?);
    }

    Ok(bytes)
}

fn byte_len_u32(len: usize) -> u32 {
    u32::try_from(len).unwrap_or(u32::MAX)
}

#[cfg(test)]
mod tests {
    use super::{estimate_value_payload_bytes, primary_key_input_resource_from_value_list};
    use crate::{
        db::{
            QueryError, RequestExecutionRoot,
            executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
            query::preparation::PreparationWork,
        },
        types::{IntBig, NatBig},
        value::{Value, ValueEnum},
    };
    use icydb_diagnostic_code::{
        DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
        DiagnosticFactTag,
    };

    fn request(steps: u64) -> RequestExecutionRoot {
        RequestExecutionRoot::new_for_tests(
            HardExecutionBudget::uniform_for_tests(
                16_000_000,
                HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
            )
            .with_limit_for_tests(Resource::PredicateExpressionSteps, steps)
            .with_limit_for_tests(Resource::TemporaryBytes, 0),
        )
    }

    #[test]
    fn bigint_key_payload_estimates_match_encoded_lengths() {
        for integer in [-8193_i32, -8192, -65, -64, -1, 0, 63, 64, 8191, 8192] {
            let signed = IntBig::from(integer);
            let unsigned = NatBig::from(integer.unsigned_abs());
            let expected =
                u32::try_from(signed.to_leb128().len() + unsigned.to_leb128().len()).unwrap();
            let values = Value::List(vec![Value::IntBig(signed), Value::NatBig(unsigned)]);
            let root = request(16_000_000);
            PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                assert_eq!(
                    estimate_value_payload_bytes(&values, work).unwrap(),
                    expected
                );
                Ok(())
            })
            .unwrap();
        }
    }

    #[test]
    fn nested_payload_sizing_is_cumulative_and_allocation_free() {
        let value = Value::Enum(ValueEnum::test_payload(
            1,
            1,
            Value::Map(vec![(
                Value::Text("key".into()),
                Value::List(vec![Value::Null, Value::Nat64(7)]),
            )]),
        ));
        // Enum, map, key, list and two elements: lengths do not scan text bytes.
        let steps = 6;
        for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
            for limit in [0, steps - 1, steps, 2 * steps] {
                let root = request(limit);
                for attempt in 1..=3 {
                    let result = PreparationWork::run(&root.scope(), lane, |work| {
                        estimate_value_payload_bytes(&value, work).map_err(QueryError::execute)
                    });
                    if attempt * steps <= limit {
                        assert_eq!(result.unwrap(), 20);
                    } else {
                        let facts = result.unwrap_err().diagnostic_facts();
                        assert!(facts.contains(&(
                            DiagnosticFactTag::BudgetResource,
                            Resource::PredicateExpressionSteps.raw()
                        )));
                        assert!(facts.contains(&(DiagnosticFactTag::ExecutionLane, lane.raw())));
                        break;
                    }
                }
                assert_eq!(root.observed(Resource::TemporaryBytes), 0);
                assert_eq!(root.observed(Resource::RowsVisited), 0);
            }
        }
    }

    #[test]
    fn payload_length_metadata_does_not_charge_or_copy_scalar_bytes() {
        let values = vec![
            Value::Text("x".repeat(256 * 1024)),
            Value::Blob(vec![0; 256 * 1024]),
        ];
        let root = request(2);
        PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
            let summary = primary_key_input_resource_from_value_list(&values, work)
                .unwrap()
                .unwrap();
            assert_eq!(summary.raw_term_count(), 2);
            assert_eq!(summary.estimated_payload_bytes(), 512 * 1024);
            assert!(
                primary_key_input_resource_from_value_list(&[], work)
                    .unwrap()
                    .is_none()
            );
            Ok(())
        })
        .unwrap();
        assert_eq!(root.observed(Resource::PredicateExpressionSteps), 2);
        assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    }

    #[test]
    fn signed_bigint_length_scan_is_admitted_before_sizing() {
        let value = IntBig::from_bigint(-(num_bigint::BigInt::from(1u8) << 223usize));
        let expected = u32::try_from(value.to_leb128().len()).unwrap();
        let steps = 1 + value.magnitude_bits().div_ceil(32);
        let value = Value::IntBig(value);
        for limit in [steps - 1, steps] {
            let root = request(limit);
            let result = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                estimate_value_payload_bytes(&value, work).map_err(QueryError::execute)
            });
            if limit == steps {
                assert_eq!(result.unwrap(), expected);
            } else {
                assert!(result.unwrap_err().diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::PredicateExpressionSteps.raw(),
                )));
            }
        }
    }
}
