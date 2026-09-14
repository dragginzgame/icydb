use crate::{
    db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::{
            CoercionId, CoercionSpec, CompareOp,
            normalize::{
                admission::admit_enum_input_construction, normalize_value_for_accepted_kind,
            },
        },
        query::preparation::PreparationWork,
        schema::AcceptedFieldKind,
    },
    types::{IntBig, NatBig},
    value::{Value, ValueEnum},
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

#[test]
fn bigint_numeric_output_is_admitted_before_conversion() {
    for (kind, expected) in [
        (
            AcceptedFieldKind::IntBig { max_bytes: 128 },
            Value::IntBig(IntBig::from(7)),
        ),
        (
            AcceptedFieldKind::NatBig { max_bytes: 128 },
            Value::NatBig(NatBig::from(7_u32)),
        ),
    ] {
        for limit in [0, 63, 64] {
            let root = request(Resource::TemporaryBytes, limit);
            let result = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                normalize_value_for_accepted_kind(
                    "number",
                    &Value::Int64(7),
                    &kind,
                    &CoercionSpec::new(CoercionId::Strict),
                    CompareOp::Eq,
                    work,
                )
            });
            if limit == 64 {
                assert_eq!(result.unwrap(), expected);
            } else {
                assert!(result.unwrap_err().diagnostic_facts().contains(&(
                    DiagnosticFactTag::BudgetResource,
                    Resource::TemporaryBytes.raw()
                )));
            }
        }
    }
}

#[test]
fn enum_input_preflight_covers_nested_payloads_and_preserves_canonical_detection() {
    for (value, expected_enum) in [
        (
            Value::List(vec![Value::Map(vec![(
                Value::Text("k".repeat(128)),
                Value::Blob(vec![1; 4096]),
            )])]),
            false,
        ),
        (
            Value::List(vec![Value::Enum(ValueEnum::test_payload(
                1,
                1,
                Value::Blob(vec![1; 4096]),
            ))]),
            true,
        ),
        (
            Value::Map(vec![(
                Value::Enum(ValueEnum::test_unit(1, 1)),
                Value::Text("v".into()),
            )]),
            true,
        ),
        (
            Value::NatBig(NatBig::from_biguint(
                num_bigint::BigUint::from(1_u8) << 4096_usize,
            )),
            false,
        ),
    ] {
        for resource in [
            Resource::TemporaryBytes,
            Resource::NestedValueSteps,
            Resource::PredicateExpressionSteps,
        ] {
            let root = request(resource, 16_000_000);
            let admitted = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                admit_enum_input_construction(&value, work)
            })
            .unwrap();
            assert_eq!(admitted, expected_enum);
            let exact = root.observed(resource);
            if exact == 0 {
                continue;
            }
            let root = request(resource, exact - 1);
            let error = PreparationWork::run(&root.scope(), Lane::PublicRead, |work| {
                admit_enum_input_construction(&value, work)
            })
            .unwrap_err();
            assert!(
                error
                    .diagnostic_facts()
                    .contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
            );
        }
    }
}
