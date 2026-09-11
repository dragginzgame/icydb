use super::*;
use crate::{
    db::{
        RequestExecutionRoot,
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::{
            CoercionId, CoercionSpec, CompareFieldsPredicate, CompareOp, ComparePredicate,
        },
        query::admission::input::MAX_QUERY_INPUT_DEPTH,
    },
    value::{Value, ValueEnum},
};
use icydb_diagnostic_code::{DiagnosticExecutionLane as Lane, DiagnosticFactTag};

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn project(
    source: &Predicate,
    root: &RequestExecutionRoot,
) -> Result<ExplainPredicate, QueryError> {
    PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
        ExplainPredicate::from_predicate(source, work)
    })
}

fn coercion() -> CoercionSpec {
    CoercionSpec {
        id: CoercionId::NumericWiden,
        // Raw model copies must not normalize, sort or drop duplicate params.
        params: vec![("z".into(), "1".into()), ("z".into(), "2".into())],
    }
}

fn cases() -> Vec<(Predicate, ExplainPredicate)> {
    let value = Value::List(vec![
        Value::Enum(ValueEnum::test_payload(
            3,
            4,
            Value::Map(vec![(Value::Nat64(9), Value::Blob(vec![1, 2, 3]))]),
        )),
        Value::Text("quote's λ".into()),
    ]);
    vec![
        (Predicate::True, ExplainPredicate::True),
        (Predicate::False, ExplainPredicate::False),
        (
            Predicate::IsNull { field: "a".into() },
            ExplainPredicate::IsNull { field: "a".into() },
        ),
        (
            Predicate::IsNotNull { field: "b".into() },
            ExplainPredicate::IsNotNull { field: "b".into() },
        ),
        (
            Predicate::IsMissing { field: "c".into() },
            ExplainPredicate::IsMissing { field: "c".into() },
        ),
        (
            Predicate::IsEmpty { field: "d".into() },
            ExplainPredicate::IsEmpty { field: "d".into() },
        ),
        (
            Predicate::IsNotEmpty { field: "e".into() },
            ExplainPredicate::IsNotEmpty { field: "e".into() },
        ),
        (
            Predicate::TextContains {
                field: "f".into(),
                value: Value::Text("needle".into()),
            },
            ExplainPredicate::TextContains {
                field: "f".into(),
                value: Value::Text("needle".into()),
            },
        ),
        (
            Predicate::TextContainsCi {
                field: "g".into(),
                value: Value::Text("Needle".into()),
            },
            ExplainPredicate::TextContainsCi {
                field: "g".into(),
                value: Value::Text("Needle".into()),
            },
        ),
        (
            Predicate::Compare(ComparePredicate {
                field: "h".into(),
                op: CompareOp::In,
                value: value.clone(),
                coercion: coercion(),
            }),
            ExplainPredicate::Compare {
                field: "h".into(),
                op: CompareOp::In,
                value,
                coercion: coercion(),
            },
        ),
        (
            Predicate::CompareFields(CompareFieldsPredicate {
                left_field: "a".into(),
                op: CompareOp::Eq,
                right_field: "z".into(),
                coercion: coercion(),
            }),
            ExplainPredicate::CompareFields {
                left_field: "a".into(),
                op: CompareOp::Eq,
                right_field: "z".into(),
                coercion: coercion(),
            },
        ),
    ]
}

#[test]
fn predicate_projection_preserves_every_family() {
    let (sources, expected): (Vec<_>, Vec<_>) = cases().into_iter().unzip();
    let source = Predicate::And(vec![
        Predicate::Or(sources),
        Predicate::Not(Box::new(Predicate::True)),
        Predicate::And(vec![]),
        Predicate::Or(vec![]),
    ]);
    let expected = ExplainPredicate::And(vec![
        ExplainPredicate::Or(expected),
        ExplainPredicate::Not(Box::new(ExplainPredicate::True)),
        ExplainPredicate::And(vec![]),
        ExplainPredicate::Or(vec![]),
    ]);
    let root = request(Resource::TemporaryBytes, 16_000_000);
    let explain = project(&source, &root).unwrap();
    assert_eq!(explain, expected);
    assert!(root.observed(Resource::NestedValueSteps) > 0);
    assert_eq!(root.observed(Resource::RowsVisited), 0);
}

#[test]
fn predicate_projection_charges_payload_and_coercion_backing_exactly() {
    let source = Predicate::Compare(ComparePredicate {
        field: "abc".into(),
        op: CompareOp::Eq,
        value: Value::Text("payload".into()),
        coercion: CoercionSpec {
            id: CoercionId::Strict,
            params: vec![("name".into(), "val".into())],
        },
    });
    let bytes = (3 + 7 + size_of::<(String, String)>() + 4 + 3) as u64;
    let root = request(Resource::TemporaryBytes, bytes);
    assert!(project(&source, &root).is_ok());
    assert_eq!(root.observed(Resource::TemporaryBytes), bytes);
    let short = request(Resource::TemporaryBytes, bytes - 1);
    let error = project(&source, &short).unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw(),
    )));
    assert_eq!(short.observed(Resource::TemporaryBytes), bytes);
}

#[test]
fn predicate_projection_admits_container_backing_before_child_visits() {
    let children = vec![Predicate::True; 8];
    let bytes = 8 * size_of::<ExplainPredicate>() as u64;
    for source in [Predicate::And(children.clone()), Predicate::Or(children)] {
        let short = request(
            Resource::TemporaryBytes,
            8 * size_of::<ExplainPredicate>() as u64 - 1,
        );
        assert!(project(&source, &short).is_err());
        assert_eq!(short.observed(Resource::PredicateExpressionSteps), 1);
        let exact = request(Resource::TemporaryBytes, bytes);
        assert!(project(&source, &exact).is_ok());
        assert_eq!(exact.observed(Resource::TemporaryBytes), bytes);
        assert_eq!(exact.observed(Resource::PredicateExpressionSteps), 9);
    }
}

#[test]
fn predicate_projection_retains_admitted_depth_and_stops_at_exhausted_visit() {
    let mut source = Predicate::True;
    for _ in 1..MAX_QUERY_INPUT_DEPTH {
        source = Predicate::Not(Box::new(source));
    }
    let exact = request(
        Resource::PredicateExpressionSteps,
        MAX_QUERY_INPUT_DEPTH as u64,
    );
    let projected = project(&source, &exact).unwrap();
    let mut node = &projected;
    for _ in 1..MAX_QUERY_INPUT_DEPTH {
        let ExplainPredicate::Not(child) = node else {
            panic!("expected nested predicate")
        };
        node = child;
    }
    assert_eq!(*node, ExplainPredicate::True);
    assert_eq!(
        exact.observed(Resource::PredicateExpressionSteps),
        MAX_QUERY_INPUT_DEPTH as u64
    );
    let short = request(
        Resource::PredicateExpressionSteps,
        MAX_QUERY_INPUT_DEPTH as u64 - 1,
    );
    assert!(project(&source, &short).is_err());
    assert_eq!(
        short.observed(Resource::PredicateExpressionSteps),
        MAX_QUERY_INPUT_DEPTH as u64
    );
}
