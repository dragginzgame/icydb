mod expression;

use crate::{
    db::{
        QueryError, RequestExecutionRoot,
        codec::{new_hash_sha256, write_hash_str_u32, write_hash_tag_u8, write_hash_u32},
        executor::budget::{HardExecutionBudget, HardExecutionFailureHeadroom},
        predicate::MissingRowPolicy,
        query::{
            builder::sum,
            fingerprint::{finalize_sha256_digest, hash_sections::hash_order_spec},
            plan::{
                AccessPlannedQuery, LogicalPlan, OrderDirection, OrderSpec, OrderTerm,
                expr::{CaseWhenArm, Expr},
            },
            preparation::{PreparationWork, with_preparation_work},
        },
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

// Freeze the maintained length-prefix/tag grammar independently of admission.
fn expected_hash(labels: &[(&str, OrderDirection)]) -> [u8; 32] {
    let mut hasher = new_hash_sha256();
    write_hash_tag_u8(&mut hasher, 0x31);
    write_hash_u32(&mut hasher, u32::try_from(labels.len()).unwrap());
    for (label, direction) in labels {
        write_hash_str_u32(&mut hasher, label);
        write_hash_tag_u8(
            &mut hasher,
            match direction {
                OrderDirection::Asc => 0x01,
                OrderDirection::Desc => 0x02,
            },
        );
    }
    finalize_sha256_digest(hasher)
}

fn request(resource: Resource, limit: u64) -> RequestExecutionRoot {
    RequestExecutionRoot::new_for_tests(
        HardExecutionBudget::uniform_for_tests(
            16_000_000,
            HardExecutionFailureHeadroom::new(500_000_000, 64 * 1024),
        )
        .with_limit_for_tests(resource, limit),
    )
}

fn admitted_hash(
    order: &OrderSpec,
    root: &RequestExecutionRoot,
    lane: Lane,
) -> Result<[u8; 32], QueryError> {
    PreparationWork::run(&root.scope(), lane, |work| {
        let mut hasher = new_hash_sha256();
        hash_order_spec(&mut hasher, Some(order), work).map_err(QueryError::execute)?;
        Ok(finalize_sha256_digest(hasher))
    })
}

#[test]
fn order_identity_preserves_empty_source_and_term_order_contracts() {
    let hash_spec = |order: Option<&OrderSpec>| {
        let mut hasher = new_hash_sha256();
        with_preparation_work(|work| hash_order_spec(&mut hasher, order, work)).unwrap();
        finalize_sha256_digest(hasher)
    };
    let empty = OrderSpec { fields: vec![] };
    assert_eq!(hash_spec(None), hash_spec(Some(&empty)));
    let mut order = OrderSpec {
        fields: vec![
            OrderTerm::field("owner_λ", OrderDirection::Asc),
            OrderTerm::field("amount", OrderDirection::Desc),
        ],
    };
    let expected = expected_hash(&[
        ("owner_λ", OrderDirection::Asc),
        ("amount", OrderDirection::Desc),
    ]);
    assert_eq!(hash_spec(Some(&order)), expected);
    order.fields.reverse();
    assert_ne!(hash_spec(Some(&order)), expected);
    order.fields.reverse();
    order.fields[1] = OrderTerm::field("amount", OrderDirection::Asc);
    assert_ne!(hash_spec(Some(&order)), expected);
}

#[test]
fn direct_order_labels_need_no_temporary_backing() {
    let order = OrderSpec {
        fields: vec![OrderTerm::field("账户", OrderDirection::Desc)],
    };
    let root = request(Resource::TemporaryBytes, 0);
    assert_eq!(
        admitted_hash(&order, &root, Lane::PublicRead).unwrap(),
        expected_hash(&[("账户", OrderDirection::Desc)])
    );
    assert_eq!(root.observed(Resource::TemporaryBytes), 0);
    assert_eq!(
        root.observed(Resource::PredicateExpressionSteps),
        1 + "账户".len() as u64
    );
}

#[test]
fn expression_order_hashing_preserves_bytes_under_exact_and_cumulative_admission() {
    let order = OrderSpec {
        fields: vec![
            OrderTerm::field("owner_λ", OrderDirection::Asc),
            OrderTerm::new(
                Expr::Case {
                    when_then_arms: vec![CaseWhenArm::new(
                        Expr::Literal(Value::Bool(true)),
                        Expr::Aggregate(
                            sum("amount")
                                .distinct()
                                .with_filter_expr(Expr::Literal(Value::Bool(false))),
                        ),
                    )],
                    else_expr: Box::new(Expr::Literal(Value::NatBig(
                        "18446744073709551616".parse().unwrap(),
                    ))),
                },
                OrderDirection::Desc,
            ),
        ],
    };
    let before = order.clone();
    let expected = expected_hash(&[
        ("owner_λ", OrderDirection::Asc),
        (
            "CASE WHEN TRUE THEN SUM(DISTINCT amount) FILTER (WHERE FALSE) ELSE 18_446_744_073_709_551_616 END",
            OrderDirection::Desc,
        ),
    ]);
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        let measured = request(Resource::TemporaryBytes, 16_000_000);
        assert_eq!(admitted_hash(&order, &measured, lane).unwrap(), expected);
        for resource in [Resource::TemporaryBytes, Resource::PredicateExpressionSteps] {
            let exact = measured.observed(resource);
            assert!(exact > 0);
            for limit in [exact - 1, exact, 2 * exact] {
                let root = request(resource, limit);
                for attempt in 1..=3 {
                    let result = admitted_hash(&order, &root, lane);
                    if attempt * exact <= limit {
                        assert_eq!(result.unwrap(), expected);
                    } else {
                        let facts = result.unwrap_err().diagnostic_facts();
                        assert!(
                            facts.contains(&(DiagnosticFactTag::BudgetResource, resource.raw()))
                        );
                        assert!(facts.contains(&(DiagnosticFactTag::ExecutionLane, lane.raw())));
                        break;
                    }
                }
                assert_eq!(root.observed(Resource::RowsVisited), 0);
                assert_eq!(root.observed(Resource::QueryExecutions), 0);
            }
            assert_eq!(
                admitted_hash(&order, &request(resource, exact), lane).unwrap(),
                expected
            );
        }
    }
    assert_eq!(order, before);
}

#[test]
fn order_format_failure_cannot_publish_a_continuation() {
    let mut plan = AccessPlannedQuery::full_scan_for_test(MissingRowPolicy::Ignore);
    let LogicalPlan::Scalar(scalar) = &mut plan.logical else {
        unreachable!()
    };
    scalar.order = Some(OrderSpec {
        fields: vec![OrderTerm::new(
            Expr::Literal(Value::NatBig("18446744073709551616".parse().unwrap())),
            OrderDirection::Desc,
        )],
    });
    let build = |root: &RequestExecutionRoot| {
        PreparationWork::run(&root.scope(), Lane::Diagnostic, |work| {
            plan.planned_continuation_contract_with_accepted_identity("tests::Entity", None, work)
                .map_err(QueryError::execute)
        })
    };
    let root = request(Resource::TemporaryBytes, 27);
    let error = build(&root).unwrap_err();
    assert!(error.diagnostic_facts().contains(&(
        DiagnosticFactTag::BudgetResource,
        Resource::TemporaryBytes.raw()
    )));
    // Bigint conversion rejects its 28-byte scratch before output or order copying.
    assert_eq!(root.observed(Resource::TemporaryBytes), 28);
    let root = request(Resource::TemporaryBytes, 16_000_000);
    let expected =
        with_preparation_work(|work| plan.continuation_signature("tests::Entity", work)).unwrap();
    assert_eq!(
        build(&root).unwrap().unwrap().continuation_signature(),
        expected
    );
}
