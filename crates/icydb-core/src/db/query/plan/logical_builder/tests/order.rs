use super::{assert_budget_error, root};
use crate::{
    db::query::{
        plan::{
            OrderDirection, OrderSpec, OrderTerm, canonicalize_order_spec_for_grouping, expr::Expr,
        },
        preparation::PreparationWork,
    },
    value::Value,
};
use icydb_diagnostic_code::{
    DiagnosticExecutionBudgetResource as Resource, DiagnosticExecutionLane as Lane,
    DiagnosticFactTag,
};

#[test]
fn absent_and_grouped_orders_skip_primary_key_work() {
    let keys = ["tenant".into(), "id".into()];
    let grouped = OrderSpec {
        fields: vec![OrderTerm::field("rank", OrderDirection::Desc)],
    };
    for (order, is_grouped) in [(None, false), (Some(grouped), true)] {
        let expected = order.clone();
        let request = root(Resource::PredicateExpressionSteps, 0);
        let result = PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
            canonicalize_order_spec_for_grouping(&keys, order, is_grouped, work)
        })
        .unwrap();
        assert_eq!(result, expected);
        assert_eq!(request.observed(Resource::PredicateExpressionSteps), 0);
        assert_eq!(request.observed(Resource::TemporaryBytes), 0);
    }
}

#[test]
fn composite_tie_break_preserves_authored_terms_and_last_direction() {
    let keys = ["tenant".into(), "id".into()];
    let authored = vec![
        OrderTerm::field("rank", OrderDirection::Asc),
        OrderTerm::field("tenant", OrderDirection::Desc),
    ];
    let request = root(Resource::TemporaryBytes, 16_000_000);
    PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
        let result = canonicalize_order_spec_for_grouping(
            &keys,
            Some(OrderSpec {
                fields: authored.clone(),
            }),
            false,
            work,
        )?
        .unwrap();
        assert_eq!(&result.fields[..2], &authored);
        assert_eq!(
            result.fields[2],
            OrderTerm::field("id", OrderDirection::Desc)
        );
        let bytes = request.observed(Resource::TemporaryBytes);
        assert_eq!(
            canonicalize_order_spec_for_grouping(&keys, Some(result.clone()), false, work)?,
            Some(result)
        );
        assert_eq!(request.observed(Resource::TemporaryBytes), bytes);
        Ok(())
    })
    .unwrap();
}

#[test]
fn only_exact_direct_field_matches_suppress_tie_breaks() {
    let keys = ["id".into()];
    for term in [
        OrderTerm::field("ID", OrderDirection::Desc),
        OrderTerm::new(
            Expr::Literal(Value::Text("id".into())),
            OrderDirection::Desc,
        ),
    ] {
        let request = root(Resource::TemporaryBytes, 16_000_000);
        let result = PreparationWork::run(&request.scope(), Lane::PublicRead, |work| {
            canonicalize_order_spec_for_grouping(
                &keys,
                Some(OrderSpec {
                    fields: vec![term.clone()],
                }),
                false,
                work,
            )
        })
        .unwrap()
        .unwrap();
        assert_eq!(
            result.fields,
            [term, OrderTerm::field("id", OrderDirection::Desc)]
        );
    }
}

#[test]
fn empty_explicit_order_uses_ascending_primary_key_order() {
    let keys = ["tenant".into(), "id".into()];
    let request = root(Resource::TemporaryBytes, 16_000_000);
    let result = PreparationWork::run(&request.scope(), Lane::PublicRead, |work| {
        canonicalize_order_spec_for_grouping(
            &keys,
            Some(OrderSpec { fields: Vec::new() }),
            false,
            work,
        )
    })
    .unwrap()
    .unwrap();
    assert_eq!(
        result.fields,
        [
            OrderTerm::field("tenant", OrderDirection::Asc),
            OrderTerm::field("id", OrderDirection::Asc),
        ]
    );
}

#[test]
fn comparisons_charge_cumulatively_without_allocating_in_every_lane() {
    let keys = ["id".into()];
    for lane in [Lane::PublicRead, Lane::TrustedRead, Lane::Diagnostic] {
        let request = root(Resource::PredicateExpressionSteps, 7);
        let copy = || {
            PreparationWork::run(&request.scope(), lane, |work| {
                canonicalize_order_spec_for_grouping(
                    &keys,
                    Some(OrderSpec {
                        fields: vec![OrderTerm::field("id", OrderDirection::Asc)],
                    }),
                    false,
                    work,
                )
            })
        };
        copy().unwrap();
        assert_eq!(request.observed(Resource::PredicateExpressionSteps), 4);
        let error = copy().unwrap_err();
        assert!(
            error
                .diagnostic_facts()
                .contains(&(DiagnosticFactTag::ExecutionLane, lane.raw()))
        );
        assert_budget_error(error, Resource::PredicateExpressionSteps);
        assert_eq!(request.observed(Resource::PredicateExpressionSteps), 8);
        assert_eq!(request.observed(Resource::TemporaryBytes), 0);
    }
}

#[test]
fn missing_key_rejects_at_backing_or_payload_before_installation() {
    let keys = ["id".into()];
    let source = OrderSpec {
        fields: vec![OrderTerm::field("rank", OrderDirection::Desc)],
    };
    let backing = 4 * size_of::<OrderTerm>() as u64;
    for (limit, expected_steps) in [(backing - 1, 2), (backing + 1, 5), (backing + 2, 5)] {
        let request = root(Resource::TemporaryBytes, limit);
        let result = PreparationWork::run(&request.scope(), Lane::Diagnostic, |work| {
            canonicalize_order_spec_for_grouping(&keys, Some(source.clone()), false, work)
        });
        if limit == backing + 2 {
            assert_eq!(result.unwrap().unwrap().fields.len(), 2);
            assert_eq!(request.observed(Resource::TemporaryBytes), limit);
        } else {
            assert_budget_error(result.unwrap_err(), Resource::TemporaryBytes);
        }
        assert_eq!(
            request.observed(Resource::PredicateExpressionSteps),
            expected_steps
        );
        assert_eq!(
            source.fields,
            [OrderTerm::field("rank", OrderDirection::Desc)]
        );
    }
}
