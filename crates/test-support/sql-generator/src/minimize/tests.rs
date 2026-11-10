use super::minimize;
use crate::{SqlGeneratorError, SqlGeneratorErrorKind};

#[test]
fn first_matching_candidate_restarts_from_the_accepted_case() {
    let mut visited = Vec::new();
    let result = minimize(
        &4,
        10,
        10,
        |value| (0..*value).collect(),
        |candidate| {
            visited.push(*candidate);
            Ok(*candidate > 1)
        },
    )
    .unwrap();
    assert_eq!(result, (2, true, 5));
    assert_eq!(visited, [0, 1, 2, 0, 1]);
}

#[test]
fn either_ceiling_stops_before_the_next_evaluation() {
    for (candidate_limit, evaluation_limit) in [(2, 10), (10, 2)] {
        let mut visited = Vec::new();
        let result = minimize(
            &4,
            candidate_limit,
            evaluation_limit,
            |value| vec![value - 1],
            |candidate| {
                visited.push(*candidate);
                Ok(true)
            },
        )
        .unwrap();
        assert_eq!(result, (2, false, 2));
        assert_eq!(visited, [3, 2]);
    }
}

#[test]
fn fixed_point_on_the_last_allowed_evaluation_is_complete() {
    let result = minimize(
        &2,
        2,
        2,
        |value| (0..*value).rev().take(1).collect(),
        |_| Ok(true),
    )
    .unwrap();
    assert_eq!(result, (0, true, 2));
}

#[test]
fn evaluator_failure_keeps_its_typed_cause() {
    let error = minimize(
        &1,
        2,
        2,
        |_| vec![0],
        |_| {
            Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::InvalidCase,
                "fixture failure",
            ))
        },
    )
    .unwrap_err();
    assert_eq!(error.kind(), SqlGeneratorErrorKind::InvalidCase);
}
