//! Deterministic first-match shrinking shared by SELECT and mutation cases.

use crate::SqlGeneratorError;

// Each valid candidate incurs exactly one complete evaluation in both lanes.
// One counter therefore enforces both ceilings without changing their public
// accounting. Candidate generation and signature matching remain lane-owned.
pub(crate) fn minimize<T: Clone>(
    original: &T,
    max_candidates: u32,
    max_evaluations: u32,
    mut candidates: impl FnMut(&T) -> Vec<T>,
    mut preserves_failure: impl FnMut(&T) -> Result<bool, SqlGeneratorError>,
) -> Result<(T, bool, u32), SqlGeneratorError> {
    let mut minimized = original.clone();
    let mut evaluations = 0_u32;
    let limit = max_candidates.min(max_evaluations);

    loop {
        let mut accepted = None;
        for candidate in candidates(&minimized) {
            if evaluations >= limit {
                return Ok((minimized, false, evaluations));
            }
            evaluations = evaluations.saturating_add(1);
            if preserves_failure(&candidate)? {
                accepted = Some(candidate);
                break;
            }
        }
        let Some(candidate) = accepted else {
            return Ok((minimized, true, evaluations));
        };
        minimized = candidate;
    }
}

#[cfg(test)]
mod tests {
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
}
