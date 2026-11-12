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
mod tests;
