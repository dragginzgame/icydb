//! Test-only commit protocol failpoints.

use crate::error::InternalError;
use std::cell::RefCell;

thread_local! {
    static ACTIVE_FAILPOINT: RefCell<Option<CommitFailpointSpec>> = const {
        RefCell::new(None)
    };
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CommitFailpoint {
    BeforeMarkerWrite,
    AfterMarkerWrite,
    BeforeMarkerBoundJournalAppend,
    AfterMarkerBoundJournalAppend,
    BeforeMarkerClear,
    AfterMarkerClear,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CommitFailpointMode {
    ReturnError,
    PanicUnwind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct CommitFailpointSpec {
    site: CommitFailpoint,
    mode: CommitFailpointMode,
}

pub(in crate::db) fn arm_commit_failpoint_for_tests(
    site: CommitFailpoint,
    mode: CommitFailpointMode,
) {
    ACTIVE_FAILPOINT.with_borrow_mut(|active| {
        *active = Some(CommitFailpointSpec { site, mode });
    });
}

pub(in crate::db) fn clear_commit_failpoint_for_tests() {
    ACTIVE_FAILPOINT.with_borrow_mut(Option::take);
}

pub(in crate::db::commit) fn hit_commit_failpoint(
    site: CommitFailpoint,
) -> Result<(), InternalError> {
    let mode = ACTIVE_FAILPOINT.with_borrow_mut(|active| {
        if active.as_ref().is_some_and(|spec| spec.site == site) {
            return active.take().map(|spec| spec.mode);
        }

        None
    });

    match mode {
        Some(CommitFailpointMode::ReturnError) => Err(InternalError::store_internal()),
        Some(CommitFailpointMode::PanicUnwind) => panic!("commit failpoint: {site:?}"),
        None => Ok(()),
    }
}
