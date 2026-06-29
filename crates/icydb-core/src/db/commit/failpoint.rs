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
pub(in crate::db) enum CommitFailpointRecoveryAuthority {
    NoCommitAuthority,
    MarkerPayload,
    MarkerPayloadAndJournalPrefix,
    RecoveredStateWithMarker,
    RecoveredStateWithoutMarker,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CommitFailpointSnapshotOracle {
    PreCommit,
    MarkerAuthorizedPostCommit,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct CommitFailpointRecoveryOracle {
    snapshot: CommitFailpointSnapshotOracle,
    marker_present: bool,
    journal_tail_batches: u64,
}

impl CommitFailpointRecoveryOracle {
    pub(in crate::db) const fn snapshot(self) -> CommitFailpointSnapshotOracle {
        self.snapshot
    }

    pub(in crate::db) const fn marker_present(self) -> bool {
        self.marker_present
    }

    pub(in crate::db) const fn journal_tail_batches(self) -> u64 {
        self.journal_tail_batches
    }
}

impl CommitFailpoint {
    pub(in crate::db) const fn recovery_authority(self) -> CommitFailpointRecoveryAuthority {
        match self {
            Self::BeforeMarkerWrite => CommitFailpointRecoveryAuthority::NoCommitAuthority,
            Self::AfterMarkerWrite | Self::BeforeMarkerBoundJournalAppend => {
                CommitFailpointRecoveryAuthority::MarkerPayload
            }
            Self::AfterMarkerBoundJournalAppend => {
                CommitFailpointRecoveryAuthority::MarkerPayloadAndJournalPrefix
            }
            Self::BeforeMarkerClear => CommitFailpointRecoveryAuthority::RecoveredStateWithMarker,
            Self::AfterMarkerClear => CommitFailpointRecoveryAuthority::RecoveredStateWithoutMarker,
        }
    }

    pub(in crate::db) const fn recovery_oracle(self) -> CommitFailpointRecoveryOracle {
        match self.recovery_authority() {
            CommitFailpointRecoveryAuthority::NoCommitAuthority => CommitFailpointRecoveryOracle {
                snapshot: CommitFailpointSnapshotOracle::PreCommit,
                marker_present: false,
                journal_tail_batches: 0,
            },
            CommitFailpointRecoveryAuthority::MarkerPayload => CommitFailpointRecoveryOracle {
                snapshot: CommitFailpointSnapshotOracle::PreCommit,
                marker_present: true,
                journal_tail_batches: 0,
            },
            CommitFailpointRecoveryAuthority::MarkerPayloadAndJournalPrefix => {
                CommitFailpointRecoveryOracle {
                    snapshot: CommitFailpointSnapshotOracle::PreCommit,
                    marker_present: true,
                    journal_tail_batches: 1,
                }
            }
            CommitFailpointRecoveryAuthority::RecoveredStateWithMarker => {
                CommitFailpointRecoveryOracle {
                    snapshot: CommitFailpointSnapshotOracle::MarkerAuthorizedPostCommit,
                    marker_present: true,
                    journal_tail_batches: 0,
                }
            }
            CommitFailpointRecoveryAuthority::RecoveredStateWithoutMarker => {
                CommitFailpointRecoveryOracle {
                    snapshot: CommitFailpointSnapshotOracle::MarkerAuthorizedPostCommit,
                    marker_present: false,
                    journal_tail_batches: 0,
                }
            }
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CommitFailpointMode {
    ReturnError,
    PanicUnwind,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum CommitFailpointFailureClass {
    StructuredReturnedError,
    HostUnwindInterruption,
}

impl CommitFailpointMode {
    pub(in crate::db) const fn failure_class(self) -> CommitFailpointFailureClass {
        match self {
            Self::ReturnError => CommitFailpointFailureClass::StructuredReturnedError,
            Self::PanicUnwind => CommitFailpointFailureClass::HostUnwindInterruption,
        }
    }
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
