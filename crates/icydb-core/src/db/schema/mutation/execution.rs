//! Schema mutation execution and runner-preflight contracts.

use super::{
    MutationPublicationBlocker, RebuildRequirement, SchemaExpressionIndexRebuildTarget,
    SchemaFieldPathIndexRebuildTarget, SchemaMutationRunnerOutcome, SchemaMutationRunnerRejection,
    SchemaMutationRunnerReport, SchemaMutationStoreVisibility, SchemaRebuildAction,
    SchemaRebuildPlan, SchemaSecondaryIndexDropCleanupTarget,
};

///
/// SchemaMutationExecutionReadiness
///
/// Schema-owned execution readiness for one mutation plan. This names whether
/// reconciliation can publish immediately, whether a future physical runner
/// must execute index work first, or whether the mutation remains unsupported.
///

#[allow(
    dead_code,
    reason = "0.152 stages execution-boundary contracts before physical runners consume them"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationExecutionReadiness {
    PublishableNow,
    RequiresPhysicalRunner(RebuildRequirement),
    Unsupported(RebuildRequirement),
}

///
/// SchemaMutationExecutionStep
///
/// Ordered physical execution step implied by one mutation plan. These are
/// contracts for a later runner, not live rebuild behavior.
///

#[allow(
    dead_code,
    reason = "0.152 stages execution-boundary contracts before physical runners consume them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationExecutionStep {
    BuildFieldPathIndex {
        target: SchemaFieldPathIndexRebuildTarget,
    },
    BuildExpressionIndex {
        target: SchemaExpressionIndexRebuildTarget,
    },
    DropSecondaryIndex {
        target: SchemaSecondaryIndexDropCleanupTarget,
    },
    ValidatePhysicalWork,
    InvalidateRuntimeState,
    RewriteAllRows,
    Unsupported {
        reason: &'static str,
    },
}

///
/// SchemaMutationRunnerCapability
///
/// Coarse physical capability required by one execution plan. Capabilities are
/// derived from accepted execution steps and give future runner wiring a small
/// fail-closed surface before any physical mutation is attempted.
///

#[allow(
    dead_code,
    reason = "0.152 stages runner capability contracts before physical runners consume them"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationRunnerCapability {
    BuildFieldPathIndex,
    BuildExpressionIndex,
    DropSecondaryIndex,
    ValidatePhysicalWork,
    InvalidateRuntimeState,
    RewriteAllRows,
}

///
/// SchemaMutationExecutionAdmission
///
/// Fail-closed admission result for one execution plan against a future
/// runner's advertised capabilities.
///

#[allow(
    dead_code,
    reason = "0.152 stages runner admission contracts before physical runners consume them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationExecutionAdmission {
    PublishableNow,
    RunnerReady {
        required: Vec<SchemaMutationRunnerCapability>,
    },
    MissingRunnerCapabilities {
        missing: Vec<SchemaMutationRunnerCapability>,
    },
    Rejected {
        requirement: RebuildRequirement,
    },
}

///
/// SchemaMutationSupportedPathRejection
///
/// Fail-closed reason for the developer-supported physical mutation path. The
/// generic execution planner may describe future physical work, but this gate
/// admits only the field-path secondary-index rebuild path.
///

#[allow(
    dead_code,
    reason = "0.154 starts supported-path admission before reconciliation consumes it"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationSupportedPathRejection {
    NoPhysicalWork,
    UnsupportedRequirement(RebuildRequirement),
    UnsupportedMutationKind,
    UnsupportedExecutionShape,
    EmptyFieldPathKey,
}

///
/// SchemaMutationSupportedExecutionPath
///
/// The single physical mutation path supported for developer testing: add one
/// field-path secondary index from accepted catalog metadata, then validate
/// physical work and invalidate runtime state.
///

#[allow(
    dead_code,
    reason = "0.154 starts supported-path admission before reconciliation consumes it"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaMutationSupportedExecutionPath {
    target: SchemaFieldPathIndexRebuildTarget,
}

#[expect(
    dead_code,
    reason = "0.154 starts supported-path admission before reconciliation consumes it"
)]
impl SchemaMutationSupportedExecutionPath {
    #[must_use]
    pub(in crate::db::schema) const fn new(target: SchemaFieldPathIndexRebuildTarget) -> Self {
        Self { target }
    }

    #[must_use]
    pub(in crate::db::schema) const fn target(&self) -> &SchemaFieldPathIndexRebuildTarget {
        &self.target
    }

    #[must_use]
    pub(in crate::db::schema) fn required_capabilities() -> Vec<SchemaMutationRunnerCapability> {
        vec![
            SchemaMutationRunnerCapability::BuildFieldPathIndex,
            SchemaMutationRunnerCapability::ValidatePhysicalWork,
            SchemaMutationRunnerCapability::InvalidateRuntimeState,
        ]
    }
}

///
/// SchemaMutationRunnerPreflight
///
/// Runner-facing preflight result for one execution plan. This is the last
/// schema-owned check before a future physical runner is allowed to start
/// rebuild or cleanup work.
///

#[allow(
    dead_code,
    reason = "0.152 stages runner preflight contracts before physical runners consume them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationRunnerPreflight {
    NoPhysicalWork,
    Ready {
        step_count: usize,
        required: Vec<SchemaMutationRunnerCapability>,
    },
    MissingCapabilities {
        missing: Vec<SchemaMutationRunnerCapability>,
    },
    Rejected {
        requirement: RebuildRequirement,
    },
}

///
/// MutationPublicationPreflight
///
/// Publication-boundary decision after consulting runner preflight. It keeps
/// metadata-only publication separate from physical-work readiness so a future
/// runner cannot accidentally make rebuild-required plans publishable before
/// physical execution and validation exist.
///

#[allow(
    dead_code,
    reason = "0.152 stages runner preflight publication checks before physical runners consume them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum MutationPublicationPreflight {
    PublishableNow,
    PhysicalWorkReady {
        step_count: usize,
        required: Vec<SchemaMutationRunnerCapability>,
    },
    MissingRunnerCapabilities {
        missing: Vec<SchemaMutationRunnerCapability>,
    },
    Rejected {
        requirement: RebuildRequirement,
    },
    Blocked(MutationPublicationBlocker),
}

///
/// SchemaMutationRunnerContract
///
/// Capability advertisement for a future physical mutation runner. It owns no
/// execution behavior yet; it only lets schema mutation plans fail closed before
/// publication policy can be widened.
///

#[allow(
    dead_code,
    reason = "0.152 stages runner preflight contracts before physical runners consume them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaMutationRunnerContract {
    capabilities: Vec<SchemaMutationRunnerCapability>,
}

#[allow(
    dead_code,
    reason = "0.152 stages runner preflight contracts before physical runners consume them"
)]
impl SchemaMutationRunnerContract {
    #[must_use]
    pub(in crate::db::schema) fn new(capabilities: &[SchemaMutationRunnerCapability]) -> Self {
        let mut deduped = Vec::new();

        for capability in capabilities {
            push_runner_capability_once(&mut deduped, *capability);
        }

        Self {
            capabilities: deduped,
        }
    }

    #[must_use]
    pub(in crate::db::schema) const fn capabilities(&self) -> &[SchemaMutationRunnerCapability] {
        self.capabilities.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) fn preflight(
        &self,
        execution_plan: &SchemaMutationExecutionPlan,
    ) -> SchemaMutationRunnerPreflight {
        match execution_plan.admit_runner_capabilities(self.capabilities()) {
            SchemaMutationExecutionAdmission::PublishableNow => {
                SchemaMutationRunnerPreflight::NoPhysicalWork
            }
            SchemaMutationExecutionAdmission::RunnerReady { required } => {
                SchemaMutationRunnerPreflight::Ready {
                    step_count: execution_plan.steps().len(),
                    required,
                }
            }
            SchemaMutationExecutionAdmission::MissingRunnerCapabilities { missing } => {
                SchemaMutationRunnerPreflight::MissingCapabilities { missing }
            }
            SchemaMutationExecutionAdmission::Rejected { requirement } => {
                SchemaMutationRunnerPreflight::Rejected { requirement }
            }
        }
    }

    #[must_use]
    pub(in crate::db::schema) fn outcome(
        &self,
        execution_plan: &SchemaMutationExecutionPlan,
    ) -> SchemaMutationRunnerOutcome {
        match self.preflight(execution_plan) {
            SchemaMutationRunnerPreflight::NoPhysicalWork => {
                SchemaMutationRunnerOutcome::NoPhysicalWork(
                    SchemaMutationRunnerReport::preflight_ready(0, Vec::new(), None),
                )
            }
            SchemaMutationRunnerPreflight::Ready {
                step_count,
                required,
            } => SchemaMutationRunnerOutcome::ReadyForPhysicalWork(
                SchemaMutationRunnerReport::preflight_ready(
                    step_count,
                    required,
                    Some(SchemaMutationStoreVisibility::StagedOnly),
                ),
            ),
            SchemaMutationRunnerPreflight::MissingCapabilities { missing } => {
                SchemaMutationRunnerOutcome::Rejected(
                    SchemaMutationRunnerRejection::missing_runner_capabilities(
                        execution_plan.physical_requirement(),
                        missing,
                    ),
                )
            }
            SchemaMutationRunnerPreflight::Rejected { requirement } => {
                SchemaMutationRunnerOutcome::Rejected(
                    SchemaMutationRunnerRejection::unsupported_requirement(requirement),
                )
            }
        }
    }
}

///
/// SchemaMutationExecutionGate
///
/// Schema-owned publish gate derived from an execution plan. It is the narrow
/// answer future callers need before deciding whether to publish, invoke a
/// physical runner, or reject the mutation.
///

#[allow(
    dead_code,
    reason = "0.152 stages execution-boundary contracts before physical runners consume them"
)]
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db::schema) enum SchemaMutationExecutionGate {
    ReadyToPublish,
    AwaitingPhysicalWork {
        requirement: RebuildRequirement,
        step_count: usize,
    },
    Rejected {
        requirement: RebuildRequirement,
    },
}

///
/// SchemaMutationExecutionPlan
///
/// Execution-facing form of a mutation plan. It keeps the future physical
/// runner contract separate from rebuild classification and publication
/// policy, so adding execution cannot silently widen startup reconciliation.
///

#[allow(
    dead_code,
    reason = "0.152 stages execution-boundary contracts before physical runners consume them"
)]
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db::schema) struct SchemaMutationExecutionPlan {
    readiness: SchemaMutationExecutionReadiness,
    steps: Vec<SchemaMutationExecutionStep>,
}

#[expect(
    dead_code,
    reason = "0.152 stages execution-boundary contracts before physical runners consume them"
)]
impl SchemaMutationExecutionPlan {
    const fn publishable_now() -> Self {
        Self {
            readiness: SchemaMutationExecutionReadiness::PublishableNow,
            steps: Vec::new(),
        }
    }

    pub(in crate::db::schema::mutation) fn from_rebuild_plan(
        rebuild_plan: SchemaRebuildPlan,
    ) -> Self {
        if !rebuild_plan.requires_physical_work() {
            return Self::publishable_now();
        }

        let readiness = match rebuild_plan.requirement() {
            RebuildRequirement::NoRebuildRequired => {
                SchemaMutationExecutionReadiness::PublishableNow
            }
            RebuildRequirement::IndexRebuildRequired => {
                SchemaMutationExecutionReadiness::RequiresPhysicalRunner(
                    RebuildRequirement::IndexRebuildRequired,
                )
            }
            RebuildRequirement::FullDataRewriteRequired | RebuildRequirement::Unsupported => {
                SchemaMutationExecutionReadiness::Unsupported(rebuild_plan.requirement())
            }
        };

        let mut steps = rebuild_plan
            .actions()
            .iter()
            .map(|action| match action {
                SchemaRebuildAction::BuildFieldPathIndex { target } => {
                    SchemaMutationExecutionStep::BuildFieldPathIndex {
                        target: target.clone(),
                    }
                }
                SchemaRebuildAction::BuildExpressionIndex { target } => {
                    SchemaMutationExecutionStep::BuildExpressionIndex {
                        target: target.clone(),
                    }
                }
                SchemaRebuildAction::DropSecondaryIndex { target } => {
                    SchemaMutationExecutionStep::DropSecondaryIndex {
                        target: target.clone(),
                    }
                }
                SchemaRebuildAction::RewriteAllRows => SchemaMutationExecutionStep::RewriteAllRows,
                SchemaRebuildAction::Unsupported { reason } => {
                    SchemaMutationExecutionStep::Unsupported { reason }
                }
            })
            .collect::<Vec<_>>();

        if matches!(
            readiness,
            SchemaMutationExecutionReadiness::RequiresPhysicalRunner(_)
        ) {
            steps.push(SchemaMutationExecutionStep::ValidatePhysicalWork);
            steps.push(SchemaMutationExecutionStep::InvalidateRuntimeState);
        }

        Self { readiness, steps }
    }

    #[must_use]
    pub(in crate::db::schema) const fn readiness(&self) -> SchemaMutationExecutionReadiness {
        self.readiness
    }

    #[must_use]
    pub(in crate::db::schema) const fn steps(&self) -> &[SchemaMutationExecutionStep] {
        self.steps.as_slice()
    }

    #[must_use]
    pub(in crate::db::schema) const fn execution_gate(&self) -> SchemaMutationExecutionGate {
        match self.readiness {
            SchemaMutationExecutionReadiness::PublishableNow => {
                SchemaMutationExecutionGate::ReadyToPublish
            }
            SchemaMutationExecutionReadiness::RequiresPhysicalRunner(requirement) => {
                SchemaMutationExecutionGate::AwaitingPhysicalWork {
                    requirement,
                    step_count: self.steps.len(),
                }
            }
            SchemaMutationExecutionReadiness::Unsupported(requirement) => {
                SchemaMutationExecutionGate::Rejected { requirement }
            }
        }
    }

    #[must_use]
    const fn physical_requirement(&self) -> Option<RebuildRequirement> {
        match self.execution_gate() {
            SchemaMutationExecutionGate::ReadyToPublish => None,
            SchemaMutationExecutionGate::AwaitingPhysicalWork { requirement, .. }
            | SchemaMutationExecutionGate::Rejected { requirement } => Some(requirement),
        }
    }

    #[must_use]
    pub(in crate::db::schema) fn runner_capabilities(&self) -> Vec<SchemaMutationRunnerCapability> {
        let mut capabilities = Vec::new();

        for step in &self.steps {
            let capability = match step {
                SchemaMutationExecutionStep::BuildFieldPathIndex { .. } => {
                    Some(SchemaMutationRunnerCapability::BuildFieldPathIndex)
                }
                SchemaMutationExecutionStep::BuildExpressionIndex { .. } => {
                    Some(SchemaMutationRunnerCapability::BuildExpressionIndex)
                }
                SchemaMutationExecutionStep::DropSecondaryIndex { .. } => {
                    Some(SchemaMutationRunnerCapability::DropSecondaryIndex)
                }
                SchemaMutationExecutionStep::ValidatePhysicalWork => {
                    Some(SchemaMutationRunnerCapability::ValidatePhysicalWork)
                }
                SchemaMutationExecutionStep::InvalidateRuntimeState => {
                    Some(SchemaMutationRunnerCapability::InvalidateRuntimeState)
                }
                SchemaMutationExecutionStep::RewriteAllRows => {
                    Some(SchemaMutationRunnerCapability::RewriteAllRows)
                }
                SchemaMutationExecutionStep::Unsupported { .. } => None,
            };

            if let Some(capability) = capability {
                push_runner_capability_once(&mut capabilities, capability);
            }
        }

        capabilities
    }

    #[must_use]
    pub(in crate::db::schema) fn admit_runner_capabilities(
        &self,
        available: &[SchemaMutationRunnerCapability],
    ) -> SchemaMutationExecutionAdmission {
        match self.execution_gate() {
            SchemaMutationExecutionGate::ReadyToPublish => {
                SchemaMutationExecutionAdmission::PublishableNow
            }
            SchemaMutationExecutionGate::Rejected { requirement } => {
                SchemaMutationExecutionAdmission::Rejected { requirement }
            }
            SchemaMutationExecutionGate::AwaitingPhysicalWork { .. } => {
                let required = self.runner_capabilities();
                let missing = required
                    .iter()
                    .copied()
                    .filter(|capability| !available.contains(capability))
                    .collect::<Vec<_>>();

                if missing.is_empty() {
                    SchemaMutationExecutionAdmission::RunnerReady { required }
                } else {
                    SchemaMutationExecutionAdmission::MissingRunnerCapabilities { missing }
                }
            }
        }
    }

    #[must_use]
    fn has_unsupported_supported_path_step(&self) -> bool {
        self.steps.iter().any(|step| {
            matches!(
                step,
                SchemaMutationExecutionStep::BuildExpressionIndex { .. }
                    | SchemaMutationExecutionStep::DropSecondaryIndex { .. }
                    | SchemaMutationExecutionStep::RewriteAllRows
                    | SchemaMutationExecutionStep::Unsupported { .. }
            )
        })
    }

    /// Admit the single developer-supported physical mutation path for 0.154.
    /// The generic execution plan may still describe future expression-index,
    /// cleanup, or rewrite work, but those shapes are rejected here before
    /// runner wiring can consume them as supported behavior.
    #[allow(
        dead_code,
        reason = "0.154 starts supported-path admission before reconciliation consumes it"
    )]
    pub(in crate::db::schema) fn supported_developer_execution_path(
        &self,
    ) -> Result<SchemaMutationSupportedExecutionPath, SchemaMutationSupportedPathRejection> {
        match self.readiness {
            SchemaMutationExecutionReadiness::PublishableNow => {
                return Err(SchemaMutationSupportedPathRejection::NoPhysicalWork);
            }
            SchemaMutationExecutionReadiness::RequiresPhysicalRunner(
                RebuildRequirement::IndexRebuildRequired,
            ) => {}
            SchemaMutationExecutionReadiness::Unsupported(requirement)
            | SchemaMutationExecutionReadiness::RequiresPhysicalRunner(requirement) => {
                return Err(
                    SchemaMutationSupportedPathRejection::UnsupportedRequirement(requirement),
                );
            }
        }

        let [
            SchemaMutationExecutionStep::BuildFieldPathIndex { target },
            SchemaMutationExecutionStep::ValidatePhysicalWork,
            SchemaMutationExecutionStep::InvalidateRuntimeState,
        ] = self.steps.as_slice()
        else {
            return if self.has_unsupported_supported_path_step() {
                Err(SchemaMutationSupportedPathRejection::UnsupportedMutationKind)
            } else {
                Err(SchemaMutationSupportedPathRejection::UnsupportedExecutionShape)
            };
        };

        if target.key_paths().is_empty() {
            return Err(SchemaMutationSupportedPathRejection::EmptyFieldPathKey);
        }

        Ok(SchemaMutationSupportedExecutionPath::new(target.clone()))
    }
}

#[allow(
    dead_code,
    reason = "0.152 stages runner capability contracts before physical runners consume them"
)]
fn push_runner_capability_once(
    capabilities: &mut Vec<SchemaMutationRunnerCapability>,
    capability: SchemaMutationRunnerCapability,
) {
    if !capabilities.contains(&capability) {
        capabilities.push(capability);
    }
}
