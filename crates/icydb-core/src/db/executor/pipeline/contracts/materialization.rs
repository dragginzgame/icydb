//! Module: executor::pipeline::contracts::materialization
//! Responsibility: scalar materialization request DTOs shared across executor phases.
//! Does not own: row decoding, page-kernel execution, or post-access materialization.
//! Boundary: data-only output-side request shapes consumed by terminal runtime.

use crate::db::{
    direction::Direction,
    executor::{
        EntityAuthority, OrderedKeyStream, ScalarContinuationContext,
        pipeline::contracts::CursorEmissionMode, projection::PreparedSlotProjectionValidation,
        route::LoadOrderRouteContract, terminal::RetainedSlotLayout,
    },
    predicate::MissingRowPolicy,
    query::plan::{AccessPlannedQuery, EffectiveRuntimeFilterProgram},
};

///
/// ScalarMaterializationCapabilities
///
/// ScalarMaterializationCapabilities carries the raw scalar-page execution
/// capabilities recovered before the terminal runtime runs.
/// It is intentionally capability-only data: the terminal resolver decides
/// policy from this bundle once instead of reinterpreting layout and cursor
/// fields across multiple sibling branches.
///

#[derive(Clone, Copy)]
pub(in crate::db::executor) struct ScalarMaterializationCapabilities<'a> {
    pub(in crate::db::executor) residual_filter_program: Option<&'a EffectiveRuntimeFilterProgram>,
    pub(in crate::db::executor) validate_projection: bool,
    pub(in crate::db::executor) retain_slot_rows: bool,
    pub(in crate::db::executor) retained_slot_layout: Option<&'a RetainedSlotLayout>,
    pub(in crate::db::executor) prepared_projection_validation:
        Option<&'a PreparedSlotProjectionValidation>,
    pub(in crate::db::executor) cursor_emission: CursorEmissionMode,
}

///
/// KernelPageMaterializationRequest
///
/// Structural inputs for one shared scalar page-materialization pass.
/// This keeps the kernel loop monomorphic while boundary adapters supply only
/// store access and outer typed response reconstruction.
///

pub(in crate::db::executor) struct KernelPageMaterializationRequest<'a> {
    pub(in crate::db::executor) authority: EntityAuthority,
    pub(in crate::db::executor) plan: &'a AccessPlannedQuery,
    pub(in crate::db::executor) key_stream: &'a mut dyn OrderedKeyStream,
    pub(in crate::db::executor) scan_budget_hint: Option<usize>,
    pub(in crate::db::executor) load_order_route_contract: LoadOrderRouteContract,
    pub(in crate::db::executor) capabilities: ScalarMaterializationCapabilities<'a>,
    pub(in crate::db::executor) consistency: MissingRowPolicy,
    pub(in crate::db::executor) continuation: &'a ScalarContinuationContext,
    pub(in crate::db::executor) direction: Direction,
}
