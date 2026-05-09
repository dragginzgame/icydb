use crate::{
    db::{
        access::LoweredKey,
        codec::hex::encode_hex_lower,
        executor::{
            PreparedExecutionPlan,
            planning::route::{
                LoadTerminalFastPathContract, derive_load_terminal_fast_path_contract_for_plan,
            },
        },
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
};
use std::ops::Bound;

impl<E: EntityKind> PreparedExecutionPlan<E> {
    /// Render one canonical executor snapshot for test-only planner/executor
    /// contract checks.
    pub(in crate::db) fn render_snapshot_canonical(&self) -> Result<String, InternalError>
    where
        E: EntityValue,
    {
        // Phase 1: project all executor-owned summary fields from the logical plan.
        let plan = self.core.plan();
        let authority = self.authority.clone();
        let projection_spec = plan.frozen_projection_spec();
        let projection_selection = if plan.grouped_plan().is_some()
            || projection_spec.len() != authority.row_layout().field_count()
        {
            "Declared"
        } else {
            "All"
        };
        let projection_coverage_flag = plan.grouped_plan().is_some();
        let continuation_signature = self.core.continuation_signature_for_runtime()?;
        let ordering_direction = self
            .core
            .continuation_contract()?
            .order_contract()
            .direction();
        let load_terminal_fast_path =
            derive_load_terminal_fast_path_contract_for_plan(authority, plan);

        // Phase 2: lower index-bound summaries into stable compact text.
        let render_lowered_bound = |bound: &Bound<LoweredKey>| match bound {
            Bound::Included(key) => {
                let bytes = key.as_bytes();
                let head_len = bytes.len().min(8);
                let tail_len = bytes.len().min(8);
                let head = encode_hex_lower(&bytes[..head_len]);
                let tail = encode_hex_lower(&bytes[bytes.len() - tail_len..]);

                format!("included(len:{}:head:{head}:tail:{tail})", bytes.len())
            }
            Bound::Excluded(key) => {
                let bytes = key.as_bytes();
                let head_len = bytes.len().min(8);
                let tail_len = bytes.len().min(8);
                let head = encode_hex_lower(&bytes[..head_len]);
                let tail = encode_hex_lower(&bytes[bytes.len() - tail_len..]);

                format!("excluded(len:{}:head:{head}:tail:{tail})", bytes.len())
            }
            Bound::Unbounded => "unbounded".to_string(),
        };
        let index_prefix_specs = format!(
            "[{}]",
            self.core
                .index_prefix_specs()?
                .iter()
                .map(|spec| {
                    format!(
                        "{{index:{},bound_type:equality,lower:{},upper:{}}}",
                        spec.index().name(),
                        render_lowered_bound(spec.lower()),
                        render_lowered_bound(spec.upper()),
                    )
                })
                .collect::<Vec<_>>()
                .join(",")
        );
        let index_range_specs = format!(
            "[{}]",
            self.core
                .index_range_specs()?
                .iter()
                .map(|spec| {
                    format!(
                        "{{index:{},lower:{},upper:{}}}",
                        spec.index().name(),
                        render_lowered_bound(spec.lower()),
                        render_lowered_bound(spec.upper()),
                    )
                })
                .collect::<Vec<_>>()
                .join(",")
        );
        let explain_plan = plan.explain();

        // Phase 3: join the canonical snapshot payload in one stable line order.
        Ok([
            "snapshot_version=1".to_string(),
            format!("plan_hash={}", plan.fingerprint()),
            format!("mode={:?}", self.core.mode()),
            format!("is_grouped={}", self.core.is_grouped()),
            format!("execution_family={:?}", self.core.execution_family()?),
            format!(
                "load_terminal_fast_path={}",
                match load_terminal_fast_path.as_ref() {
                    Some(LoadTerminalFastPathContract::CoveringRead(_)) => "CoveringRead",
                    None => "Materialized",
                }
            ),
            format!("ordering_direction={ordering_direction:?}"),
            format!(
                "distinct_execution_strategy={:?}",
                plan.distinct_execution_strategy()
            ),
            format!("projection_selection={projection_selection}"),
            format!("projection_spec={projection_spec:?}"),
            format!("order_spec={:?}", plan.scalar_plan().order),
            format!("page_spec={:?}", plan.scalar_plan().page),
            format!("projection_coverage_flag={projection_coverage_flag}"),
            format!("continuation_signature={continuation_signature}"),
            format!("index_prefix_specs={index_prefix_specs}"),
            format!("index_range_specs={index_range_specs}"),
            format!("explain_plan={explain_plan:?}"),
        ]
        .join("\n"))
    }
}
