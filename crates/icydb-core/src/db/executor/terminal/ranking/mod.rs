//! Module: executor::terminal::ranking
//! Responsibility: ranking terminal selection (`min/max` and `*_by`) for read execution.
//! Does not own: planner aggregate semantics or projection-expression evaluation.
//! Boundary: consumes planned slots and returns entity response terminals.

mod by_slot;
mod materialized;
mod take;

use crate::{
    db::{
        PersistedRow,
        data::DataKey,
        executor::{
            PreparedLoadPlan,
            aggregate::field::{
                AggregateFieldValueError,
                resolve_orderable_aggregate_target_slot_from_planner_slot_with_model,
            },
            pipeline::{contracts::LoadExecutor, entrypoints::PreparedScalarMaterializedBoundary},
        },
        query::plan::FieldSlot as PlannedFieldSlot,
        response::EntityResponse,
    },
    error::InternalError,
    traits::{EntityKind, EntityValue},
    types::Id,
    value::Value,
};

// Typed boundary request for one scalar ranking terminal family call.
enum RankingTerminalBoundaryRequest {
    Take {
        take_count: u32,
    },
    TopKRows {
        target_field: PlannedFieldSlot,
        take_count: u32,
    },
    BottomKRows {
        target_field: PlannedFieldSlot,
        take_count: u32,
    },
    TopKValues {
        target_field: PlannedFieldSlot,
        take_count: u32,
    },
    BottomKValues {
        target_field: PlannedFieldSlot,
        take_count: u32,
    },
    TopKValuesWithIds {
        target_field: PlannedFieldSlot,
        take_count: u32,
    },
    BottomKValuesWithIds {
        target_field: PlannedFieldSlot,
        take_count: u32,
    },
}

// Internal projection selector for ranked field terminal boundaries.
enum RankedFieldBoundaryProjection {
    Rows,
    Values,
    ValuesWithIds,
}

// Internal direction selector for ranked field terminal boundaries.
enum RankedFieldBoundaryDirection {
    Top,
    Bottom,
}

// Typed boundary output for one scalar ranking terminal family call.
enum RankingTerminalBoundaryOutput<E: EntityKind + EntityValue> {
    Rows(EntityResponse<E>),
    Values(Vec<Value>),
    ValuesWithDataKeys(Vec<(DataKey, Value)>),
}

impl<E> RankingTerminalBoundaryOutput<E>
where
    E: EntityKind + EntityValue,
{
    // Decode row-returning ranking boundary output.
    fn into_rows(self) -> Result<EntityResponse<E>, InternalError> {
        match self {
            Self::Rows(rows) => Ok(rows),
            _ => Err(crate::db::error::query_executor_invariant(
                "ranking terminal boundary rows output kind mismatch",
            )),
        }
    }

    // Decode value-returning ranking boundary output.
    fn into_values(self) -> Result<Vec<Value>, InternalError> {
        match self {
            Self::Values(values) => Ok(values),
            _ => Err(crate::db::error::query_executor_invariant(
                "ranking terminal boundary values output kind mismatch",
            )),
        }
    }

    // Decode `(id, value)` ranking boundary output.
    fn into_values_with_ids(self) -> Result<Vec<(Id<E>, Value)>, InternalError> {
        match self {
            Self::ValuesWithDataKeys(values) => values
                .into_iter()
                .map(|(data_key, value)| Ok((Id::from_key(data_key.try_key::<E>()?), value)))
                .collect(),
            _ => Err(crate::db::error::query_executor_invariant(
                "ranking terminal boundary values-with-ids output kind mismatch",
            )),
        }
    }
}

impl<E> LoadExecutor<E>
where
    E: PersistedRow + EntityValue,
{
    // Execute one scalar ranking terminal family request from the typed API
    // boundary and immediately hand off to shared materialized ranking logic.
    fn execute_ranking_terminal_boundary(
        &self,
        plan: PreparedLoadPlan,
        request: RankingTerminalBoundaryRequest,
    ) -> Result<RankingTerminalBoundaryOutput<E>, InternalError> {
        let prepared = self.prepare_scalar_materialized_boundary(plan)?;

        self.execute_prepared_ranking_terminal_boundary(prepared, request)
    }

    // Execute one scalar ranking terminal family request from the neutral
    // non-aggregate prepared boundary payload.
    fn execute_prepared_ranking_terminal_boundary(
        &self,
        prepared: PreparedScalarMaterializedBoundary<'_>,
        request: RankingTerminalBoundaryRequest,
    ) -> Result<RankingTerminalBoundaryOutput<E>, InternalError> {
        match request {
            RankingTerminalBoundaryRequest::Take { take_count } => {
                let page = self.execute_scalar_materialized_page_boundary(prepared)?;
                let (mut data_rows, _) = page.into_parts();
                let take_len = usize::try_from(take_count).unwrap_or(usize::MAX);
                if data_rows.len() > take_len {
                    data_rows.truncate(take_len);
                }

                Ok(RankingTerminalBoundaryOutput::Rows(
                    EntityResponse::from_data_rows(data_rows)?,
                ))
            }
            RankingTerminalBoundaryRequest::TopKRows {
                target_field,
                take_count,
            } => self.execute_ranked_field_boundary(
                prepared,
                target_field,
                take_count,
                RankedFieldBoundaryDirection::Top,
                RankedFieldBoundaryProjection::Rows,
            ),
            RankingTerminalBoundaryRequest::BottomKRows {
                target_field,
                take_count,
            } => self.execute_ranked_field_boundary(
                prepared,
                target_field,
                take_count,
                RankedFieldBoundaryDirection::Bottom,
                RankedFieldBoundaryProjection::Rows,
            ),
            RankingTerminalBoundaryRequest::TopKValues {
                target_field,
                take_count,
            } => self.execute_ranked_field_boundary(
                prepared,
                target_field,
                take_count,
                RankedFieldBoundaryDirection::Top,
                RankedFieldBoundaryProjection::Values,
            ),
            RankingTerminalBoundaryRequest::BottomKValues {
                target_field,
                take_count,
            } => self.execute_ranked_field_boundary(
                prepared,
                target_field,
                take_count,
                RankedFieldBoundaryDirection::Bottom,
                RankedFieldBoundaryProjection::Values,
            ),
            RankingTerminalBoundaryRequest::TopKValuesWithIds {
                target_field,
                take_count,
            } => self.execute_ranked_field_boundary(
                prepared,
                target_field,
                take_count,
                RankedFieldBoundaryDirection::Top,
                RankedFieldBoundaryProjection::ValuesWithIds,
            ),
            RankingTerminalBoundaryRequest::BottomKValuesWithIds {
                target_field,
                take_count,
            } => self.execute_ranked_field_boundary(
                prepared,
                target_field,
                take_count,
                RankedFieldBoundaryDirection::Bottom,
                RankedFieldBoundaryProjection::ValuesWithIds,
            ),
        }
    }

    // Execute one ranked field terminal after slot resolution at the typed
    // boundary and project the requested output shape.
    fn execute_ranked_field_boundary(
        &self,
        prepared: PreparedScalarMaterializedBoundary<'_>,
        target_field: PlannedFieldSlot,
        take_count: u32,
        direction: RankedFieldBoundaryDirection,
        projection: RankedFieldBoundaryProjection,
    ) -> Result<RankingTerminalBoundaryOutput<E>, InternalError> {
        let model = prepared.authority.model();
        let field_slot = resolve_orderable_aggregate_target_slot_from_planner_slot_with_model(
            model,
            &target_field,
        )
        .map_err(AggregateFieldValueError::into_internal_error)?;
        let page = self.execute_scalar_materialized_page_boundary(prepared)?;
        let data_rows = page.data_rows();
        let target_field = target_field.field();

        match (direction, projection) {
            (RankedFieldBoundaryDirection::Top, RankedFieldBoundaryProjection::Rows) => {
                Self::top_k_field_from_materialized(
                    model,
                    data_rows,
                    target_field,
                    field_slot,
                    take_count,
                )
                .map(RankingTerminalBoundaryOutput::Rows)
            }
            (RankedFieldBoundaryDirection::Bottom, RankedFieldBoundaryProjection::Rows) => {
                Self::bottom_k_field_from_materialized(
                    model,
                    data_rows,
                    target_field,
                    field_slot,
                    take_count,
                )
                .map(RankingTerminalBoundaryOutput::Rows)
            }
            (RankedFieldBoundaryDirection::Top, RankedFieldBoundaryProjection::Values) => {
                Self::top_k_field_values_from_materialized(
                    model,
                    data_rows,
                    target_field,
                    field_slot,
                    take_count,
                )
                .map(RankingTerminalBoundaryOutput::Values)
            }
            (RankedFieldBoundaryDirection::Bottom, RankedFieldBoundaryProjection::Values) => {
                Self::bottom_k_field_values_from_materialized(
                    model,
                    data_rows,
                    target_field,
                    field_slot,
                    take_count,
                )
                .map(RankingTerminalBoundaryOutput::Values)
            }
            (RankedFieldBoundaryDirection::Top, RankedFieldBoundaryProjection::ValuesWithIds) => {
                Self::top_k_field_values_with_ids_from_materialized(
                    model,
                    data_rows,
                    target_field,
                    field_slot,
                    take_count,
                )
                .map(RankingTerminalBoundaryOutput::ValuesWithDataKeys)
            }
            (
                RankedFieldBoundaryDirection::Bottom,
                RankedFieldBoundaryProjection::ValuesWithIds,
            ) => Self::bottom_k_field_values_with_ids_from_materialized(
                model,
                data_rows,
                target_field,
                field_slot,
                take_count,
            )
            .map(RankingTerminalBoundaryOutput::ValuesWithDataKeys),
        }
    }
}
