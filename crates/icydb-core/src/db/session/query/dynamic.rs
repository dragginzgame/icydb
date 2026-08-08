//! Module: db::session::query::dynamic
//! Responsibility: lower and execute public dynamic reads against accepted schema.
//! Does not own: query planning, accepted schema construction, or row projection.
//! Boundary: entity-name requests converge on the shared structural read lane.

use crate::{
    db::{
        DbSession, DynamicQuery, DynamicTypedEntityBinding, ExhaustiveQueryPageOutput,
        ExhaustiveReadError, GroupedQueryOutput, LiveQueryPageOutput, MissingRowPolicy, QueryError,
        ReadSetRevisionError, ReadSetRevisionProof, ScalarPageWork,
        codec::{finalize_hash_sha256, new_hash_sha256_prefixed},
        commit::{cursor_authentication_key, database_incarnation_id},
        cursor::{
            CursorBoundary, CursorBoundarySlot, CursorPlanError, ScalarOrderTermContract,
            ScalarPageMode, ScalarPageToken, ScalarPageTokenAuthority, ScalarPageTokenProgress,
            ScalarPageTokenWindow, decode_optional_cursor_token, encode_cursor,
        },
        data::{DecodedDataStoreKey, RawDataStoreKey},
        executor::{
            CoveringProjectionMetricsRecorder, PageWorkEnvelope,
            ProjectionMaterializationMetricsRecorder, ScalarContinuationContext,
            StructuralProjectionRequest, execute_structural_projection_page,
        },
        query::{
            admission::{QueryAdmissionPolicy, QueryAdmissionSummary},
            expr::{FilterExpr, OrderTerm as FluentOrderTerm},
            intent::{IntentError, StructuralQuery},
        },
        session::AcceptedSchemaCatalogContext,
    },
    traits::CanisterKind,
};
use icydb_diagnostic_code::{
    DiagnosticDecodeReason, DiagnosticExecutionBudgetResource, DiagnosticExecutionLane,
    QueryReadAdmissionCode,
};
use sha2::Digest;
#[cfg(test)]
use std::cell::Cell;

#[cfg(not(test))]
const SCALAR_PAGE_OUTPUT_ROWS: usize = 1_024;
#[cfg(test)]
const SCALAR_PAGE_OUTPUT_ROWS: usize = 2;
#[cfg(test)]
const SCALAR_PAGE_KEY_ENTRIES: u64 = 4;

#[cfg(test)]
std::thread_local! {
    static SCALAR_PAGE_RESULT_BYTES_LIMIT_OVERRIDE: Cell<Option<u64>> = const { Cell::new(None) };
}

#[cfg(test)]
struct ScalarPageResultBytesLimitGuard(Option<u64>);

#[cfg(test)]
impl Drop for ScalarPageResultBytesLimitGuard {
    fn drop(&mut self) {
        SCALAR_PAGE_RESULT_BYTES_LIMIT_OVERRIDE.with(|limit| limit.set(self.0));
    }
}

#[derive(Clone, Copy)]
enum DynamicReadLane {
    Public,
    Trusted,
}

struct ScalarCursorContract {
    signature: crate::db::cursor::ContinuationSignature,
    authority: ScalarPageTokenAuthority,
    window: ScalarPageTokenWindow,
    order_terms: Vec<ScalarOrderTermContract>,
}

impl<C: CanisterKind> DbSession<C> {
    fn may_select_exact_single_primary_key(
        request: &DynamicQuery,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> bool {
        let [primary_key] = catalog.accepted_schema_info().primary_key_names() else {
            return false;
        };
        matches!(
            request.filter_expr(),
            Some(FilterExpr::Eq { field, .. } | FilterExpr::In { field, .. })
                if field.eq_ignore_ascii_case(primary_key)
        )
    }

    fn exact_primary_key_candidate_bound(
        prepared_plan: &crate::db::executor::SharedPreparedExecutionPlan,
    ) -> Option<usize> {
        let access = &prepared_plan.logical_plan().access;
        if access.as_by_key_path().is_some() {
            return Some(1);
        }

        access.as_by_keys_path().map(<[crate::value::Value]>::len)
    }

    fn structural_query_from_dynamic_request(
        request: &DynamicQuery,
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<StructuralQuery, QueryError> {
        Self::structural_query_from_dynamic_request_with_page_limit(request, catalog, None, false)
    }

    fn structural_query_from_dynamic_request_with_page_limit(
        request: &DynamicQuery,
        catalog: &AcceptedSchemaCatalogContext,
        page_limit: Option<u32>,
        require_total_order: bool,
    ) -> Result<StructuralQuery, QueryError> {
        let schema = catalog.accepted_schema_info();
        let mut query = StructuralQuery::new(MissingRowPolicy::Ignore);
        if let Some(filter) = request.filter_expr() {
            query = query.filter_for_schema(schema, filter.clone());
        }
        for order in request.order_terms() {
            query = query.order_term(order.clone());
        }
        if require_total_order && request.order_terms().is_empty() {
            for primary_key in schema.primary_key_names() {
                query = query.order_term(FluentOrderTerm::asc(primary_key.clone()));
            }
        }
        if !request.selected_fields().is_empty() {
            query = query.select_fields(request.selected_fields().iter().cloned());
        }
        if let Some(limit) = page_limit.or_else(|| request.row_limit()) {
            query = query.limit(limit);
        }
        for field in request.group_fields() {
            query = query.group_by_with_schema(field, schema)?;
        }
        for aggregate in request.aggregates() {
            query = query.aggregate(aggregate.clone());
        }
        if let Some((max_groups, max_group_bytes)) = request.grouped_execution_limits() {
            if max_groups == 0 || max_group_bytes == 0 {
                return Err(QueryReadAdmissionCode::GroupedQueryRequiresLimits.into());
            }
            query = query.grouped_limits(u64::from(max_groups), u64::from(max_group_bytes));
        }

        Ok(query)
    }

    fn scalar_page_cursor_error() -> QueryError {
        QueryError::from_cursor_plan_error(CursorPlanError::invalid_continuation_cursor_payload(
            DiagnosticDecodeReason::CursorTokenDecode,
        ))
    }

    fn scalar_cursor_contract(
        request: &DynamicQuery,
        catalog: &AcceptedSchemaCatalogContext,
        envelope: PageWorkEnvelope,
        prepared_plan: &crate::db::executor::SharedPreparedExecutionPlan,
        mode: ScalarPageMode,
        proof: Option<&ReadSetRevisionProof>,
    ) -> Result<ScalarCursorContract, QueryError> {
        let mut signature = prepared_plan
            .continuation_signature_for_runtime()
            .map_err(QueryError::execute)?;
        match (mode, proof) {
            (ScalarPageMode::Live, None) => {}
            (ScalarPageMode::Exhaustive, Some(proof)) => {
                let mut hasher = new_hash_sha256_prefixed(b"icydb.exhaustive-cursor-proof.v1");
                hasher.update(signature.into_bytes());
                hasher.update(proof.signature_bytes());
                signature = crate::db::cursor::ContinuationSignature::from_bytes(
                    finalize_hash_sha256(hasher),
                );
            }
            _ => return Err(Self::scalar_page_cursor_error()),
        }
        let root_identity = catalog.runtime_root_identity();
        let (root_fingerprint_method, root_fingerprint) = root_identity.fingerprint();
        let authority = ScalarPageTokenAuthority::new(
            database_incarnation_id()
                .map_err(QueryError::execute)?
                .to_bytes(),
            root_identity.accepted_root_revision().get(),
            root_fingerprint_method,
            root_fingerprint,
            catalog.fingerprint(),
            prepared_plan.authority_ref().entity_tag(),
        );
        let window = ScalarPageTokenWindow::new(0, request.row_limit(), envelope.identity());
        let canonical_order = prepared_plan
            .logical_plan()
            .scalar_plan()
            .order
            .as_ref()
            .ok_or_else(Self::scalar_page_cursor_error)?;
        let order_terms = canonical_order
            .fields
            .iter()
            .map(|term| ScalarOrderTermContract::new(term.rendered_label(), term.direction()))
            .collect::<Vec<_>>();

        Ok(ScalarCursorContract {
            signature,
            authority,
            window,
            order_terms,
        })
    }

    fn validate_scalar_page_token(
        token: &ScalarPageToken,
        mode: ScalarPageMode,
        signature: crate::db::cursor::ContinuationSignature,
        authority: ScalarPageTokenAuthority,
        window: ScalarPageTokenWindow,
        order_terms: &[ScalarOrderTermContract],
        entity: &str,
    ) -> Result<(), QueryError> {
        if token.signature() != signature {
            return Err(QueryError::from_cursor_plan_error(
                CursorPlanError::continuation_cursor_signature_mismatch(
                    entity,
                    &signature,
                    &token.signature(),
                ),
            ));
        }
        if token.mode() != mode
            || token.authority() != authority
            || token.window() != window
            || token.order_terms() != order_terms
        {
            return Err(Self::scalar_page_cursor_error());
        }

        Ok(())
    }

    fn physical_primary_key_boundary(
        bytes: &[u8],
        catalog: &AcceptedSchemaCatalogContext,
    ) -> Result<CursorBoundary, QueryError> {
        let raw = RawDataStoreKey::from_persisted_bytes(bytes.to_vec());
        let key = DecodedDataStoreKey::try_from_raw(&raw)
            .map_err(|_| Self::scalar_page_cursor_error())?;
        if key.entity_tag() != catalog.accepted_entity_authority().entity_tag() {
            return Err(Self::scalar_page_cursor_error());
        }
        let primary_key_arity = catalog.accepted_schema_info().primary_key_names().len();
        let mut slots = Vec::with_capacity(primary_key_arity);
        for component_index in 0..primary_key_arity {
            slots.push(CursorBoundarySlot::Present(
                key.primary_key_component_runtime_value(component_index)
                    .map_err(|_| Self::scalar_page_cursor_error())?,
            ));
        }

        Ok(CursorBoundary { slots })
    }

    fn execute_dynamic_grouped_query_against_catalog(
        &self,
        request: &DynamicQuery,
        lane: DynamicReadLane,
        catalog: AcceptedSchemaCatalogContext,
    ) -> Result<GroupedQueryOutput, QueryError> {
        if !request.has_grouping() {
            return Err(QueryError::intent(
                IntentError::grouped_terminal_requires_grouped_query(),
            ));
        }
        if request.grouped_execution_limits().is_none() {
            return Err(QueryReadAdmissionCode::GroupedQueryRequiresLimits.into());
        }
        if !request.selected_fields().is_empty() {
            return Err(QueryError::intent(
                IntentError::grouped_output_defined_by_group_and_aggregates(),
            ));
        }
        let query = Self::structural_query_from_dynamic_request(request, &catalog)?;
        let public_admission = match lane {
            DynamicReadLane::Public => Some(QueryAdmissionPolicy::default_bounded_read()),
            DynamicReadLane::Trusted => None,
        };

        self.execute_structural_grouped_from_query(
            &query,
            &catalog,
            public_admission.as_ref(),
            request.continuation_cursor(),
        )
    }

    #[expect(
        clippy::too_many_lines,
        reason = "live-page orchestration keeps planning, cursor validation, execution, and response proof in one auditable boundary"
    )]
    fn execute_scalar_page_against_catalog(
        &self,
        request: &DynamicQuery,
        continuation: Option<&str>,
        lane: DynamicReadLane,
        catalog: AcceptedSchemaCatalogContext,
        mode: ScalarPageMode,
        supplied_proof: Option<&ReadSetRevisionProof>,
    ) -> Result<(LiveQueryPageOutput, Option<ReadSetRevisionProof>), ExhaustiveReadError> {
        if request.has_grouping()
            || request.grouped_execution_limits().is_some()
            || request.continuation_cursor().is_some()
        {
            return Err(
                QueryError::intent(IntentError::scalar_terminal_requires_scalar_query()).into(),
            );
        }

        let exhaustive_proof = match mode {
            ScalarPageMode::Live => None,
            ScalarPageMode::Exhaustive => {
                if continuation.is_some() && supplied_proof.is_none() {
                    return Err(ReadSetRevisionError::ResumeProofRequired.into());
                }
                let proof = supplied_proof.cloned().map_or_else(
                    || self.capture_entity_read_set_revision_proof(catalog.identity().store_path()),
                    Ok,
                )?;
                Self::ensure_read_set_contains_store(&proof, catalog.identity().store_path())?;
                self.verify_read_set_revision_proof(&proof)?;
                Some(proof)
            }
        };

        let envelope = match lane {
            DynamicReadLane::Public => PageWorkEnvelope::public_scalar(),
            DynamicReadLane::Trusted => PageWorkEnvelope::default_scalar(),
        };
        #[cfg(test)]
        let envelope = SCALAR_PAGE_RESULT_BYTES_LIMIT_OVERRIDE.with(|limit| {
            limit.get().map_or(envelope, |limit| {
                envelope.with_limit_for_tests(DiagnosticExecutionBudgetResource::ResultBytes, limit)
            })
        });
        #[cfg(test)]
        let envelope = envelope.with_limit_for_tests(
            DiagnosticExecutionBudgetResource::KeyIndexEntriesVisited,
            SCALAR_PAGE_KEY_ENTRIES,
        );
        let page_row_limit = envelope
            .limit(DiagnosticExecutionBudgetResource::ResultRows)
            .and_then(|limit| usize::try_from(limit).ok())
            .unwrap_or(SCALAR_PAGE_OUTPUT_ROWS)
            .min(SCALAR_PAGE_OUTPUT_ROWS);
        let decoded_token = decode_optional_cursor_token(continuation)
            .map_err(QueryError::from_cursor_plan_error)?
            .map(|bytes| {
                ScalarPageToken::decode(
                    bytes.as_slice(),
                    &cursor_authentication_key().map_err(QueryError::execute)?,
                )
                .map_err(|error| {
                    QueryError::from_cursor_plan_error(CursorPlanError::from_token_wire_error(
                        error,
                    ))
                })
            })
            .transpose()?;
        let prior_rows_emitted = decoded_token
            .as_ref()
            .map_or(0, |token| token.progress().rows_emitted());
        let remaining_limit = request
            .row_limit()
            .map(|limit| u64::from(limit).saturating_sub(prior_rows_emitted));
        let page_output_limit = remaining_limit
            .unwrap_or(page_row_limit as u64)
            .min(page_row_limit as u64);
        let page_output_limit = usize::try_from(page_output_limit).unwrap_or(page_row_limit);
        let execution_limit = u32::try_from(page_row_limit).unwrap_or(u32::MAX);
        let execution_lane = match lane {
            DynamicReadLane::Public => DiagnosticExecutionLane::PublicRead,
            DynamicReadLane::Trusted => DiagnosticExecutionLane::TrustedRead,
        };
        let exact_candidate =
            decoded_token.is_none() && Self::may_select_exact_single_primary_key(request, &catalog);
        let initial_plan = if exact_candidate {
            let query = Self::structural_query_from_dynamic_request(request, &catalog)?;
            Some(
                self.structural_projection_prepared_plan_for_accepted_authority(
                    &query,
                    catalog.accepted_entity_authority(),
                    catalog.snapshot(),
                    execution_lane,
                )?,
            )
        } else {
            None
        };
        let initial_is_exact_exhaustion =
            initial_plan.as_ref().is_some_and(|(prepared_plan, _, _)| {
                Self::exact_primary_key_candidate_bound(prepared_plan)
                    .is_some_and(|bound| bound == 1 && bound <= page_output_limit)
            });
        let (prepared_plan, projection, _) = if initial_is_exact_exhaustion {
            initial_plan.ok_or_else(Self::scalar_page_cursor_error)?
        } else {
            let query = Self::structural_query_from_dynamic_request_with_page_limit(
                request,
                &catalog,
                Some(execution_limit),
                true,
            )?;
            self.structural_projection_prepared_plan_for_accepted_authority(
                &query,
                catalog.accepted_entity_authority(),
                catalog.snapshot(),
                execution_lane,
            )?
        };
        if matches!(lane, DynamicReadLane::Public) {
            let policy = QueryAdmissionPolicy::default_bounded_read();
            let summary = policy.evaluate(QueryAdmissionSummary::from_plan(
                policy.lane(),
                prepared_plan.logical_plan(),
            ));
            if let Some(rejection) = summary.rejection() {
                return Err(QueryError::from(rejection.code()).into());
            }
        }

        let exact_initial_exhaustion = initial_is_exact_exhaustion;
        let cursor_contract = decoded_token
            .as_ref()
            .map(|token| {
                let contract = Self::scalar_cursor_contract(
                    request,
                    &catalog,
                    envelope,
                    &prepared_plan,
                    mode,
                    exhaustive_proof.as_ref(),
                )?;
                Self::validate_scalar_page_token(
                    token,
                    mode,
                    contract.signature,
                    contract.authority,
                    contract.window,
                    contract.order_terms.as_slice(),
                    request.entity(),
                )?;
                Ok::<_, QueryError>(contract)
            })
            .transpose()?;
        let deferred_cursor_plan =
            (!exact_initial_exhaustion && decoded_token.is_none()).then(|| prepared_plan.clone());
        let continuation_context = match decoded_token.as_ref() {
            None => ScalarContinuationContext::initial(),
            Some(token) if token.progress().unconsumed_lookahead().is_some() => {
                return Err(Self::scalar_page_cursor_error().into());
            }
            Some(token) => {
                let logical = token.progress().last_emitted_logical().cloned();
                match token.progress().last_consumed_physical() {
                    Some(physical) => ScalarContinuationContext::resumed_with_primary_progress(
                        logical,
                        Self::physical_primary_key_boundary(physical, &catalog)?,
                    ),
                    None => logical.map_or_else(
                        ScalarContinuationContext::initial,
                        ScalarContinuationContext::resumed,
                    ),
                }
            }
        };
        if decoded_token.is_some() && !continuation_context.has_progress() {
            return Err(Self::scalar_page_cursor_error().into());
        }

        let value_catalog = prepared_plan
            .authority_ref()
            .accepted_schema_info()
            .map(crate::db::schema::SchemaInfo::value_catalog_handle)
            .cloned()
            .ok_or_else(QueryError::invariant)?;
        let (columns, _fixed_scales) = projection.into_components();
        let projection_request = StructuralProjectionRequest::new(
            self.debug,
            prepared_plan,
            CoveringProjectionMetricsRecorder::none(),
            ProjectionMaterializationMetricsRecorder::none(),
            execution_lane,
        )
        .with_distinct_output_offset(usize::try_from(prior_rows_emitted).unwrap_or(usize::MAX))
        .with_page_work_envelope(envelope);
        let projection_request = if exact_initial_exhaustion {
            projection_request
        } else {
            projection_request
                .with_continuation(continuation_context)
                .with_cursor_emission(page_output_limit)
        };
        let page = execute_structural_projection_page(&self.db, projection_request)
            .map_err(QueryError::execute)?;
        let row_count = page.rows.row_count();
        let rows = page
            .rows
            .into_value_rows()
            .into_iter()
            .map(|row| {
                row.iter()
                    .map(|value| {
                        crate::db::schema::output_value_from_runtime(
                            value_catalog.enum_catalog(),
                            value,
                        )
                        .map_err(|_| QueryError::invariant())
                    })
                    .collect::<Result<Vec<_>, _>>()
            })
            .collect::<Result<Vec<_>, _>>()?;
        let rows_emitted = prior_rows_emitted.saturating_add(u64::from(row_count));
        let total_limit_reached = request
            .row_limit()
            .is_some_and(|limit| rows_emitted >= u64::from(limit));
        let continuation = if page.has_more && !total_limit_reached {
            if page.last_emitted_logical.is_none() && page.last_consumed_physical.is_none() {
                return Err(Self::scalar_page_cursor_error().into());
            }
            let cursor_contract = if let Some(contract) = cursor_contract {
                contract
            } else {
                let prepared_plan = deferred_cursor_plan
                    .as_ref()
                    .ok_or_else(Self::scalar_page_cursor_error)?;
                Self::scalar_cursor_contract(
                    request,
                    &catalog,
                    envelope,
                    prepared_plan,
                    mode,
                    exhaustive_proof.as_ref(),
                )?
            };
            let token = ScalarPageToken::new(
                mode,
                cursor_contract.signature,
                cursor_contract.authority,
                cursor_contract.window,
                cursor_contract.order_terms,
                ScalarPageTokenProgress::new(
                    page.last_emitted_logical,
                    page.last_consumed_physical,
                    None,
                    decoded_token
                        .as_ref()
                        .map_or(0, |token| token.progress().matching_rows_skipped()),
                    rows_emitted,
                ),
            );
            Some(encode_cursor(
                token
                    .encode(&cursor_authentication_key().map_err(QueryError::execute)?)
                    .map_err(|error| {
                        QueryError::from_cursor_plan_error(CursorPlanError::from_token_wire_error(
                            error,
                        ))
                    })?
                    .as_slice(),
            ))
        } else {
            None
        };

        if let Some(proof) = exhaustive_proof.as_ref() {
            self.verify_read_set_revision_proof(proof)?;
        }

        Ok((
            LiveQueryPageOutput {
                entity: catalog.snapshot().entity_name().to_string(),
                columns,
                rows,
                row_count,
                continuation,
                work: ScalarPageWork {
                    envelope_identity: envelope.identity(),
                    entries_visited: page.scanned_keys as u64,
                    result_rows: row_count,
                },
            },
            exhaustive_proof,
        ))
    }

    /// Execute one revision-tolerant bounded scalar page.
    pub fn execute_public_live_page(
        &self,
        request: &DynamicQuery,
        continuation: Option<&str>,
    ) -> Result<LiveQueryPageOutput, QueryError> {
        let catalog = self
            .accepted_schema_catalog_context_for_entity_name(Some(request.entity()))
            .map_err(QueryError::execute)?;
        self.execute_scalar_page_against_catalog(
            request,
            continuation,
            DynamicReadLane::Public,
            catalog,
            ScalarPageMode::Live,
            None,
        )
        .map(|(page, _)| page)
        .map_err(Self::live_page_error)
    }

    /// Execute one live page through a typed binding's immutable accepted
    /// entity identity. `None` means the opaque binding is stale.
    #[doc(hidden)]
    pub fn execute_public_live_page_for_typed_binding(
        &self,
        binding: &DynamicTypedEntityBinding,
        request: &DynamicQuery,
        continuation: Option<&str>,
    ) -> Result<Option<LiveQueryPageOutput>, QueryError> {
        let Some(catalog) = self
            .current_typed_entity_binding_catalog(binding)
            .map_err(QueryError::execute)?
        else {
            return Ok(None);
        };
        self.execute_scalar_page_against_catalog(
            request,
            continuation,
            DynamicReadLane::Public,
            catalog,
            ScalarPageMode::Live,
            None,
        )
        .map(|(page, _)| Some(page))
        .map_err(Self::live_page_error)
    }

    /// Execute one ordinary entity-name-driven bounded grouped read.
    pub fn execute_public_dynamic_grouped_query(
        &self,
        request: &DynamicQuery,
    ) -> Result<GroupedQueryOutput, QueryError> {
        let catalog = self
            .accepted_schema_catalog_context_for_entity_name(Some(request.entity()))
            .map_err(QueryError::execute)?;
        self.execute_dynamic_grouped_query_against_catalog(
            request,
            DynamicReadLane::Public,
            catalog,
        )
    }

    /// Execute one grouped typed read through the binding's immutable accepted
    /// entity identity. `None` means the opaque binding is stale.
    #[doc(hidden)]
    pub fn execute_public_dynamic_grouped_query_for_typed_binding(
        &self,
        binding: &DynamicTypedEntityBinding,
        request: &DynamicQuery,
    ) -> Result<Option<GroupedQueryOutput>, QueryError> {
        let Some(catalog) = self
            .current_typed_entity_binding_catalog(binding)
            .map_err(QueryError::execute)?
        else {
            return Ok(None);
        };
        self.execute_dynamic_grouped_query_against_catalog(
            request,
            DynamicReadLane::Public,
            catalog,
        )
        .map(Some)
    }

    /// Execute one trusted entity-name-driven grouped read.
    ///
    /// This bypasses ordinary public admission but retains accepted-schema
    /// planning, explicit grouped limits, cursor validation, and execution.
    pub fn execute_trusted_dynamic_grouped_query(
        &self,
        request: &DynamicQuery,
    ) -> Result<GroupedQueryOutput, QueryError> {
        let catalog = self
            .accepted_schema_catalog_context_for_entity_name(Some(request.entity()))
            .map_err(QueryError::execute)?;
        self.execute_dynamic_grouped_query_against_catalog(
            request,
            DynamicReadLane::Trusted,
            catalog,
        )
    }

    /// Execute one trusted revision-tolerant bounded dynamic page.
    ///
    /// Trusted execution bypasses public admission but retains the same
    /// physical and aggregate request budgets as every other read lane.
    pub fn execute_trusted_live_page(
        &self,
        request: &DynamicQuery,
        continuation: Option<&str>,
    ) -> Result<LiveQueryPageOutput, QueryError> {
        let catalog = self
            .accepted_schema_catalog_context_for_entity_name(Some(request.entity()))
            .map_err(QueryError::execute)?;
        self.execute_scalar_page_against_catalog(
            request,
            continuation,
            DynamicReadLane::Trusted,
            catalog,
            ScalarPageMode::Live,
            None,
        )
        .map(|(page, _)| page)
        .map_err(Self::live_page_error)
    }

    #[cfg(test)]
    pub(in crate::db) fn execute_trusted_live_page_with_result_bytes_limit_for_tests(
        &self,
        request: &DynamicQuery,
        continuation: Option<&str>,
        result_bytes_limit: u64,
    ) -> Result<LiveQueryPageOutput, QueryError> {
        let previous = SCALAR_PAGE_RESULT_BYTES_LIMIT_OVERRIDE
            .with(|limit| limit.replace(Some(result_bytes_limit)));
        let _guard = ScalarPageResultBytesLimitGuard(previous);
        self.execute_trusted_live_page(request, continuation)
    }

    /// Execute one revision-strict bounded dynamic page.
    pub fn execute_public_exhaustive_page(
        &self,
        request: &DynamicQuery,
        continuation: Option<&str>,
        proof: Option<&ReadSetRevisionProof>,
    ) -> Result<ExhaustiveQueryPageOutput, ExhaustiveReadError> {
        let catalog =
            self.accepted_schema_catalog_context_for_entity_name(Some(request.entity()))?;
        let (page, proof) = self.execute_scalar_page_against_catalog(
            request,
            continuation,
            DynamicReadLane::Public,
            catalog,
            ScalarPageMode::Exhaustive,
            proof,
        )?;
        let proof = proof.ok_or(ReadSetRevisionError::NonCanonical)?;
        Ok(ExhaustiveQueryPageOutput::from_live_page(page, proof))
    }

    /// Execute one exhaustive page through a typed binding's accepted identity.
    #[doc(hidden)]
    pub fn execute_public_exhaustive_page_for_typed_binding(
        &self,
        binding: &DynamicTypedEntityBinding,
        request: &DynamicQuery,
        continuation: Option<&str>,
        proof: Option<&ReadSetRevisionProof>,
    ) -> Result<Option<ExhaustiveQueryPageOutput>, ExhaustiveReadError> {
        let Some(catalog) = self.current_typed_entity_binding_catalog(binding)? else {
            return Ok(None);
        };
        let (page, proof) = self.execute_scalar_page_against_catalog(
            request,
            continuation,
            DynamicReadLane::Public,
            catalog,
            ScalarPageMode::Exhaustive,
            proof,
        )?;
        let proof = proof.ok_or(ReadSetRevisionError::NonCanonical)?;
        Ok(Some(ExhaustiveQueryPageOutput::from_live_page(page, proof)))
    }

    /// Execute one trusted revision-strict bounded dynamic page.
    pub fn execute_trusted_exhaustive_page(
        &self,
        request: &DynamicQuery,
        continuation: Option<&str>,
        proof: Option<&ReadSetRevisionProof>,
    ) -> Result<ExhaustiveQueryPageOutput, ExhaustiveReadError> {
        let catalog =
            self.accepted_schema_catalog_context_for_entity_name(Some(request.entity()))?;
        let (page, proof) = self.execute_scalar_page_against_catalog(
            request,
            continuation,
            DynamicReadLane::Trusted,
            catalog,
            ScalarPageMode::Exhaustive,
            proof,
        )?;
        let proof = proof.ok_or(ReadSetRevisionError::NonCanonical)?;
        Ok(ExhaustiveQueryPageOutput::from_live_page(page, proof))
    }

    fn live_page_error(error: ExhaustiveReadError) -> QueryError {
        match error {
            ExhaustiveReadError::Query(error) => error,
            ExhaustiveReadError::Revision(_) => QueryError::invariant(),
        }
    }
}
