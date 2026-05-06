use crate::{
    db::{
        PersistedRow,
        commit::prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint,
        data::DataKey,
        executor::mutation::save::shared::accumulate_prepared_row_op_delta,
        executor::mutation::{
            PreparedRowOpDelta, emit_index_delta_metrics, mutation_write_context,
        },
        schema::commit_schema_fingerprint_for_entity,
    },
    error::InternalError,
    metrics::sink::{ExecKind, MetricsEvent, Span, record},
    traits::EntityValue,
    types::Timestamp,
};
use std::collections::HashSet;

use crate::db::executor::mutation::save::{SaveExecutor, SaveMode, SavePreflightInputs, SaveRule};

impl<E: PersistedRow + EntityValue> SaveExecutor<E> {
    /// Save a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: this helper is fail-fast and non-atomic. If one element fails,
    /// earlier elements in the batch remain committed.
    fn save_batch_non_atomic(
        &self,
        mode: SaveMode,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        let iter = entities.into_iter();
        let mut out = Vec::with_capacity(iter.size_hint().0);
        let ctx = mutation_write_context::<E>(&self.db)?;
        let save_rule = SaveRule::from_mode(mode);
        let schema = Self::schema_info();
        let schema_fingerprint = commit_schema_fingerprint_for_entity::<E>();
        let validate_relations = E::MODEL.has_any_strong_relations();
        let write_context = Self::save_write_context(mode, Timestamp::now());
        let preflight = SavePreflightInputs {
            schema,
            schema_fingerprint,
            validate_relations,
            write_context,
            authored_create_slots: None,
        };
        let mut batch_span = None;
        let mut batch_delta = PreparedRowOpDelta {
            index_inserts: 0,
            index_removes: 0,
            reverse_index_inserts: 0,
            reverse_index_removes: 0,
        };

        // Phase 1: apply each row independently while reusing the same resolved
        // mutation context and schema metadata across the whole batch.
        for entity in iter {
            let span = batch_span.get_or_insert_with(|| Span::<E>::new(ExecKind::Save));

            let result = (|| {
                let (saved, marker_row_op) =
                    self.prepare_entity_save_row_op(&ctx, save_rule, preflight, entity)?;
                let prepared_row_op =
                    prepare_row_commit_for_entity_with_structural_readers_and_schema_fingerprint::<
                        E,
                    >(&self.db, &marker_row_op, &ctx, &ctx, schema_fingerprint)?;
                Self::commit_prepared_single_row(
                    &self.db,
                    marker_row_op,
                    prepared_row_op,
                    |delta| accumulate_prepared_row_op_delta(&mut batch_delta, delta),
                    || {},
                )?;

                Ok::<_, InternalError>(saved)
            })();

            match result {
                Ok(saved) => {
                    out.push(saved);
                    span.set_rows(u64::try_from(out.len()).unwrap_or(u64::MAX));
                }
                Err(err) => {
                    span.set_error(&err);
                    if !out.is_empty() {
                        emit_index_delta_metrics::<E>(&batch_delta);
                        Self::record_save_mutation(
                            save_rule.save_mutation_kind(),
                            u64::try_from(out.len()).unwrap_or(u64::MAX),
                        );
                        record(MetricsEvent::NonAtomicPartialCommit {
                            entity_path: E::PATH,
                            committed_rows: u64::try_from(out.len()).unwrap_or(u64::MAX),
                        });
                    }

                    return Err(err);
                }
            }
        }

        if !out.is_empty() {
            emit_index_delta_metrics::<E>(&batch_delta);
            Self::record_save_mutation(
                save_rule.save_mutation_kind(),
                u64::try_from(out.len()).unwrap_or(u64::MAX),
            );
        }

        Ok(out)
    }

    /// Save a single-entity-type batch atomically in a single commit window.
    ///
    /// All entities are prevalidated first; if any entity fails pre-commit validation,
    /// no row in this batch is persisted.
    ///
    /// This is not a multi-entity transaction surface.
    fn save_batch_atomic(
        &self,
        mode: SaveMode,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        let entities: Vec<E> = entities.into_iter().collect();

        self.save_batch_atomic_impl(SaveRule::from_mode(mode), entities)
    }

    /// Insert a single-entity-type batch atomically in one commit window.
    ///
    /// This API is not a multi-entity transaction surface.
    pub(in crate::db) fn insert_many_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_atomic(SaveMode::Insert, entities)
    }

    /// Update a single-entity-type batch atomically in one commit window.
    ///
    /// This API is not a multi-entity transaction surface.
    pub(in crate::db) fn update_many_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_atomic(SaveMode::Update, entities)
    }

    /// Replace a single-entity-type batch atomically in one commit window.
    ///
    /// This API is not a multi-entity transaction surface.
    pub(in crate::db) fn replace_many_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_atomic(SaveMode::Replace, entities)
    }

    /// Insert a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier inserts may commit before an error.
    pub(in crate::db) fn insert_many_non_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_non_atomic(SaveMode::Insert, entities)
    }

    /// Update a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier updates may commit before an error.
    pub(in crate::db) fn update_many_non_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_non_atomic(SaveMode::Update, entities)
    }

    /// Replace a batch with explicitly non-atomic semantics.
    ///
    /// WARNING: fail-fast and non-atomic. Earlier replaces may commit before an error.
    pub(in crate::db) fn replace_many_non_atomic(
        &self,
        entities: impl IntoIterator<Item = E>,
    ) -> Result<Vec<E>, InternalError> {
        self.save_batch_non_atomic(SaveMode::Replace, entities)
    }

    // Keep the atomic batch body out of the iterator-generic wrapper so mode
    // wrappers do not each own one copy of the full staging pipeline.
    #[inline(never)]
    fn save_batch_atomic_impl(
        &self,
        save_rule: SaveRule,
        entities: Vec<E>,
    ) -> Result<Vec<E>, InternalError> {
        // Phase 1: validate + stage all row ops before opening the commit window.
        let mut span = Span::<E>::new(ExecKind::Save);
        let result = (|| {
            let ctx = mutation_write_context::<E>(&self.db)?;
            let mut out = Vec::with_capacity(entities.len());
            let mut marker_row_ops = Vec::with_capacity(entities.len());
            let mut seen_row_keys = HashSet::with_capacity(entities.len());
            let schema = Self::schema_info();
            let schema_fingerprint = commit_schema_fingerprint_for_entity::<E>();
            let validate_relations = E::MODEL.has_any_strong_relations();
            let write_context = Self::save_write_context(
                match save_rule {
                    SaveRule::RequireAbsent => SaveMode::Insert,
                    SaveRule::RequirePresent => SaveMode::Update,
                    SaveRule::AllowAny => SaveMode::Replace,
                },
                Timestamp::now(),
            );
            let preflight = SavePreflightInputs {
                schema,
                schema_fingerprint,
                validate_relations,
                write_context,
                authored_create_slots: None,
            };

            // Validate and stage all row ops before opening the commit window.
            for mut entity in entities {
                self.preflight_entity_with_cached_schema(
                    &mut entity,
                    preflight.schema,
                    preflight.validate_relations,
                    preflight.write_context,
                    preflight.authored_create_slots,
                )?;
                let marker_row_op = self.prepare_typed_entity_row_op(
                    &ctx,
                    save_rule,
                    &entity,
                    preflight.schema_fingerprint,
                )?;
                if !seen_row_keys.insert(marker_row_op.key) {
                    let data_key = DataKey::try_new::<E>(entity.id().key())?;
                    return Err(InternalError::mutation_atomic_save_duplicate_key(
                        E::PATH,
                        data_key,
                    ));
                }
                marker_row_ops.push(marker_row_op);
                out.push(entity);
            }

            if marker_row_ops.is_empty() {
                return Ok(out);
            }

            // Phase 2: enter commit window and apply staged row ops atomically.
            Self::commit_atomic_batch(&self.db, marker_row_ops, &mut span)?;
            Self::record_save_mutation(
                save_rule.save_mutation_kind(),
                u64::try_from(out.len()).unwrap_or(u64::MAX),
            );

            Ok(out)
        })();
        if let Err(err) = &result {
            span.set_error(err);
        }

        result
    }
}
