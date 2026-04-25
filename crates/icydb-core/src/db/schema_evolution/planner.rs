//! Module: db::schema_evolution::planner
//! Responsibility: validate schema-evolution descriptors and derive migration plans.
//! Does not own: migration execution or commit-marker recovery.
//! Boundary: descriptor + model targets -> deterministic `MigrationPlan`.

use crate::{
    db::{
        identity::EntityName,
        migration::{MigrationPlan, MigrationStep},
        schema_evolution::{
            SchemaMigrationDescriptor, SchemaMigrationEntityTarget, SchemaMigrationRowOp,
            SchemaMigrationStepIntent,
        },
    },
    error::InternalError,
};

///
/// SchemaMigrationPlanner
///
/// SchemaMigrationPlanner is the schema-aware derivation boundary above
/// `db::migration`.
/// It validates descriptor identity against known runtime models and emits one
/// deterministic row-op migration plan for the lower execution engine.
///

#[derive(Clone, Debug)]
pub struct SchemaMigrationPlanner {
    entities: Vec<SchemaMigrationEntityTarget>,
}

impl SchemaMigrationPlanner {
    /// Build one planner from canonical schema-evolution entity targets.
    pub fn new(entities: Vec<SchemaMigrationEntityTarget>) -> Result<Self, InternalError> {
        // Phase 1: fail before planning if two runtime models claim the same
        // canonical schema identity. Planning cannot be deterministic otherwise.
        for (index, entity) in entities.iter().enumerate() {
            if entities[..index]
                .iter()
                .any(|existing| existing.name() == entity.name())
            {
                return Err(InternalError::schema_evolution_duplicate_entity(
                    entity.name().as_str(),
                ));
            }
        }

        Ok(Self { entities })
    }

    /// Build one planner directly from runtime entity models.
    pub fn from_models(
        models: &[&'static crate::model::EntityModel],
    ) -> Result<Self, InternalError> {
        let entities = models
            .iter()
            .copied()
            .map(SchemaMigrationEntityTarget::from_model)
            .collect::<Result<Vec<_>, _>>()?;

        Self::new(entities)
    }

    /// Derive one deterministic low-level migration plan from a descriptor.
    pub fn plan(
        &self,
        descriptor: &SchemaMigrationDescriptor,
    ) -> Result<MigrationPlan, InternalError> {
        // Phase 1: validate schema intent against canonical entity/model facts.
        self.validate_intent(descriptor.intent())?;

        // Phase 2: convert explicit schema-evolution row rewrites into migration
        // row operations only after schema compatibility has been proven.
        let row_ops = descriptor
            .data_transformation()
            .ok_or_else(|| {
                InternalError::schema_evolution_row_ops_required(descriptor.migration_id().as_str())
            })?
            .row_ops();
        if row_ops.is_empty() {
            return Err(InternalError::schema_evolution_row_ops_required(
                descriptor.migration_id().as_str(),
            ));
        }

        for row_op in row_ops {
            self.require_entity(row_op.target().name())?;
        }

        let migration_row_ops = descriptor
            .clone()
            .into_data_transformation()
            .expect("descriptor data transformation already checked")
            .into_row_ops()
            .into_iter()
            .map(SchemaMigrationRowOp::into_migration_row_op)
            .collect::<Result<Vec<_>, _>>()?;
        let step = MigrationStep::from_row_ops("schema_evolution_apply", migration_row_ops)?;

        MigrationPlan::new(
            descriptor.migration_id().as_str(),
            descriptor.version(),
            vec![step],
        )
    }

    fn validate_intent(&self, intent: &SchemaMigrationStepIntent) -> Result<(), InternalError> {
        match intent {
            SchemaMigrationStepIntent::AddIndex { index } => {
                let (entity, fields) = parse_index_name_parts(index.as_str())?;
                let target = self.require_entity(entity)?;
                for field in &fields {
                    if target.model().resolve_field_slot(field).is_none() {
                        return Err(InternalError::schema_evolution_unknown_field(
                            target.name().as_str(),
                            field,
                        ));
                    }
                }
                if target
                    .model()
                    .indexes()
                    .iter()
                    .any(|existing| existing.fields() == fields.as_slice())
                {
                    return Err(InternalError::schema_evolution_duplicate_index(
                        target.name().as_str(),
                        index.as_str(),
                    ));
                }
            }
        }

        Ok(())
    }

    fn require_entity(
        &self,
        entity: EntityName,
    ) -> Result<SchemaMigrationEntityTarget, InternalError> {
        self.entities
            .iter()
            .copied()
            .find(|target| target.name() == entity)
            .ok_or_else(|| InternalError::schema_evolution_unknown_entity(entity.as_str()))
    }
}

fn parse_index_name_parts(index: &str) -> Result<(EntityName, Vec<&str>), InternalError> {
    let mut parts = index.split('|');
    let entity = parts
        .next()
        .ok_or_else(|| InternalError::schema_evolution_invalid_index_name(index))?;
    let entity = EntityName::try_from_str(entity).map_err(|err| {
        InternalError::schema_evolution_invalid_identity(format!(
            "invalid index entity segment '{entity}': {err}",
        ))
    })?;
    let fields = parts.collect::<Vec<_>>();
    if fields.is_empty() {
        return Err(InternalError::schema_evolution_invalid_index_name(index));
    }

    Ok((entity, fields))
}
