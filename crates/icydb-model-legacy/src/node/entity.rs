//! Module: node::entity
//!
//! Responsibility: entity schema node metadata and relationship validation.
//! Does not own: runtime data storage or query execution.
//! Boundary: validates model declarations before catalog/runtime acceptance.

#[cfg(test)]
mod tests;

use crate::prelude::*;
use std::any::Any;

///
/// Entity
///

#[derive(Clone, Debug, Serialize)]
pub struct Entity {
    def: Def,
    source_key: &'static str,
    store: &'static str,
    schema_version: u32,
    primary_key: PrimaryKey,

    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'static str>,

    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    indexes: &'static [Index],

    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    relations: &'static [RelationEdge],

    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    constraints: &'static [CheckConstraint],

    fields: FieldList,
    ty: Type,
}

impl Entity {
    #[must_use]
    #[expect(
        clippy::too_many_arguments,
        reason = "schema entity construction keeps store, key, index, relation, field, and type metadata explicit"
    )]
    pub const fn new(
        def: Def,
        source_key: &'static str,
        store: &'static str,
        schema_version: u32,
        primary_key: PrimaryKey,
        name: Option<&'static str>,
        indexes: &'static [Index],
        relations: &'static [RelationEdge],
        constraints: &'static [CheckConstraint],
        fields: FieldList,
        ty: Type,
    ) -> Self {
        Self {
            def,
            source_key,
            store,
            schema_version,
            primary_key,
            name,
            indexes,
            relations,
            constraints,
            fields,
            ty,
        }
    }

    /// Borrow the immutable entity source key.
    #[must_use]
    pub const fn source_key(&self) -> &'static str {
        self.source_key
    }

    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }

    #[must_use]
    pub const fn store(&self) -> &'static str {
        self.store
    }

    #[must_use]
    pub const fn schema_version(&self) -> u32 {
        self.schema_version
    }

    #[must_use]
    pub const fn primary_key(&self) -> &PrimaryKey {
        &self.primary_key
    }

    #[must_use]
    pub const fn name(&self) -> Option<&'static str> {
        self.name
    }

    #[must_use]
    pub const fn indexes(&self) -> &'static [Index] {
        self.indexes
    }

    #[must_use]
    pub const fn relations(&self) -> &'static [RelationEdge] {
        self.relations
    }

    /// Borrow accepted-check declarations owned by this entity.
    #[must_use]
    pub const fn constraints(&self) -> &'static [CheckConstraint] {
        self.constraints
    }

    #[must_use]
    pub const fn fields(&self) -> &FieldList {
        &self.fields
    }

    #[must_use]
    pub const fn ty(&self) -> &Type {
        &self.ty
    }

    /// Return the scalar primary key field if this entity uses a scalar
    /// primary-key contract.
    #[must_use]
    pub fn scalar_primary_key_field(&self) -> Option<&Field> {
        self.fields().get(self.primary_key().scalar_field()?)
    }

    /// Resolve the entity name used for schema identity.
    #[must_use]
    pub fn resolved_name(&self) -> &'static str {
        self.name().unwrap_or_else(|| self.def().ident())
    }

    fn validate_relation_storage_policy(&self, errs: &mut ErrorTree) {
        for field in self.fields().fields() {
            if let Some(target) = field.value().item().relation() {
                self.validate_relation_target_storage_policy(errs, field.ident(), target);
            }
        }

        for relation in self.relations() {
            self.validate_relation_target_storage_policy(errs, relation.ident(), relation.target());
        }
    }

    fn validate_relation_target_storage_policy(
        &self,
        errs: &mut ErrorTree,
        relation_name: &str,
        target_path: &str,
    ) {
        let Some((source_capabilities, target_capabilities, target_store_path)) = ({
            let schema = schema_read();
            let Ok(source_store) = schema.cast_node::<Store>(self.store()) else {
                return;
            };
            let Ok(target) = schema.cast_node::<Self>(target_path) else {
                return;
            };
            let Ok(target_store) = schema.cast_node::<Store>(target.store()) else {
                return;
            };
            let source_capabilities = source_store.storage_capabilities();
            let target_capabilities = target_store.storage_capabilities();
            let target_store_path = target.store().to_string();
            drop(schema);

            Some((source_capabilities, target_capabilities, target_store_path))
        }) else {
            return;
        };

        if matches!(
            source_capabilities.relation_source(),
            RelationSourceCapability::DurableSource
        ) && matches!(
            target_capabilities.relation_target(),
            RelationTargetCapability::VolatileTarget
        ) {
            err!(
                errs,
                "relation '{}' from durable store '{}' to volatile target store '{}' is not supported; durable stores cannot own referential integrity against volatile heap targets",
                relation_name,
                self.store(),
                target_store_path,
            );
        }
    }
}

impl MacroNode for Entity {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl ValidateNode for Entity {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();

        validate_source_key(
            &mut errs,
            "entity",
            self.source_key(),
            icydb_schema::EntitySourceKey::try_new,
        );
        if self.schema_version() == 0 {
            err!(errs, "entity schema_version must be a positive integer");
        }

        {
            let schema = schema_read();

            // store
            match schema.cast_node::<Store>(self.store()) {
                Ok(_) => {}
                Err(e) => errs.add(e),
            }
        }

        for index in self.indexes() {
            validate_source_key(
                &mut errs,
                "index",
                index.source_key(),
                icydb_schema::IndexSourceKey::try_new,
            );
        }
        for relation in self.relations() {
            validate_source_key(
                &mut errs,
                "relation",
                relation.source_key(),
                icydb_schema::RelationSourceKey::try_new,
            );
            if let Err(e) = relation.validate_for_source(self) {
                errs.merge_for(relation.ident(), e);
            }
        }
        validate_entity_local_source_keys(self, &mut errs);
        self.validate_relation_storage_policy(&mut errs);

        errs.result()
    }
}

impl VisitableNode for Entity {
    fn route_key(&self) -> String {
        self.def().path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def().accept(v);
        self.fields().accept(v);
        for constraint in self.constraints() {
            constraint.accept(v);
        }
        self.ty().accept(v);
    }
}

fn validate_entity_local_source_keys(entity: &Entity, errs: &mut ErrorTree) {
    validate_unique_local_keys(
        errs,
        "field",
        entity.fields().fields().iter().map(Field::source_key),
    );
    validate_unique_local_keys(
        errs,
        "index",
        entity.indexes().iter().map(Index::source_key),
    );
    validate_unique_local_keys(
        errs,
        "relation",
        entity.relations().iter().map(RelationEdge::source_key),
    );
    validate_unique_local_keys(
        errs,
        "constraint",
        entity.constraints().iter().map(CheckConstraint::source_key),
    );
}

fn validate_unique_local_keys<'a>(
    errs: &mut ErrorTree,
    kind: &str,
    source_keys: impl IntoIterator<Item = &'a str>,
) {
    let mut seen = std::collections::BTreeSet::new();
    for source_key in source_keys {
        if !seen.insert(source_key) {
            err!(
                errs,
                "duplicate {kind} source key '{source_key}' within entity",
            );
        }
    }
}
