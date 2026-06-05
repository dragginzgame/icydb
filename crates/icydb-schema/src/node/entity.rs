use crate::prelude::*;
use std::any::Any;

///
/// Entity
///

#[derive(Clone, Debug, Serialize)]
pub struct Entity {
    def: Def,
    store: &'static str,
    schema_version: u32,
    primary_key: PrimaryKey,

    #[serde(skip_serializing_if = "Option::is_none")]
    name: Option<&'static str>,

    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    indexes: &'static [Index],

    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    relations: &'static [RelationEdge],

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
        store: &'static str,
        schema_version: u32,
        primary_key: PrimaryKey,
        name: Option<&'static str>,
        indexes: &'static [Index],
        relations: &'static [RelationEdge],
        fields: FieldList,
        ty: Type,
    ) -> Self {
        Self {
            def,
            store,
            schema_version,
            primary_key,
            name,
            indexes,
            relations,
            fields,
            ty,
        }
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

        if source_store.is_stable_storage() && target_store.is_heap_storage() {
            err!(
                errs,
                "relation '{}' from stable store '{}' to heap target store '{}' is not supported in 0.169; stable stores cannot own referential integrity against volatile heap targets",
                relation_name,
                self.store(),
                target.store(),
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
        let schema = schema_read();

        if self.schema_version() == 0 {
            err!(errs, "entity schema_version must be a positive integer");
        }

        // store
        match schema.cast_node::<Store>(self.store()) {
            Ok(_) => {}
            Err(e) => errs.add(e),
        }

        for relation in self.relations() {
            if let Err(e) = relation.validate_for_source(self) {
                errs.merge_for(relation.ident(), e);
            }
        }
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
        self.ty().accept(v);
    }
}

#[cfg(test)]
mod tests;
