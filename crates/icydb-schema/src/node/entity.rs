use crate::prelude::*;
use std::any::Any;

///
/// Entity
///

#[derive(Clone, Debug, Serialize)]
pub struct Entity {
    def: Def,
    store: &'static str,
    primary_key: PrimaryKey,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    name: Option<&'static str>,

    #[serde(default, skip_serializing_if = "<[_]>::is_empty")]
    indexes: &'static [Index],

    fields: FieldList,
    ty: Type,
}

impl Entity {
    #[must_use]
    pub const fn new(
        def: Def,
        store: &'static str,
        primary_key: PrimaryKey,
        name: Option<&'static str>,
        indexes: &'static [Index],
        fields: FieldList,
        ty: Type,
    ) -> Self {
        Self {
            def,
            store,
            primary_key,
            name,
            indexes,
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
    pub const fn fields(&self) -> &FieldList {
        &self.fields
    }

    #[must_use]
    pub const fn ty(&self) -> &Type {
        &self.ty
    }

    #[must_use]
    /// Return the primary key field if it exists on the entity.
    pub fn get_pk_field(&self) -> Option<&Field> {
        self.fields().get(self.primary_key().field())
    }

    #[must_use]
    /// Resolve the entity name used for schema identity.
    pub fn resolved_name(&self) -> &'static str {
        self.name().unwrap_or_else(|| self.def().ident())
    }
}

impl MacroNode for Entity {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl TypeNode for Entity {
    fn ty(&self) -> &Type {
        self.ty()
    }
}

impl ValidateNode for Entity {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        let schema = schema_read();

        // store
        match schema.cast_node::<Store>(self.store()) {
            Ok(_) => {}
            Err(e) => errs.add(e),
        }

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
