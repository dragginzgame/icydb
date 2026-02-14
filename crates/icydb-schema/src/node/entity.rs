use crate::prelude::*;
use std::any::Any;

///
/// Entity
///

#[derive(Clone, Debug, Serialize)]
pub struct Entity {
    pub def: Def,
    pub store: &'static str,
    pub primary_key: PrimaryKey,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub name: Option<&'static str>,

    #[serde(default, skip_serializing_if = "<[_]>::is_empty")]
    pub indexes: &'static [Index],

    pub fields: FieldList,
    pub ty: Type,
}

impl Entity {
    #[must_use]
    /// Return the primary key field if it exists on the entity.
    pub fn get_pk_field(&self) -> Option<&Field> {
        self.fields.get(self.primary_key.field)
    }

    #[must_use]
    /// Resolve the entity name used for schema identity.
    pub fn resolved_name(&self) -> &'static str {
        self.name.unwrap_or(self.def.ident)
    }
}

impl MacroNode for Entity {
    fn as_any(&self) -> &dyn Any {
        self
    }
}

impl TypeNode for Entity {
    fn ty(&self) -> &Type {
        &self.ty
    }
}

impl ValidateNode for Entity {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        let schema = schema_read();

        // store
        match schema.cast_node::<Store>(self.store) {
            Ok(_) => {}
            Err(e) => errs.add(e),
        }

        errs.result()
    }
}

impl VisitableNode for Entity {
    fn route_key(&self) -> String {
        self.def.path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def.accept(v);
        self.fields.accept(v);
        self.ty.accept(v);
    }
}
