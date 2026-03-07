use crate::prelude::*;

///
/// Record
///

#[derive(Clone, Debug, Serialize)]
pub struct Record {
    def: Def,
    fields: FieldList,
    ty: Type,
}

impl Record {
    /// Creates a record node from its canonical schema parts.
    #[must_use]
    pub const fn new(def: Def, fields: FieldList, ty: Type) -> Self {
        Self { def, fields, ty }
    }

    /// Returns the definition metadata for this record node.
    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }

    /// Returns the record field list.
    #[must_use]
    pub const fn fields(&self) -> &FieldList {
        &self.fields
    }

    /// Returns the canonical runtime type descriptor.
    #[must_use]
    pub const fn ty(&self) -> &Type {
        &self.ty
    }
}

impl MacroNode for Record {
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
}

impl TypeNode for Record {
    fn ty(&self) -> &Type {
        self.ty()
    }
}

impl ValidateNode for Record {}

impl VisitableNode for Record {
    fn route_key(&self) -> String {
        self.def().path()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.def().accept(v);
        self.fields().accept(v);
        self.ty().accept(v);
    }
}
