use crate::prelude::*;

///
/// Record
///

#[derive(Clone, Debug, Serialize)]
pub struct Record {
    def: Def,
    source_key: &'static str,
    fields: FieldList,
    ty: Type,
}

impl Record {
    /// Creates a record node from its canonical schema parts.
    #[must_use]
    pub const fn new(def: Def, source_key: &'static str, fields: FieldList, ty: Type) -> Self {
        Self {
            def,
            source_key,
            fields,
            ty,
        }
    }

    /// Returns the definition metadata for this record node.
    #[must_use]
    pub const fn def(&self) -> &Def {
        &self.def
    }

    /// Returns the immutable type source key.
    #[must_use]
    pub const fn source_key(&self) -> &'static str {
        self.source_key
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

impl ValidateNode for Record {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        validate_source_key(
            &mut errs,
            "record type",
            self.source_key(),
            icydb_schema::TypeSourceKey::try_new,
        );
        let mut seen = std::collections::BTreeSet::new();
        for field in self.fields().fields() {
            if !seen.insert(field.source_key()) {
                err!(
                    errs,
                    "duplicate record field source key '{}'",
                    field.source_key(),
                );
            }
        }
        errs.result()
    }
}

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
