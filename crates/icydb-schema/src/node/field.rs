use crate::prelude::*;

///
/// FieldList
///

#[derive(Clone, Debug, Serialize)]
pub struct FieldList {
    fields: &'static [Field],
}

impl FieldList {
    #[must_use]
    pub const fn new(fields: &'static [Field]) -> Self {
        Self { fields }
    }

    #[must_use]
    pub const fn fields(&self) -> &'static [Field] {
        self.fields
    }

    // get
    #[must_use]
    pub fn get(&self, ident: &str) -> Option<&Field> {
        self.fields.iter().find(|field| field.ident() == ident)
    }
}

impl ValidateNode for FieldList {}

impl VisitableNode for FieldList {
    fn drive<V: Visitor>(&self, v: &mut V) {
        for node in self.fields() {
            node.accept(v);
        }
    }
}

///
/// Field
///

#[derive(Clone, Debug, Serialize)]
pub enum FieldGeneration {
    Insert(Arg),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
pub enum FieldWriteManagement {
    CreatedAt,
    UpdatedAt,
}

#[derive(Clone, Debug, Serialize)]
pub struct Field {
    ident: &'static str,
    value: Value,

    #[serde(skip_serializing_if = "Option::is_none")]
    default: Option<Arg>,

    #[serde(skip_serializing_if = "Option::is_none")]
    db_default: Option<Arg>,

    #[serde(skip_serializing_if = "Option::is_none")]
    generated: Option<FieldGeneration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    write_management: Option<FieldWriteManagement>,
}

impl Field {
    #[must_use]
    pub const fn new(
        ident: &'static str,
        value: Value,
        default: Option<Arg>,
        db_default: Option<Arg>,
        generated: Option<FieldGeneration>,
        write_management: Option<FieldWriteManagement>,
    ) -> Self {
        Self {
            ident,
            value,
            default,
            db_default,
            generated,
            write_management,
        }
    }

    #[must_use]
    pub const fn ident(&self) -> &'static str {
        self.ident
    }

    #[must_use]
    pub const fn value(&self) -> &Value {
        &self.value
    }

    #[must_use]
    pub const fn default(&self) -> Option<&Arg> {
        self.default.as_ref()
    }

    #[must_use]
    pub const fn db_default(&self) -> Option<&Arg> {
        self.db_default.as_ref()
    }

    #[must_use]
    pub const fn generated(&self) -> Option<&FieldGeneration> {
        self.generated.as_ref()
    }

    #[must_use]
    pub const fn write_management(&self) -> Option<FieldWriteManagement> {
        self.write_management
    }
}

impl ValidateNode for Field {
    fn validate(&self) -> Result<(), ErrorTree> {
        Ok(())
    }
}

impl VisitableNode for Field {
    fn route_key(&self) -> String {
        self.ident().to_string()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.value().accept(v);
        if let Some(node) = self.default() {
            node.accept(v);
        }
        if let Some(node) = self.db_default() {
            node.accept(v);
        }
        if let Some(FieldGeneration::Insert(node)) = self.generated() {
            node.accept(v);
        }
    }
}
