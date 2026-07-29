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
    pub fn get(&self, name: &str) -> Option<&Field> {
        self.fields.iter().find(|field| field.name() == name)
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
    name: &'static str,
    value: Value,

    #[serde(skip_serializing_if = "Option::is_none")]
    default: Option<Arg>,

    #[serde(skip_serializing_if = "Option::is_none")]
    generated: Option<FieldGeneration>,

    #[serde(skip_serializing_if = "Option::is_none")]
    write_management: Option<FieldWriteManagement>,
}

impl Field {
    #[must_use]
    pub const fn new(
        name: &'static str,
        value: Value,
        default: Option<Arg>,
        generated: Option<FieldGeneration>,
        write_management: Option<FieldWriteManagement>,
    ) -> Self {
        Self {
            name,
            value,
            default,
            generated,
            write_management,
        }
    }

    /// Borrow the current declared field name.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
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
        let mut errs = ErrorTree::new();
        validate_source_name(
            &mut errs,
            "field",
            self.name(),
            icydb_schema::FieldSourceKey::try_new,
        );
        errs.result()
    }
}

impl VisitableNode for Field {
    fn route_key(&self) -> String {
        self.name().to_string()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.value().accept(v);
        if let Some(node) = self.default() {
            node.accept(v);
        }
        if let Some(FieldGeneration::Insert(node)) = self.generated() {
            node.accept(v);
        }
    }
}
