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
    /// Wrap a static field list, preserving declaration order.
    pub const fn new(fields: &'static [Field]) -> Self {
        Self { fields }
    }

    #[must_use]
    /// Borrow the fields in declaration order.
    pub const fn fields(&self) -> &'static [Field] {
        self.fields
    }

    // get
    #[must_use]
    /// Find the first field with the given authored name.
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
    /// Generate an omitted field value on insertion using this argument expression.
    Insert(Arg),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
/// Engine-managed timestamp behavior attached to a field.
pub enum FieldWriteManagement {
    /// Set the creation timestamp when inserting a row.
    CreatedAt,
    /// Set the modification timestamp when writing a row.
    UpdatedAt,
}

#[derive(Clone, Debug, Serialize)]
/// Authoring field shape, default, generation and write-management policies.
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
    /// Declare a field with its value shape, default and write policies.
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
    /// Borrow the field cardinality and element declaration.
    pub const fn value(&self) -> &Value {
        &self.value
    }

    #[must_use]
    /// Borrow the authored default expression, if present.
    pub const fn default(&self) -> Option<&Arg> {
        self.default.as_ref()
    }

    #[must_use]
    /// Borrow the insertion-time generation expression, if present.
    pub const fn generated(&self) -> Option<&FieldGeneration> {
        self.generated.as_ref()
    }

    #[must_use]
    /// Return the engine-managed timestamp policy, if declared.
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
