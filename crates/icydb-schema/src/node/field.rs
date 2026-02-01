use crate::prelude::*;

///
/// FieldList
///

#[derive(Clone, Debug, Serialize)]
pub struct FieldList {
    pub fields: &'static [Field],
}

impl FieldList {
    // get
    #[must_use]
    pub fn get(&self, ident: &str) -> Option<&Field> {
        self.fields.iter().find(|f| f.ident == ident)
    }
}

impl ValidateNode for FieldList {}

impl VisitableNode for FieldList {
    fn drive<V: Visitor>(&self, v: &mut V) {
        for node in self.fields {
            node.accept(v);
        }
    }
}

///
/// Field
///

#[derive(Clone, Debug, Serialize)]
pub struct Field {
    pub ident: &'static str,
    pub value: Value,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub default: Option<Arg>,
}

impl ValidateNode for Field {
    fn validate(&self) -> Result<(), ErrorTree> {
        Ok(())
    }
}

impl VisitableNode for Field {
    fn route_key(&self) -> String {
        self.ident.to_string()
    }

    fn drive<V: Visitor>(&self, v: &mut V) {
        self.value.accept(v);
        if let Some(node) = &self.default {
            node.accept(v);
        }
    }
}
