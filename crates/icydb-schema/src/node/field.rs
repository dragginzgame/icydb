use crate::prelude::*;
use canic_utils::case::{Case, Casing};

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
        let mut errs = ErrorTree::new();

        // idents
        errs.add_result(validate_ident(self.ident));

        // snake case
        if !self.ident.is_case(Case::Snake) {
            err!(errs, "field ident '{}' must be snake_case", self.ident);
        }

        // relation naming
        if self.value.item.is_relation() {
            let ident = self.ident;
            match self.value.cardinality {
                Cardinality::One | Cardinality::Opt if !ident.ends_with("id") => {
                    err!(
                        errs,
                        "one or optional relationship '{ident}' should end with 'id'"
                    );
                }
                Cardinality::Many if !ident.ends_with("ids") => {
                    err!(errs, "many relationship '{ident}' should end with 'ids'");
                }

                _ => {}
            }
        }

        errs.result()
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
