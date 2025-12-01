use crate::prelude::*;

///
/// Type
///

#[derive(Clone, Debug, Serialize)]
pub struct Type {
    #[serde(default, skip_serializing_if = "<[_]>::is_empty")]
    pub sanitizers: &'static [TypeSanitizer],

    #[serde(default, skip_serializing_if = "<[_]>::is_empty")]
    pub validators: &'static [TypeValidator],
}

impl ValidateNode for Type {}

impl VisitableNode for Type {
    fn drive<V: Visitor>(&self, v: &mut V) {
        for node in self.sanitizers {
            node.accept(v);
        }
        for node in self.validators {
            node.accept(v);
        }
    }
}

///
/// TypeSanitizer
///

#[derive(Clone, Debug, Serialize)]
pub struct TypeSanitizer {
    pub path: &'static str,
    pub args: Args,
}

impl ValidateNode for TypeSanitizer {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();

        // check path
        let res = schema_read().check_node_as::<Sanitizer>(self.path);
        if let Err(e) = res {
            errs.add(e.to_string());
        }

        errs.result()
    }
}

impl VisitableNode for TypeSanitizer {}

///
/// TypeValidator
///

#[derive(Clone, Debug, Serialize)]
pub struct TypeValidator {
    pub path: &'static str,
    pub args: Args,
}

impl ValidateNode for TypeValidator {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();

        // check path
        let res = schema_read().check_node_as::<Validator>(self.path);
        if let Err(e) = res {
            errs.add(e.to_string());
        }

        errs.result()
    }
}

impl VisitableNode for TypeValidator {}
