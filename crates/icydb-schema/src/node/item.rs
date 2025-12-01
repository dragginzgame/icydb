use crate::prelude::*;
use std::ops::Not;

///
/// Item
///

#[derive(Clone, Debug, Serialize)]
pub struct Item {
    pub target: ItemTarget,

    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub relation: Option<&'static str>,

    #[serde(default, skip_serializing_if = "<[_]>::is_empty")]
    pub validators: &'static [TypeValidator],

    #[serde(default, skip_serializing_if = "<[_]>::is_empty")]
    pub sanitizers: &'static [TypeSanitizer],

    #[serde(default, skip_serializing_if = "Not::not")]
    pub indirect: bool,
}

impl Item {
    #[must_use]
    pub const fn is_relation(&self) -> bool {
        self.relation.is_some()
    }
}

impl ValidateNode for Item {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        let schema = schema_read();

        match &self.target {
            ItemTarget::Is(path) => {
                // cannot be an entity
                if schema.check_node_as::<Entity>(path).is_ok() {
                    err!(errs, "a non-relation Item cannot reference an Entity");
                }
            }

            ItemTarget::Primitive(_) => {}
        }

        // relation
        if let Some(relation) = &self.relation {
            if self.indirect {
                err!(errs, "relations cannot be set to indirect");
            }

            // Step 1: Ensure the relation path exists and is an Entity
            match schema.cast_node::<Entity>(relation) {
                Ok(entity) => {
                    // Step 2: Get target of the relation entity (usually from its primary key field)
                    let primary_field = entity.get_pk_field();
                    let relation_target = &primary_field.value.item.target;

                    // Step 3: Compare to self.target()
                    if &self.target != relation_target {
                        err!(
                            errs,
                            "relation target type mismatch: expected {:?}, found {:?}",
                            relation_target,
                            self.target
                        );
                    }
                }
                Err(_) => {
                    err!(errs, "relation entity '{relation}' not found");
                }
            }
        }

        errs.result()
    }
}

impl VisitableNode for Item {
    fn drive<V: Visitor>(&self, v: &mut V) {
        for node in self.validators {
            node.accept(v);
        }
    }
}

///
/// ItemTarget
///

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum ItemTarget {
    Is(&'static str),
    Primitive(Primitive),
}
