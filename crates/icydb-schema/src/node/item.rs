use crate::prelude::*;
use std::ops::Not;

///
/// Item
///
/// Canonical schema item descriptor for one scalar, relation, or primitive
/// field target plus its attached sanitizers and validators.
///

#[derive(Clone, Debug, Serialize)]
pub struct Item {
    target: ItemTarget,

    #[serde(skip_serializing_if = "Option::is_none")]
    relation: Option<&'static str>,

    #[serde(skip_serializing_if = "Option::is_none")]
    scale: Option<u32>,

    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    validators: &'static [TypeValidator],

    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    sanitizers: &'static [TypeSanitizer],

    #[serde(skip_serializing_if = "Not::not")]
    indirect: bool,
}

impl Item {
    #[must_use]
    pub const fn new(
        target: ItemTarget,
        relation: Option<&'static str>,
        scale: Option<u32>,
        validators: &'static [TypeValidator],
        sanitizers: &'static [TypeSanitizer],
        indirect: bool,
    ) -> Self {
        Self {
            target,
            relation,
            scale,
            validators,
            sanitizers,
            indirect,
        }
    }

    #[must_use]
    pub const fn target(&self) -> &ItemTarget {
        &self.target
    }

    #[must_use]
    pub const fn relation(&self) -> Option<&'static str> {
        self.relation
    }

    #[must_use]
    pub const fn scale(&self) -> Option<u32> {
        self.scale
    }

    #[must_use]
    pub const fn validators(&self) -> &'static [TypeValidator] {
        self.validators
    }

    #[must_use]
    pub const fn sanitizers(&self) -> &'static [TypeSanitizer] {
        self.sanitizers
    }

    #[must_use]
    pub const fn indirect(&self) -> bool {
        self.indirect
    }

    #[must_use]
    pub const fn is_relation(&self) -> bool {
        self.relation().is_some()
    }
}

impl ValidateNode for Item {
    fn validate(&self) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        let schema = schema_read();

        // Phase 1: validate target shape.
        match self.target() {
            ItemTarget::Is(path) => {
                // cannot be an entity
                if schema.check_node_as::<Entity>(path).is_ok() {
                    err!(errs, "a non-relation Item cannot reference an Entity");
                }
            }

            ItemTarget::Primitive(_) => {}
        }

        // Phase 2: validate relation target compatibility.
        if let Some(relation) = self.relation() {
            // Step 1: Ensure the relation path exists and is an Entity
            match schema.cast_node::<Entity>(relation) {
                Ok(entity) => {
                    // Step 2: Get target of the relation entity (usually from its primary key field)
                    if let Some(primary_field) = entity.get_pk_field() {
                        let relation_target = primary_field.value().item().target();

                        // Step 3: Compare declared item target and decimal-scale metadata.
                        let relation_scale = primary_field.value().item().scale();
                        if self.target() != relation_target || self.scale() != relation_scale {
                            err!(
                                errs,
                                "relation target type mismatch: expected ({:?}, scale={:?}), found ({:?}, scale={:?})",
                                relation_target,
                                relation_scale,
                                self.target(),
                                self.scale()
                            );
                        }
                    } else {
                        err!(
                            errs,
                            "relation entity '{relation}' missing primary key field '{0}'",
                            entity.primary_key().field()
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
        for node in self.validators() {
            node.accept(v);
        }
    }
}

///
/// ItemTarget
///
/// Local item target declaration, either by schema path or primitive runtime
/// kind.
///

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
pub enum ItemTarget {
    Is(&'static str),
    Primitive(Primitive),
}
