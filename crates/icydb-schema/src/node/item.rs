use super::relation::RelationComponentContract;
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

    #[serde(skip_serializing_if = "Option::is_none")]
    max_len: Option<u32>,

    #[serde(skip_serializing_if = "Option::is_none")]
    max_bytes: Option<u32>,

    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    validators: &'static [TypeValidator],

    #[serde(skip_serializing_if = "<[_]>::is_empty")]
    sanitizers: &'static [TypeSanitizer],

    #[serde(skip_serializing_if = "Not::not")]
    indirect: bool,
}

impl Item {
    #[must_use]
    #[expect(
        clippy::too_many_arguments,
        reason = "schema item construction keeps generated scalar, relation, and validation metadata explicit"
    )]
    pub const fn new(
        target: ItemTarget,
        relation: Option<&'static str>,
        scale: Option<u32>,
        max_len: Option<u32>,
        max_bytes: Option<u32>,
        validators: &'static [TypeValidator],
        sanitizers: &'static [TypeSanitizer],
        indirect: bool,
    ) -> Self {
        Self {
            target,
            relation,
            scale,
            max_len,
            max_bytes,
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
    pub const fn max_len(&self) -> Option<u32> {
        self.max_len
    }

    #[must_use]
    pub const fn max_bytes(&self) -> Option<u32> {
        self.max_bytes
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
            match schema.cast_node::<Entity>(relation) {
                Ok(entity) => {
                    if entity.primary_key().fields().len() != 1 {
                        err!(
                            errs,
                            "relation entity '{relation}' uses composite primary key fields {:?}; single-field relation targets require a scalar primary key; use ordered relation tuple metadata for composite targets",
                            entity.primary_key().fields()
                        );
                    } else if let Some(primary_field) = entity.scalar_primary_key_field() {
                        let expected = RelationComponentContract::from_field(primary_field);
                        let actual = RelationComponentContract::from_item(self);
                        if expected.mismatches(actual) {
                            err!(
                                errs,
                                "relation target type mismatch: expected ({:?}, scale={:?}, max_len={:?}, max_bytes={:?}), found ({:?}, scale={:?}, max_len={:?}, max_bytes={:?})",
                                expected.target(),
                                expected.scale(),
                                expected.max_len(),
                                expected.max_bytes(),
                                actual.target(),
                                actual.scale(),
                                actual.max_len(),
                                actual.max_bytes(),
                            );
                        }
                    } else {
                        let primary_key_field =
                            entity.primary_key().scalar_field().unwrap_or("<composite>");
                        err!(
                            errs,
                            "relation entity '{relation}' missing primary key field '{0}'",
                            primary_key_field
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

#[cfg(test)]
mod tests;
