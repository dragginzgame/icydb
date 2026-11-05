use crate::prelude::*;

///
/// RelationComponentContract
///
/// Schema-side type contract for one relation key component.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct RelationComponentContract<'a> {
    target: &'a ItemTarget,
    scale: Option<u32>,
    max_len: Option<u32>,
}

impl<'a> RelationComponentContract<'a> {
    pub(crate) const fn from_field(field: &'a Field) -> Self {
        Self::from_item(field.value().item())
    }

    pub(crate) const fn from_item(item: &'a Item) -> Self {
        Self {
            target: item.target(),
            scale: item.scale(),
            max_len: item.max_len(),
        }
    }

    pub(crate) const fn target(&self) -> &'a ItemTarget {
        self.target
    }

    pub(crate) const fn scale(&self) -> Option<u32> {
        self.scale
    }

    pub(crate) const fn max_len(&self) -> Option<u32> {
        self.max_len
    }

    pub(crate) fn mismatches(self, other: Self) -> bool {
        self != other
    }
}

///
/// RelationEdge
///
/// Schema-side relation edge declaration over one or more local component
/// fields. Runtime acceptance still owns durable field IDs and slots; this
/// helper proves arity/order/kind compatibility before a tuple relation shape
/// can be admitted.
///

#[derive(Clone, Debug, Serialize)]
pub struct RelationEdge {
    name: &'static str,
    target: &'static str,
    local_fields: &'static [&'static str],
}

impl RelationEdge {
    /// Build one relation-edge declaration from a relation name, target entity
    /// path, and ordered local component fields.
    #[must_use]
    pub const fn new(
        name: &'static str,
        target: &'static str,
        local_fields: &'static [&'static str],
    ) -> Self {
        Self {
            name,
            target,
            local_fields,
        }
    }

    /// Borrow the relation-edge name used by diagnostics.
    #[must_use]
    pub const fn name(&self) -> &'static str {
        self.name
    }

    /// Borrow the target entity path.
    #[must_use]
    pub const fn target(&self) -> &'static str {
        self.target
    }

    /// Borrow ordered local source fields that map to the target primary key.
    #[must_use]
    pub const fn local_fields(&self) -> &'static [&'static str] {
        self.local_fields
    }

    /// Validate this edge against one source entity and the target entity
    /// stored in the current schema graph.
    pub fn validate_for_source(&self, source: &Entity) -> Result<(), ErrorTree> {
        let schema = schema_read();

        match schema.cast_node::<Entity>(self.target()) {
            Ok(target) => self.validate_against_entities(source, target),
            Err(_) => Err(ErrorTree::from(format!(
                "relation edge '{}' target entity '{}' not found",
                self.name(),
                self.target()
            ))),
        }
    }

    /// Validate this edge against explicit source and target entity metadata.
    pub fn validate_against_entities(
        &self,
        source: &Entity,
        target: &Entity,
    ) -> Result<(), ErrorTree> {
        let mut errs = ErrorTree::new();
        let target_fields = target.primary_key().fields();

        if self.local_fields().is_empty() {
            err!(
                errs,
                "relation edge '{}' must declare at least one local field",
                self.name()
            );
        }

        if self.local_fields().len() != target_fields.len() {
            err!(
                errs,
                "relation edge '{}' arity mismatch: local fields {:?} target primary key fields {:?}",
                self.name(),
                self.local_fields(),
                target_fields,
            );
            return errs.result();
        }

        let mut local_component_cardinality = None;
        for (index, (local_name, target_name)) in self
            .local_fields()
            .iter()
            .zip(target_fields.iter())
            .enumerate()
        {
            let Some(local_field) = source.fields().get(local_name) else {
                err!(
                    errs,
                    "relation edge '{}' local field '{}' not found",
                    self.name(),
                    local_name
                );
                continue;
            };
            let Some(target_field) = target.fields().get(target_name) else {
                err!(
                    errs,
                    "relation edge '{}' target primary key field '{}' not found",
                    self.name(),
                    target_name
                );
                continue;
            };

            if !self.validate_local_component_shape(
                &mut errs,
                local_name,
                local_field,
                &mut local_component_cardinality,
            ) {
                continue;
            }

            self.validate_component_contract(
                &mut errs,
                index,
                local_name,
                local_field,
                target_name,
                target_field,
            );
        }

        errs.result()
    }

    fn validate_local_component_shape(
        &self,
        errs: &mut ErrorTree,
        local_name: &str,
        local_field: &Field,
        local_component_cardinality: &mut Option<Cardinality>,
    ) -> bool {
        let local_cardinality = local_field.value().cardinality();
        if local_cardinality == Cardinality::Many {
            err!(
                errs,
                "relation edge '{}' local field '{}' cannot have many cardinality",
                self.name(),
                local_name
            );
            return false;
        }
        match *local_component_cardinality {
            Some(expected) if expected != local_cardinality => {
                err!(
                    errs,
                    "relation edge '{}' local field '{}' cardinality mismatch: all local component fields must be required or all optional",
                    self.name(),
                    local_name
                );
                return false;
            }
            Some(_) => {}
            None => *local_component_cardinality = Some(local_cardinality),
        }

        if local_field.generated().is_some() {
            err!(
                errs,
                "relation edge '{}' local field '{}' is generated and cannot be a relation component",
                self.name(),
                local_name
            );
            return false;
        }

        true
    }

    fn validate_component_contract(
        &self,
        errs: &mut ErrorTree,
        index: usize,
        local_name: &str,
        local_field: &Field,
        target_name: &str,
        target_field: &Field,
    ) {
        let expected = RelationComponentContract::from_field(target_field);
        if !target_primary_key_component_is_admissible(expected) {
            err!(
                errs,
                "relation edge '{}' target primary key field '{}' uses non-admissible component {:?}",
                self.name(),
                target_name,
                expected.target(),
            );
            return;
        }

        let actual = RelationComponentContract::from_field(local_field);
        if expected.mismatches(actual) {
            err!(
                errs,
                "relation edge '{}' component {index} type mismatch: local field '{}' has ({:?}, scale={:?}, max_len={:?}); target field '{}' requires ({:?}, scale={:?}, max_len={:?})",
                self.name(),
                local_name,
                actual.target(),
                actual.scale(),
                actual.max_len(),
                target_name,
                expected.target(),
                expected.scale(),
                expected.max_len(),
            );
        }
    }
}

const fn target_primary_key_component_is_admissible(
    contract: RelationComponentContract<'_>,
) -> bool {
    match contract.target() {
        ItemTarget::Primitive(primitive) => primitive.is_primary_key_component_encodable(),
        ItemTarget::Is(_) => false,
    }
}

#[cfg(test)]
mod tests;
