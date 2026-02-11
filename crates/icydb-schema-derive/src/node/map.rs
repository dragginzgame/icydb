use crate::{imp::*, prelude::*};

///
/// Map
///

#[derive(Debug, FromMeta)]
pub struct Map {
    #[darling(default, skip)]
    pub def: Def,

    pub key: Item,
    pub value: Value,

    #[darling(default)]
    pub ty: Type,

    #[darling(default)]
    pub traits: TraitBuilder,
}

impl HasDef for Map {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for Map {
    fn validate(&self) -> Result<(), DarlingError> {
        self.traits.with_type_traits().validate()?;
        self.key.validate()?;
        self.value.validate()?;

        // Map keys must stay scalar and canonical in runtime representation.
        if self.key.relation.is_some() {
            return Err(DarlingError::custom(
                "map key must be scalar and cannot be a relation",
            ));
        }

        if self.key.indirect {
            return Err(DarlingError::custom("map key cannot be indirect"));
        }

        if matches!(self.key.target(), ItemTarget::Primitive(Primitive::Unit)) {
            return Err(DarlingError::custom("map key cannot be Unit"));
        }

        // Map values are intentionally non-nested in 0.7.
        if self.value.cardinality() == Cardinality::Many {
            return Err(DarlingError::custom(
                "map value cardinality cannot be many in icydb 0.7",
            ));
        }

        if matches!(
            self.value.item.target(),
            ItemTarget::Primitive(Primitive::Unit)
        ) {
            return Err(DarlingError::custom("map value cannot be Unit"));
        }

        // Relations inside map values are currently unsupported.
        if self.value.item.relation.is_some() {
            return Err(DarlingError::custom(
                "map value cannot be a relation in icydb 0.7",
            ));
        }

        if self.value.item.indirect {
            return Err(DarlingError::custom("map value cannot be indirect"));
        }

        Ok(())
    }
}

impl HasSchema for Map {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::Map
    }
}

impl HasSchemaPart for Map {
    fn schema_part(&self) -> TokenStream {
        let def = self.def.schema_part();
        let key = self.key.schema_part();
        let value = self.value.schema_part();
        let ty = self.ty.schema_part();

        // quote
        quote! {
            ::icydb::schema::node::Map {
                def: #def,
                key: #key,
                value: #value,
                ty: #ty,
            }
        }
    }
}

impl HasTraits for Map {
    fn traits(&self) -> Vec<TraitKind> {
        let mut traits = self.traits.with_type_traits().build();
        traits.add(TraitKind::FieldValue);
        traits.add(TraitKind::MapCollection);
        traits.add(TraitKind::Inherent);

        traits.into_vec()
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        match t {
            TraitKind::AsView => AsViewTrait::strategy(self),
            TraitKind::FieldValue => FieldValueTrait::strategy(self),
            TraitKind::From => FromTrait::strategy(self),
            TraitKind::Inherent => InherentTrait::strategy(self),
            TraitKind::MapCollection => MapCollectionTrait::strategy(self),
            TraitKind::SanitizeAuto => SanitizeAutoTrait::strategy(self),
            TraitKind::UpdateView => UpdateViewTrait::strategy(self),
            TraitKind::ValidateAuto => ValidateAutoTrait::strategy(self),
            TraitKind::Visitable => VisitableTrait::strategy(self),

            _ => None,
        }
    }
}

impl HasType for Map {
    fn type_part(&self) -> TokenStream {
        let ident = self.def.ident();
        let key = &self.key.type_expr();
        let value = &self.value.type_expr();

        quote! {
            #[repr(transparent)]
            pub struct #ident(pub ::std::collections::BTreeMap<#key, #value>);
        }
    }
}

impl ToTokens for Map {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.all_tokens());
    }
}

#[cfg(test)]
mod tests {
    use super::Map;
    use crate::prelude::*;

    fn map_node() -> Map {
        Map {
            def: Def::default(),
            key: Item {
                primitive: Some(Primitive::Text),
                ..Default::default()
            },
            value: Value {
                item: Item {
                    primitive: Some(Primitive::Nat32),
                    ..Default::default()
                },
                ..Default::default()
            },
            ty: Type::default(),
            traits: TraitBuilder::default(),
        }
    }

    #[test]
    fn map_value_relation_is_rejected() {
        let mut node = map_node();
        node.value.item.relation = Some(syn::parse_quote!(SomeEntity));

        let err = node
            .validate()
            .expect_err("map relation values should fail");
        assert!(
            err.to_string().contains("map value cannot be a relation"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn map_value_indirect_is_rejected() {
        let mut node = map_node();
        node.value.item.indirect = true;

        let err = node
            .validate()
            .expect_err("indirect map values should fail");
        assert!(
            err.to_string().contains("map value cannot be indirect"),
            "unexpected error: {err}"
        );
    }
}
