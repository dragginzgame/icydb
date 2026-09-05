//! Module: node::map
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::{imp::*, prelude::*};

///
/// Map
///

#[derive(Debug, FromMeta)]
pub struct Map {
    #[darling(default, skip)]
    pub(crate) def: Def,

    #[darling(default)]
    pub(crate) name: Option<LitStr>,

    pub(crate) key: Item,
    pub(crate) value: Value,

    #[darling(default)]
    pub(crate) ty: Type,

    #[darling(default)]
    pub(crate) traits: TraitBuilder,
}

impl HasDef for Map {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for Map {
    fn validate(&self) -> Result<(), DarlingError> {
        self.validate_traits()?;
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

        if matches!(
            self.value.item.target(),
            ItemTarget::Primitive(Primitive::Unit)
        ) {
            return Err(DarlingError::custom("map value cannot be Unit"));
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
        let name = self.current_name_literal(self.name.as_ref());
        let key = self.key.schema_part();
        let value = self.value.schema_part();
        let ty = self.ty.schema_part();

        // quote
        quote! {
            ::icydb_model::node::Map::new(#def, #name, #key, #value, #ty)
        }
    }
}

impl_collection_has_traits!(Map, Map);

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
        let base = self.all_tokens();
        let typed_adapter = crate::node::typed_adapter::map_adapter_tokens(self);
        tokens.extend(quote! {
            #base
            #typed_adapter
        });
    }
}

#[cfg(test)]
mod tests {
    use super::Map;
    use crate::prelude::*;

    fn map_node() -> Map {
        Map {
            def: Def::new(syn::parse_quote!(
                struct TestMap;
            )),
            name: None,
            key: Item {
                primitive: Some(Primitive::Text),
                unbounded: true,
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
    fn map_value_relation_is_admitted() {
        let mut node = map_node();
        node.value.item.relation = Some(syn::parse_quote!(SomeEntity));

        node.validate()
            .expect("map relation values should validate");
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
