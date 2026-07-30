//! Module: node::tuple
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::{imp::*, prelude::*};

///
/// Tuple
///

#[derive(Debug, FromMeta)]
pub struct Tuple {
    #[darling(default, skip)]
    pub(crate) def: Def,

    #[darling(default)]
    pub(crate) name: Option<LitStr>,

    #[darling(multiple, rename = "value")]
    pub(crate) values: Vec<Value>,

    #[darling(default)]
    pub(crate) ty: Type,

    #[darling(default)]
    pub(crate) typed_adapters: bool,

    #[darling(default)]
    pub(crate) traits: TraitBuilder,
}

impl HasDef for Tuple {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for Tuple {
    fn validate(&self) -> Result<(), DarlingError> {
        self.traits.with_type_traits().validate()?;

        for value in &self.values {
            value.validate()?;
        }

        Ok(())
    }
}

impl HasSchema for Tuple {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::Tuple
    }
}

impl HasSchemaPart for Tuple {
    fn schema_part(&self) -> TokenStream {
        let def = self.def.schema_part();
        let name = self.current_name_literal(self.name.as_ref());
        let values = quote_slice(&self.values, Value::schema_part);
        let ty = &self.ty.schema_part();

        // quote
        quote! {
            ::icydb_model::node::Tuple::new(#def, #name, #values, #ty)
        }
    }
}

impl HasTraits for Tuple {
    fn traits(&self) -> Vec<TraitKind> {
        let traits = self.traits.with_type_traits().build();

        traits.into_vec()
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        match t {
            TraitKind::Default => DefaultTrait::strategy(self),
            TraitKind::Visitable => VisitableTrait::strategy(self),

            _ => None,
        }
    }
}

impl HasType for Tuple {
    fn type_part(&self) -> TokenStream {
        let ident = self.def.ident();
        let values = self.values.iter().map(HasTypeExpr::type_expr);

        quote! {
            pub struct #ident(#(pub #values),*);
        }
    }
}

impl ToTokens for Tuple {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        let base = self.all_tokens();
        let typed_adapter = crate::node::typed_adapter::tuple_adapter_tokens(self);
        tokens.extend(quote! {
            #base
            #typed_adapter
        });
    }
}
