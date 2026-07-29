//! Module: node::record
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::{imp::*, prelude::*};

///
/// Record
///

#[derive(Debug, FromMeta)]
pub struct Record {
    #[darling(default, skip)]
    pub(crate) def: Def,

    #[darling(default)]
    pub(crate) name: Option<LitStr>,

    #[darling(default)]
    pub(crate) fields: FieldList,

    #[darling(default)]
    pub(crate) traits: TraitBuilder,

    #[darling(default)]
    pub(crate) ty: Type,
}

impl HasDef for Record {
    fn def(&self) -> &Def {
        &self.def
    }
}

impl ValidateNode for Record {
    fn validate(&self) -> Result<(), DarlingError> {
        self.traits.with_type_traits().validate()?;
        self.fields.validate()?;
        if self.traits.explicitly_adds(TraitKind::Default) {
            validate_struct_default_request("record", self.def(), &self.fields)?;
        }

        Ok(())
    }
}

impl HasSchema for Record {
    fn schema_node_kind() -> SchemaNodeKind {
        SchemaNodeKind::Record
    }
}

impl HasSchemaPart for Record {
    fn schema_part(&self) -> TokenStream {
        let def = self.def.schema_part();
        let name = self.current_name_literal(self.name.as_ref());
        let fields = self.fields.schema_part();
        let ty = self.ty.schema_part();

        // quote
        quote! {
            ::icydb_model::node::Record::new(#def, #name, #fields, #ty)
        }
    }
}

impl HasTraits for Record {
    fn traits(&self) -> Vec<TraitKind> {
        let traits = self.traits.with_type_traits().build();
        traits.into_vec()
    }

    fn map_trait(&self, t: TraitKind) -> Option<TraitStrategy> {
        match t {
            TraitKind::Default => DefaultTrait::strategy(self),
            TraitKind::NormalizeAuto => NormalizeAutoTrait::strategy(self),
            TraitKind::ValidateAuto => ValidateAutoTrait::strategy(self),
            TraitKind::Visitable => VisitableTrait::strategy(self),

            _ => None,
        }
    }
}

impl HasType for Record {
    fn type_part(&self) -> TokenStream {
        let ident = self.def.ident();
        let fields = self.fields.iter().map(|field| {
            let expr = field.type_expr();

            quote! {
                pub #expr
            }
        });

        quote! {
            pub struct #ident {
                #(#fields),*
            }
        }
    }
}

impl ToTokens for Record {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.all_tokens());
    }
}
