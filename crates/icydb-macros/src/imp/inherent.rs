use crate::prelude::*;
use canic_utils::case::{Case, Casing};

///
/// InherentTrait
///

pub struct InherentTrait {}

///
/// Entity
///

impl Imp<Entity> for InherentTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        // emit typed field consts
        let field_consts: Vec<TokenStream> = node
            .fields
            .iter()
            .map(|f| {
                let constant = &f.ident.to_string().to_case(Case::Constant);
                let ident = format_ident!("{constant}");
                let name_str = f.ident.to_string();

                // right now just strings, could be your Field<Self, T> later
                quote! {
                    pub const #ident: &str = #name_str;
                }
            })
            .collect();

        let tokens = quote! {
            #(#field_consts)*
        };

        // IMPORTANT: pass Trait::Inherent so Implementor will do `impl Entity { … }`
        let tokens = Implementor::new(node.def(), TraitKind::Inherent)
            .set_tokens(tokens)
            .to_token_stream();

        Some(TraitStrategy::from_impl(tokens))
    }
}
