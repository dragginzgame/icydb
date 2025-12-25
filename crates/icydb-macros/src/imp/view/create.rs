use crate::prelude::*;

///
/// CreateViewTrait
///
/// Generates the conversion logic from a `Create` view into an entity.
/// Unlike `UpdateView`, this assumes all fields are present and constructs
/// a new instance directly (no merging of optional fields).
///

pub struct CreateViewTrait {}

impl Imp<Entity> for CreateViewTrait {
    fn strategy(node: &Entity) -> Option<TraitStrategy> {
        let create_ident = node.create_ident();
        let ident = node.def().ident();

        // Collect field identifiers
        let field_idents: Vec<_> = node
            .iter_editable_fields()
            .map(|f| f.ident.clone())
            .collect();

        // For each field, generate `field: View::from_view(create.field)`
        let init_pairs: Vec<_> = field_idents
            .iter()
            .map(|ident| {
                quote! {
                    #ident: ::icydb::core::traits::View::from_view(create.#ident),
                }
            })
            .collect();

        // Build the trait implementation
        let q = quote! {
            type CreateViewType = #create_ident;
        };

        let create_impl = Implementor::new(node.def(), TraitKind::CreateView)
            .set_tokens(q)
            .to_token_stream();

        // Generate From<Create> impl that performs the construction
        let conversions = quote! {
            impl From<#create_ident> for #ident {
                fn from(create: #create_ident) -> Self {
                    Self {
                        #(#init_pairs)*
                        ..Default::default()
                    }
                }
            }
        };

        // Merge both impls
        let tokens = quote! {
            #create_impl
            #conversions
        };

        Some(TraitStrategy::from_impl(tokens))
    }
}
