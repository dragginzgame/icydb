use crate::{
    node::Entity,
    prelude::*,
    view::{
        FieldUpdate, FieldView,
        helper::generate_field_list_filter,
        traits::{View, ViewExpr},
    },
};

///
/// EntityView
///

pub struct EntityView<'a>(pub &'a Entity);

impl View for EntityView<'_> {
    fn generate(&self) -> TokenStream {
        let node = self.0;
        let node_ident = node.def().ident();
        let view_ident = node.view_ident();
        let fields = node.fields.iter().map(|f| FieldView(f).expr());

        // all traits are derived for now
        let derives = self.traits();

        quote! {
            #derives
            pub struct #view_ident {
                #(#fields),*
            }

            impl Default for #view_ident {
                fn default() -> Self {
                    #node_ident::default().to_view()
                }
            }
        }
    }
}

impl ToTokens for EntityView<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.generate());
    }
}

///
/// EntityCreate
///

pub struct EntityCreate<'a>(pub &'a Entity);

impl View for EntityCreate<'_> {
    fn generate(&self) -> TokenStream {
        let node = self.0;
        let node_ident = node.def().ident();
        let create_ident = node.create_ident();
        let fields = node.iter_editable_fields().map(|f| FieldView(f).expr());

        // paths
        let cp = paths().core;
        let defaults = node.iter_editable_fields().map(|f| {
            let ident = &f.ident;

            quote!(#ident: #cp::traits::View::to_view(&entity.#ident))
        });

        let derives = self.traits();

        quote! {
            #derives
            pub struct #create_ident {
                #(#fields),*
            }

            impl Default for #create_ident {
                fn default() -> Self {
                    let entity = #node_ident::default();

                    Self {
                        #(#defaults),*
                    }
                }
            }
        }
    }
}

impl ToTokens for EntityCreate<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.generate());
    }
}

///
/// EntityUpdate
///

pub struct EntityUpdate<'a>(pub &'a Entity);

impl View for EntityUpdate<'_> {
    fn generate(&self) -> TokenStream {
        let node = self.0;
        let update_ident = node.update_ident();
        let fields = node.iter_editable_fields().map(|f| FieldUpdate(f).expr());

        // add in default manually
        let mut derives = self.traits();
        derives.add(TraitKind::Default);

        quote! {
            #derives
            pub struct #update_ident {
                #(#fields),*
            }
        }
    }
}

impl ToTokens for EntityUpdate<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.generate());
    }
}

///
/// EntityFilter
///

pub struct EntityFilter<'a>(pub &'a Entity);

impl View for EntityFilter<'_> {
    fn generate(&self) -> TokenStream {
        let node = self.0;
        let filter_ident = node.filter_ident();
        let mut derives = self.traits();
        derives.add(TraitKind::Default);

        generate_field_list_filter(&filter_ident, &node.fields, &derives)
    }
}

impl ToTokens for EntityFilter<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.generate());
    }
}
