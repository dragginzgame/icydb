use crate::{
    node::Record,
    prelude::*,
    view::{
        FieldUpdate, FieldView,
        helper::generate_field_list_filter,
        traits::{View, ViewExpr},
    },
};

///
/// RecordView
///

pub struct RecordView<'a>(pub &'a Record);

impl View for RecordView<'_> {
    fn generate(&self) -> TokenStream {
        let node = self.0;
        let node_ident = node.def().ident();
        let view_ident = node.view_ident();
        let fields = node.fields.iter().map(|f| FieldView(f).expr());

        // all traits derived
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

impl ToTokens for RecordView<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.generate());
    }
}

///
/// RecordUpdate
///

pub struct RecordUpdate<'a>(pub &'a Record);

impl View for RecordUpdate<'_> {
    fn generate(&self) -> TokenStream {
        let node = self.0;
        let update_ident = node.update_ident();
        let fields = node.fields.iter().map(|f| FieldUpdate(f).expr());

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

impl ToTokens for RecordUpdate<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.generate());
    }
}

///
/// RecordFilter
///

pub struct RecordFilter<'a>(pub &'a Record);

impl View for RecordFilter<'_> {
    fn generate(&self) -> TokenStream {
        let node = self.0;
        let filter_ident = node.filter_ident();
        let mut derives = self.traits();
        derives.add(TraitKind::Default);

        generate_field_list_filter(&filter_ident, &node.fields, &derives)
    }
}

impl ToTokens for RecordFilter<'_> {
    fn to_tokens(&self, tokens: &mut TokenStream) {
        tokens.extend(self.generate());
    }
}
