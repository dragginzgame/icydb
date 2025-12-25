use crate::{
    prelude::*,
    view::{FieldFilter, traits::ViewExpr},
};

/// Shared generator for any node with a field list (Entity, Record).
pub fn generate_field_list_filter(
    filter_ident: &Ident,
    fields: &FieldList,
    derives: &TraitSet,
) -> TokenStream {
    let field_defs = fields.iter().map(|f| FieldFilter(f).expr());
    let idents = fields.iter().map(|f| &f.ident);
    let names = fields.iter().map(|f| f.ident.to_string());

    // paths
    quote! {
        #derives
        pub struct #filter_ident {
            #(#field_defs),*
        }

        impl ::icydb::core::db::primitives::filter::IntoFilterExpr for #filter_ident {
            fn into_expr(self) -> ::icydb::core::db::primitives::filter::FilterExpr {
                let mut exprs = Vec::new();

                #(
                    if let Some(f) = self.#idents {
                        exprs.push(
                            ::icydb::core::db::primitives::filter::IntoScopedFilterExpr::into_scoped(
                                f,
                                #names
                            )
                        );
                    }
                )*

                if exprs.is_empty() {
                    ::icydb::core::db::primitives::filter::FilterExpr::True
                } else {
                    ::icydb::core::db::primitives::filter::FilterDsl::all(exprs)
                }
            }
        }
    }
}
