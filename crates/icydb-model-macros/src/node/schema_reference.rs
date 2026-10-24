//! Module: node::schema_reference
//! Responsibility: runtime-enabled schema source-reference emission.
//! Does not own: accepted-schema binding, validation, or runtime authority.
//! Boundary: authored entity/record names to generated application constants.

use crate::prelude::*;

pub(crate) fn runtime_schema_reference_tokens(
    def: &Def,
    fields: &FieldList,
    entity: Option<&Ident>,
    paths: &CratePaths,
) -> TokenStream {
    let model = &paths.model;
    let runtime = &paths.runtime;
    let entity = entity.map(|ident| {
        let source = quote_one(ident, to_str_lit);

        quote! {
            impl #model::EntitySource for #ident {
                const ENTITY: &'static str = #source;
            }
        }
    });
    let fields = fields.iter().map(|field| {
        let source = quote_one(&field.name, to_str_lit);
        let constant = format_ident!("{}", field.name.to_string().to_ascii_uppercase());
        let documentation = format!(
            "Schema-authored source reference for field `{}`.",
            field.name
        );

        quote! {
            #[doc = #documentation]
            pub const #constant: #runtime::db::query::FieldRef =
                #runtime::db::query::FieldRef::new(#source);
        }
    });

    let fields = Implementor::inherent(def)
        .set_tokens(quote! {
            #(#fields)*
        })
        .impl_tokens(paths);

    quote! {
        #entity
        #fields
    }
}
