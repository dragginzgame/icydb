use crate::prelude::*;

///
/// Index
///

#[derive(Debug, FromMeta)]
pub struct Index {
    pub store: Path,

    #[darling(default, map = "split_idents")]
    pub fields: Vec<Ident>,

    #[darling(default)]
    pub unique: bool,
}

impl HasSchemaPart for Index {
    fn schema_part(&self) -> TokenStream {
        let store = quote_one(&self.store, to_path);
        let fields = quote_slice(&self.fields, to_str_lit);
        let unique = &self.unique;

        // quote
        quote! {
            ::icydb::schema::node::Index {
                store: #store,
                fields: #fields,
                unique: #unique,
            }
        }
    }
}

impl Index {
    pub fn runtime_part(&self) -> TokenStream {
        let store = quote_one(&self.store, to_path);
        let fields = quote_slice(&self.fields, to_str_lit);
        let unique = &self.unique;
        let store_str = self.store.to_token_stream().to_string().replace(' ', "");
        let field_names = self
            .fields
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join(",");

        let name = LitStr::new(&format!("{store_str}({field_names})"), Span::call_site());

        // quote
        quote! {
            ::icydb::model::index::IndexModel::new(#name, #store, #fields, #unique)
        }
    }
}
