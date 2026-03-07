use crate::prelude::*;

///
/// Def
///
/// the default gets overridden after the initial darling::from_list() call
/// the schema doesn't care about the generics as they're not useful as static text
///

#[derive(Debug, Default)]
pub struct Def {
    pub(crate) item: Option<ItemStruct>,
    pub(crate) comments: Option<LitStr>,
}

impl Def {
    pub const fn new(item: ItemStruct, comments: Option<LitStr>) -> Self {
        Self {
            item: Some(item),
            comments,
        }
    }

    pub fn ident(&self) -> Ident {
        self.item.as_ref().unwrap().ident.clone()
    }
}

impl HasSchemaPart for Def {
    fn schema_part(&self) -> TokenStream {
        let comments = quote_option(self.comments.as_ref(), as_tokens);
        let ident = quote_one(&self.ident(), to_str_lit);

        // quote
        quote! {
            ::icydb::schema::node::Def::new(module_path!(), #ident, #comments)
        }
    }
}
