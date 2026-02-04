use crate::prelude::*;

///
/// Item
///

#[derive(Clone, Debug, Default, FromMeta)]
pub struct Item {
    #[darling(default)]
    pub is: Option<Path>,

    #[darling(default, rename = "prim")]
    pub primitive: Option<Primitive>,

    #[darling(default, rename = "rel")]
    pub relation: Option<Path>,

    #[darling(default, rename = "ext")]
    pub external: Option<Path>,

    #[darling(multiple, rename = "sanitizer")]
    pub sanitizers: Vec<TypeSanitizer>,

    #[darling(multiple, rename = "validator")]
    pub validators: Vec<TypeValidator>,

    #[darling(default)]
    pub indirect: bool,
}

impl Item {
    pub fn validate(&self) -> Result<(), DarlingError> {
        // Phase 1: reject incompatible option pairs.
        if let (Some(_), Some(_)) = (&self.relation, &self.external) {
            return Err(
                DarlingError::custom("item may not specify both rel and ext")
                    .with_span(self.relation.as_ref().unwrap()),
            );
        }
        if self.is.is_some() && self.primitive.is_some() {
            return Err(DarlingError::custom(
                "item may not specify both is and prim",
            ));
        }

        // Phase 2: enforce relation/external constraints.
        if let Some(relation) = &self.relation
            && self.indirect
        {
            return Err(
                DarlingError::custom("relations cannot be set to indirect").with_span(relation)
            );
        }

        if let Some(external) = &self.external
            && self.indirect
        {
            return Err(
                DarlingError::custom("external relations cannot be set to indirect")
                    .with_span(external),
            );
        }

        Ok(())
    }

    // If rel/ext is set and no is/primitive is specified, default to Ulid.
    pub fn target(&self) -> ItemTarget {
        debug_assert!(
            !(self.is.is_some() && self.primitive.is_some()),
            "item 'is' cannot be combined with 'prim'",
        );
        debug_assert!(
            !(self.relation.is_some() && self.external.is_some()),
            "item 'rel' cannot be combined with 'ext'",
        );

        if let Some(path) = &self.is {
            return ItemTarget::Is(path.clone());
        }
        if let Some(prim) = &self.primitive {
            return ItemTarget::Primitive(*prim);
        }
        if self.relation.is_some() || self.external.is_some() {
            return ItemTarget::Primitive(Primitive::Ulid);
        }

        ItemTarget::Primitive(Primitive::Unit)
    }

    pub fn created_at() -> Self {
        Self {
            primitive: Some(Primitive::Timestamp),
            sanitizers: vec![TypeSanitizer::new(
                "icydb::base::sanitizer::time::CreatedAt",
                Args::none(),
            )],
            ..Default::default()
        }
    }

    pub fn updated_at() -> Self {
        Self {
            primitive: Some(Primitive::Timestamp),
            sanitizers: vec![TypeSanitizer::new(
                "icydb::base::sanitizer::time::UpdatedAt",
                Args::none(),
            )],
            ..Default::default()
        }
    }

    pub const fn is_relation(&self) -> bool {
        self.relation.is_some()
    }

    pub const fn is_external(&self) -> bool {
        self.external.is_some()
    }

    pub const fn is_primitive(&self) -> bool {
        self.primitive.is_some()
    }
}

impl HasSchemaPart for Item {
    fn schema_part(&self) -> TokenStream {
        let target = self.target().schema_part();
        let relation = quote_option(self.relation.as_ref(), to_path);
        let external = quote_option(self.external.as_ref(), to_path);
        let validators = quote_slice(&self.validators, TypeValidator::schema_part);
        let sanitizers = quote_slice(&self.sanitizers, TypeSanitizer::schema_part);
        let indirect = self.indirect;

        quote! {
            ::icydb::schema::node::Item {
                target: #target,
                relation: #relation,
                external: #external,
                validators: #validators,
                sanitizers: #sanitizers,
                indirect: #indirect,
            }
        }
    }
}

impl HasTypeExpr for Item {
    fn type_expr(&self) -> TokenStream {
        let ty = if let Some(relation) = &self.relation {
            quote!(::icydb::types::Ref<#relation>)
        } else if let Some(external) = &self.external {
            let id_ty = self.target().type_expr();
            quote!(::icydb::types::Ext<#external, #id_ty>)
        } else {
            self.target().type_expr()
        };

        if self.indirect {
            quote!(Box<#ty>)
        } else {
            quote!(#ty)
        }
    }
}

///
/// ItemTarget
///

pub enum ItemTarget {
    Is(Path),
    Primitive(Primitive),
}

impl HasSchemaPart for ItemTarget {
    fn schema_part(&self) -> TokenStream {
        match self {
            Self::Is(path) => {
                let path = quote_one(path, to_path);
                quote! {
                    ::icydb::schema::node::ItemTarget::Is(#path)
                }
            }
            Self::Primitive(prim) => {
                quote! {
                    ::icydb::schema::node::ItemTarget::Primitive(#prim)
                }
            }
        }
    }
}

impl HasTypeExpr for ItemTarget {
    fn type_expr(&self) -> TokenStream {
        match self {
            Self::Is(path) => quote!(#path),
            Self::Primitive(prim) => {
                let ty = prim.as_type();
                quote!(#ty)
            }
        }
    }
}
