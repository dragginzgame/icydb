use crate::{node::Entity, prelude::*};
use icydb_core::{
    db::{
        CoercionId as CoreCoercionId, CompareFieldsPredicate as CoreCompareFieldsPredicate,
        CompareOp as CoreCompareOp, ComparePredicate as CoreComparePredicate, EntityName,
        IndexName, Predicate as CorePredicate, parse_generated_index_predicate_sql,
        validate_generated_index_predicate_fields,
    },
    model::{
        FieldKind as CoreFieldKind, FieldModel as CoreFieldModel,
        FieldStorageDecode as CoreFieldStorageDecode, RelationStrength as CoreRelationStrength,
    },
    types::EntityTag as CoreEntityTag,
    value::Value as CoreValue,
};

///
/// Index
///

#[derive(Debug, FromMeta)]
pub struct Index {
    pub(crate) fields: LitStr,

    #[darling(default)]
    pub(crate) unique: bool,

    #[darling(default)]
    // Raw SQL predicate text is accepted at the derive boundary and lowered
    // into canonical predicate semantics during macro expansion.
    pub(crate) predicate: Option<String>,
}

impl HasSchemaPart for Index {
    fn schema_part(&self) -> TokenStream {
        let fields = self.validated_field_idents();
        let fields = quote_slice(&fields, to_str_lit);
        let key_items = self.schema_key_items_tokens();
        let unique = &self.unique;
        let predicate = self
            .predicate
            .as_ref()
            .map(|value| LitStr::new(value, Span::call_site()));
        let predicate = if let Some(predicate) = predicate {
            quote! { Some(#predicate) }
        } else {
            quote! { None }
        };

        // quote
        quote! {
            ::icydb::schema::node::Index::new_with_key_items_and_predicate(
                #fields,
                #key_items,
                #unique,
                #predicate,
            )
        }
    }
}

impl Index {
    /// Build the canonical index name (`entity|key_item|...`) shared across
    /// validation and codegen.
    pub fn generated_name(&self, entity_name: &str) -> String {
        let entity = EntityName::try_from_str(entity_name)
            .expect("validated entity name should build canonical index name");
        let segments = self.generated_name_segments();
        let segment_refs: Vec<&str> = segments.iter().map(String::as_str).collect();
        let name = IndexName::try_from_parts(&entity, segment_refs.as_slice())
            .expect("validated index key items should build canonical index name");

        name.as_str().to_string()
    }

    pub fn runtime_part(
        &self,
        entity: &Entity,
        entity_name: &str,
        store: &Path,
        ordinal: usize,
    ) -> (Vec<TokenStream>, TokenStream) {
        let fields = self.validated_field_idents();
        let fields = quote_slice(&fields, to_str_lit);
        let key_items = self.runtime_key_items_tokens();
        let unique = self.unique;
        let (predicate_support, predicate) = self
            .predicate_runtime_part(entity, ordinal)
            .expect("validated generated index predicate should lower");
        let name = LitStr::new(&self.generated_name(entity_name), Span::call_site());
        let ordinal = u16::try_from(ordinal).expect("index ordinal should fit u16");
        let store = quote_one(store, to_path);

        (
            predicate_support,
            quote! {
                ::icydb::model::index::IndexModel::generated_with_ordinal_and_key_items_and_predicate(
                    #ordinal,
                    #name,
                    #store,
                    #fields,
                    #key_items,
                    #unique,
                    #predicate,
                )
            },
        )
    }

    pub(crate) fn parsed_key_items(&self) -> Result<Vec<IndexKeyItemSpec>, DarlingError> {
        parse_index_key_items(&self.fields)
    }

    pub(crate) fn validated_key_item_terms(&self) -> Vec<String> {
        self.validated_key_items()
            .into_iter()
            .map(|item| item.canonical_text())
            .collect()
    }

    pub(crate) fn generated_name_segments(&self) -> Vec<String> {
        self.validated_key_item_terms()
    }

    fn validated_key_items(&self) -> Vec<IndexKeyItemSpec> {
        self.parsed_key_items()
            .expect("validated index fields should parse")
    }

    pub(crate) fn validated_field_idents(&self) -> Vec<Ident> {
        self.validated_key_items()
            .iter()
            .map(IndexKeyItemSpec::field_ident)
            .fold(Vec::<Ident>::new(), |mut fields, field| {
                if !fields.contains(field) {
                    fields.push(field.clone());
                }

                fields
            })
    }

    fn has_expression_key_items(&self) -> bool {
        self.validated_key_items()
            .iter()
            .any(|item| matches!(item, IndexKeyItemSpec::Expression(_)))
    }

    fn schema_key_items_tokens(&self) -> TokenStream {
        if !self.has_expression_key_items() {
            return quote! { None };
        }

        let key_items = self
            .validated_key_items()
            .iter()
            .map(IndexKeyItemSpec::schema_part)
            .collect::<Vec<_>>();

        quote! { Some(&[#(#key_items),*]) }
    }

    fn runtime_key_items_tokens(&self) -> TokenStream {
        if !self.has_expression_key_items() {
            return quote! { None };
        }

        let key_items = self
            .validated_key_items()
            .iter()
            .map(IndexKeyItemSpec::runtime_part)
            .collect::<Vec<_>>();

        quote! { Some(&[#(#key_items),*]) }
    }

    pub(crate) fn validated_generated_predicate(
        &self,
        entity: &Entity,
    ) -> Result<Option<CorePredicate>, DarlingError> {
        let Some(predicate_sql) = self.predicate.as_deref() else {
            return Ok(None);
        };

        let predicate =
            parse_generated_index_predicate_sql(predicate_sql).map_err(DarlingError::custom)?;
        let field_models = generated_field_models_for_predicate(entity, &predicate)?;
        validate_generated_index_predicate_fields(field_models.as_slice(), &predicate)
            .map_err(DarlingError::custom)?;

        Ok(Some(predicate))
    }

    fn predicate_runtime_part(
        &self,
        entity: &Entity,
        ordinal: usize,
    ) -> Result<(Vec<TokenStream>, TokenStream), DarlingError> {
        let Some(predicate_sql) = self.predicate.as_deref() else {
            return Ok((Vec::new(), quote! { None }));
        };
        let Some(predicate) = self.validated_generated_predicate(entity)? else {
            return Ok((Vec::new(), quote! { None }));
        };

        // Generate one per-index static predicate so runtime planning borrows a
        // canonical AST without reparsing SQL text.
        let entity_ident = entity.def.ident().to_string().to_ascii_uppercase();
        let predicate_static_ident =
            format_ident!("__{}_INDEX_PREDICATE_{}", entity_ident, ordinal);
        let predicate_resolver_ident = format_ident!(
            "__{}_index_predicate_{}_resolver",
            entity_ident.to_ascii_lowercase(),
            ordinal
        );
        let predicate_sql = LitStr::new(predicate_sql, Span::call_site());
        let predicate_tokens = predicate_runtime_tokens(&predicate)?;

        Ok((
            vec![
                quote! {
                    static #predicate_static_ident:
                        ::std::sync::LazyLock<::icydb::db::Predicate> =
                        ::std::sync::LazyLock::new(|| #predicate_tokens);
                },
                quote! {
                    fn #predicate_resolver_ident() -> &'static ::icydb::db::Predicate {
                        &#predicate_static_ident
                    }
                },
            ],
            quote! {
                Some(::icydb::model::index::IndexPredicateMetadata::generated(
                    #predicate_sql,
                    #predicate_resolver_ident,
                ))
            },
        ))
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum IndexKeyItemSpec {
    Field(Ident),
    Expression(IndexExpressionSpec),
}

impl IndexKeyItemSpec {
    pub(crate) const fn field_ident(&self) -> &Ident {
        match self {
            Self::Field(field) => field,
            Self::Expression(expression) => expression.field_ident(),
        }
    }

    fn canonical_text(&self) -> String {
        match self {
            Self::Field(field) => field.to_string(),
            Self::Expression(expression) => expression.canonical_text(),
        }
    }

    fn schema_part(&self) -> TokenStream {
        match self {
            Self::Field(field) => {
                let field = to_str_lit(field);
                quote! { ::icydb::schema::node::IndexKeyItem::Field(#field) }
            }
            Self::Expression(expression) => {
                let expression = expression.schema_part();
                quote! { ::icydb::schema::node::IndexKeyItem::Expression(#expression) }
            }
        }
    }

    fn runtime_part(&self) -> TokenStream {
        match self {
            Self::Field(field) => {
                let field = to_str_lit(field);
                quote! { ::icydb::model::index::IndexKeyItem::Field(#field) }
            }
            Self::Expression(expression) => {
                let expression = expression.runtime_part();
                quote! { ::icydb::model::index::IndexKeyItem::Expression(#expression) }
            }
        }
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum IndexExpressionSpec {
    Lower(Ident),
    Upper(Ident),
    Trim(Ident),
    LowerTrim(Ident),
    Date(Ident),
    Year(Ident),
    Month(Ident),
    Day(Ident),
}

impl IndexExpressionSpec {
    const fn field_ident(&self) -> &Ident {
        match self {
            Self::Lower(field)
            | Self::Upper(field)
            | Self::Trim(field)
            | Self::LowerTrim(field)
            | Self::Date(field)
            | Self::Year(field)
            | Self::Month(field)
            | Self::Day(field) => field,
        }
    }

    fn canonical_text(&self) -> String {
        match self {
            Self::Lower(field) => format!("LOWER({field})"),
            Self::Upper(field) => format!("UPPER({field})"),
            Self::Trim(field) => format!("TRIM({field})"),
            Self::LowerTrim(field) => format!("LOWER(TRIM({field}))"),
            Self::Date(field) => format!("DATE({field})"),
            Self::Year(field) => format!("YEAR({field})"),
            Self::Month(field) => format!("MONTH({field})"),
            Self::Day(field) => format!("DAY({field})"),
        }
    }

    fn schema_part(&self) -> TokenStream {
        let field = to_str_lit(self.field_ident());

        match self {
            Self::Lower(_) => quote! { ::icydb::schema::node::IndexExpression::Lower(#field) },
            Self::Upper(_) => quote! { ::icydb::schema::node::IndexExpression::Upper(#field) },
            Self::Trim(_) => quote! { ::icydb::schema::node::IndexExpression::Trim(#field) },
            Self::LowerTrim(_) => {
                quote! { ::icydb::schema::node::IndexExpression::LowerTrim(#field) }
            }
            Self::Date(_) => quote! { ::icydb::schema::node::IndexExpression::Date(#field) },
            Self::Year(_) => quote! { ::icydb::schema::node::IndexExpression::Year(#field) },
            Self::Month(_) => quote! { ::icydb::schema::node::IndexExpression::Month(#field) },
            Self::Day(_) => quote! { ::icydb::schema::node::IndexExpression::Day(#field) },
        }
    }

    fn runtime_part(&self) -> TokenStream {
        let field = to_str_lit(self.field_ident());

        match self {
            Self::Lower(_) => quote! { ::icydb::model::index::IndexExpression::Lower(#field) },
            Self::Upper(_) => quote! { ::icydb::model::index::IndexExpression::Upper(#field) },
            Self::Trim(_) => quote! { ::icydb::model::index::IndexExpression::Trim(#field) },
            Self::LowerTrim(_) => {
                quote! { ::icydb::model::index::IndexExpression::LowerTrim(#field) }
            }
            Self::Date(_) => quote! { ::icydb::model::index::IndexExpression::Date(#field) },
            Self::Year(_) => quote! { ::icydb::model::index::IndexExpression::Year(#field) },
            Self::Month(_) => quote! { ::icydb::model::index::IndexExpression::Month(#field) },
            Self::Day(_) => quote! { ::icydb::model::index::IndexExpression::Day(#field) },
        }
    }
}

fn parse_index_key_items(literal: &LitStr) -> Result<Vec<IndexKeyItemSpec>, DarlingError> {
    let raw_items = split_top_level_key_items(literal)?;
    if raw_items.is_empty() {
        return Err(
            DarlingError::custom("index fields must reference at least one key item")
                .with_span(literal),
        );
    }

    raw_items
        .iter()
        .map(|item| parse_index_key_item(item.as_str(), literal))
        .collect()
}

fn generated_field_models_for_predicate(
    entity: &Entity,
    predicate: &CorePredicate,
) -> Result<Vec<CoreFieldModel>, DarlingError> {
    let mut field_models = Vec::new();

    for field_name in referenced_predicate_fields(predicate) {
        let Some(field) = entity
            .fields
            .iter()
            .find(|candidate| candidate.ident == field_name)
        else {
            continue;
        };

        field_models.push(generated_field_model_for_predicate(field)?);
    }

    Ok(field_models)
}

fn generated_field_model_for_predicate(field: &Field) -> Result<CoreFieldModel, DarlingError> {
    Ok(
        CoreFieldModel::generated_with_storage_decode_and_nullability(
            Box::leak(field.ident.to_string().into_boxed_str()),
            generated_field_kind_for_predicate(&field.value)?,
            CoreFieldStorageDecode::ByKind,
            matches!(field.value.cardinality(), Cardinality::Opt),
        ),
    )
}

fn generated_field_kind_for_predicate(value: &Value) -> Result<CoreFieldKind, DarlingError> {
    let base_kind = generated_item_kind_for_predicate(&value.item)?;

    Ok(match value.cardinality() {
        Cardinality::Many => CoreFieldKind::List(leak_core_field_kind(base_kind)),
        Cardinality::One | Cardinality::Opt => base_kind,
    })
}

fn generated_item_kind_for_predicate(item: &Item) -> Result<CoreFieldKind, DarlingError> {
    if item.is.is_some() {
        return Err(DarlingError::custom(
            "filtered index predicates on custom field types are not supported at build time",
        ));
    }

    let base_kind = match item.primitive.unwrap_or(Primitive::Unit) {
        Primitive::Account => CoreFieldKind::Account,
        Primitive::Blob => CoreFieldKind::Blob,
        Primitive::Bool => CoreFieldKind::Bool,
        Primitive::Date => CoreFieldKind::Date,
        Primitive::Decimal => CoreFieldKind::Decimal {
            scale: item.scale.unwrap_or(0),
        },
        Primitive::Duration => CoreFieldKind::Duration,
        Primitive::Float32 => CoreFieldKind::Float32,
        Primitive::Float64 => CoreFieldKind::Float64,
        Primitive::Int => CoreFieldKind::IntBig,
        Primitive::Int8 | Primitive::Int16 | Primitive::Int32 | Primitive::Int64 => {
            CoreFieldKind::Int
        }
        Primitive::Int128 => CoreFieldKind::Int128,
        Primitive::Nat => CoreFieldKind::UintBig,
        Primitive::Nat8 | Primitive::Nat16 | Primitive::Nat32 | Primitive::Nat64 => {
            CoreFieldKind::Uint
        }
        Primitive::Nat128 => CoreFieldKind::Uint128,
        Primitive::Principal => CoreFieldKind::Principal,
        Primitive::Subaccount => CoreFieldKind::Subaccount,
        Primitive::Text => CoreFieldKind::Text,
        Primitive::Timestamp => CoreFieldKind::Timestamp,
        Primitive::Ulid => CoreFieldKind::Ulid,
        Primitive::Unit => CoreFieldKind::Unit,
    };

    let Some(relation_path) = item.relation.as_ref() else {
        return Ok(base_kind);
    };

    Ok(CoreFieldKind::Relation {
        target_path: Box::leak(relation_path.to_token_stream().to_string().into_boxed_str()),
        target_entity_name: "",
        target_entity_tag: CoreEntityTag::new(0),
        target_store_path: "",
        key_kind: leak_core_field_kind(base_kind),
        strength: if item.strong {
            CoreRelationStrength::Strong
        } else {
            CoreRelationStrength::Weak
        },
    })
}

fn leak_core_field_kind(kind: CoreFieldKind) -> &'static CoreFieldKind {
    Box::leak(Box::new(kind))
}

fn referenced_predicate_fields(predicate: &CorePredicate) -> Vec<String> {
    let mut fields = Vec::new();
    push_referenced_predicate_fields(predicate, &mut fields);
    fields
}

fn push_referenced_predicate_fields(predicate: &CorePredicate, fields: &mut Vec<String>) {
    match predicate {
        CorePredicate::True | CorePredicate::False => {}
        CorePredicate::And(children) | CorePredicate::Or(children) => {
            for child in children {
                push_referenced_predicate_fields(child, fields);
            }
        }
        CorePredicate::Not(inner) => push_referenced_predicate_fields(inner, fields),
        CorePredicate::Compare(compare) => push_unique_field(compare.field(), fields),
        CorePredicate::CompareFields(compare) => {
            push_unique_field(compare.left_field(), fields);
            push_unique_field(compare.right_field(), fields);
        }
        CorePredicate::IsNull { field }
        | CorePredicate::IsNotNull { field }
        | CorePredicate::IsMissing { field }
        | CorePredicate::IsEmpty { field }
        | CorePredicate::IsNotEmpty { field }
        | CorePredicate::TextContains { field, .. }
        | CorePredicate::TextContainsCi { field, .. } => push_unique_field(field, fields),
    }
}

fn push_unique_field(field: &str, fields: &mut Vec<String>) {
    if fields.iter().any(|existing| existing == field) {
        return;
    }

    fields.push(field.to_string());
}

fn predicate_runtime_tokens(predicate: &CorePredicate) -> Result<TokenStream, DarlingError> {
    Ok(match predicate {
        CorePredicate::True => quote! { ::icydb::db::Predicate::True },
        CorePredicate::False => quote! { ::icydb::db::Predicate::False },
        CorePredicate::And(children) => {
            let children = children
                .iter()
                .map(predicate_runtime_tokens)
                .collect::<Result<Vec<_>, _>>()?;
            quote! { ::icydb::db::Predicate::And(vec![#(#children),*]) }
        }
        CorePredicate::Or(children) => {
            let children = children
                .iter()
                .map(predicate_runtime_tokens)
                .collect::<Result<Vec<_>, _>>()?;
            quote! { ::icydb::db::Predicate::Or(vec![#(#children),*]) }
        }
        CorePredicate::Not(inner) => {
            let inner = predicate_runtime_tokens(inner)?;
            quote! { ::icydb::db::Predicate::Not(Box::new(#inner)) }
        }
        CorePredicate::Compare(compare) => {
            let compare = compare_predicate_runtime_tokens(compare)?;
            quote! { ::icydb::db::Predicate::Compare(#compare) }
        }
        CorePredicate::CompareFields(compare) => {
            let compare = compare_fields_predicate_runtime_tokens(compare)?;
            quote! { ::icydb::db::Predicate::CompareFields(#compare) }
        }
        CorePredicate::IsNull { field } => {
            quote! { ::icydb::db::Predicate::IsNull { field: #field.to_string() } }
        }
        CorePredicate::IsNotNull { field } => {
            quote! { ::icydb::db::Predicate::IsNotNull { field: #field.to_string() } }
        }
        CorePredicate::IsMissing { field } => {
            quote! { ::icydb::db::Predicate::IsMissing { field: #field.to_string() } }
        }
        CorePredicate::IsEmpty { field } => {
            quote! { ::icydb::db::Predicate::IsEmpty { field: #field.to_string() } }
        }
        CorePredicate::IsNotEmpty { field } => {
            quote! { ::icydb::db::Predicate::IsNotEmpty { field: #field.to_string() } }
        }
        CorePredicate::TextContains { field, value } => {
            let value = predicate_value_runtime_tokens(value)?;
            quote! {
                ::icydb::db::Predicate::TextContains {
                    field: #field.to_string(),
                    value: #value,
                }
            }
        }
        CorePredicate::TextContainsCi { field, value } => {
            let value = predicate_value_runtime_tokens(value)?;
            quote! {
                ::icydb::db::Predicate::TextContainsCi {
                    field: #field.to_string(),
                    value: #value,
                }
            }
        }
    })
}

fn compare_predicate_runtime_tokens(
    compare: &CoreComparePredicate,
) -> Result<TokenStream, DarlingError> {
    if !compare.coercion().params().is_empty() {
        return Err(DarlingError::custom(
            "generated filtered index predicates do not support coercion parameters",
        ));
    }

    let field = compare.field();
    let op = compare_op_runtime_tokens(compare.op());
    let value = predicate_value_runtime_tokens(compare.value())?;
    let coercion = coercion_id_runtime_tokens(compare.coercion().id());

    Ok(quote! {
        ::icydb::db::ComparePredicate::with_coercion(#field, #op, #value, #coercion)
    })
}

fn compare_fields_predicate_runtime_tokens(
    compare: &CoreCompareFieldsPredicate,
) -> Result<TokenStream, DarlingError> {
    if !compare.coercion().params().is_empty() {
        return Err(DarlingError::custom(
            "generated filtered index predicates do not support coercion parameters",
        ));
    }

    let left_field = compare.left_field();
    let op = compare_op_runtime_tokens(compare.op());
    let right_field = compare.right_field();
    let coercion = coercion_id_runtime_tokens(compare.coercion().id());

    Ok(quote! {
        ::icydb::db::CompareFieldsPredicate::with_coercion(
            #left_field,
            #op,
            #right_field,
            #coercion,
        )
    })
}

fn compare_op_runtime_tokens(op: CoreCompareOp) -> TokenStream {
    match op {
        CoreCompareOp::Eq => quote! { ::icydb::db::CompareOp::Eq },
        CoreCompareOp::Ne => quote! { ::icydb::db::CompareOp::Ne },
        CoreCompareOp::Lt => quote! { ::icydb::db::CompareOp::Lt },
        CoreCompareOp::Lte => quote! { ::icydb::db::CompareOp::Lte },
        CoreCompareOp::Gt => quote! { ::icydb::db::CompareOp::Gt },
        CoreCompareOp::Gte => quote! { ::icydb::db::CompareOp::Gte },
        CoreCompareOp::In => quote! { ::icydb::db::CompareOp::In },
        CoreCompareOp::NotIn => quote! { ::icydb::db::CompareOp::NotIn },
        CoreCompareOp::Contains => quote! { ::icydb::db::CompareOp::Contains },
        CoreCompareOp::StartsWith => quote! { ::icydb::db::CompareOp::StartsWith },
        CoreCompareOp::EndsWith => quote! { ::icydb::db::CompareOp::EndsWith },
    }
}

fn coercion_id_runtime_tokens(coercion: CoreCoercionId) -> TokenStream {
    match coercion {
        CoreCoercionId::Strict => quote! { ::icydb::db::CoercionId::Strict },
        CoreCoercionId::NumericWiden => quote! { ::icydb::db::CoercionId::NumericWiden },
        CoreCoercionId::TextCasefold => quote! { ::icydb::db::CoercionId::TextCasefold },
        CoreCoercionId::CollectionElement => {
            quote! { ::icydb::db::CoercionId::CollectionElement }
        }
    }
}

fn predicate_value_runtime_tokens(value: &CoreValue) -> Result<TokenStream, DarlingError> {
    Ok(match value {
        CoreValue::Bool(value) => quote! { ::icydb::__macro::Value::Bool(#value) },
        CoreValue::Decimal(value) => {
            let value = value.to_string();
            quote! {
                ::icydb::__macro::Value::Decimal(
                    <::icydb::types::Decimal as ::std::str::FromStr>::from_str(#value)
                        .expect("generated decimal literal should parse"),
                )
            }
        }
        CoreValue::Int(value) => quote! { ::icydb::__macro::Value::Int(#value) },
        CoreValue::List(values) => {
            let values = values
                .iter()
                .map(predicate_value_runtime_tokens)
                .collect::<Result<Vec<_>, _>>()?;
            quote! { ::icydb::__macro::Value::List(vec![#(#values),*]) }
        }
        CoreValue::Null => quote! { ::icydb::__macro::Value::Null },
        CoreValue::Text(value) => quote! { ::icydb::__macro::Value::Text(#value.to_string()) },
        CoreValue::Uint(value) => quote! { ::icydb::__macro::Value::Uint(#value) },
        unexpected => {
            return Err(DarlingError::custom(format!(
                "generated filtered index predicates do not support literal variant {unexpected:?}",
            )));
        }
    })
}

fn split_top_level_key_items(literal: &LitStr) -> Result<Vec<String>, DarlingError> {
    let raw = literal.value();
    let mut items = Vec::new();
    let mut depth = 0usize;
    let mut segment_start = 0usize;

    for (index, ch) in raw.char_indices() {
        match ch {
            '(' => depth = depth.saturating_add(1),
            ')' => {
                if depth == 0 {
                    return Err(DarlingError::custom(format!(
                        "index fields '{raw}' has one unmatched closing ')'"
                    ))
                    .with_span(literal));
                }
                depth = depth.saturating_sub(1);
            }
            ',' if depth == 0 => {
                items.push(raw[segment_start..index].trim().to_string());
                segment_start = index.saturating_add(1);
            }
            _ => {}
        }
    }

    if depth != 0 {
        return Err(DarlingError::custom(format!(
            "index fields '{raw}' has one unmatched opening '('"
        ))
        .with_span(literal));
    }

    items.push(raw[segment_start..].trim().to_string());
    if items.iter().any(String::is_empty) {
        return Err(DarlingError::custom(format!(
            "index fields '{raw}' contains an empty key item"
        ))
        .with_span(literal));
    }

    Ok(items)
}

fn parse_index_key_item(item: &str, literal: &LitStr) -> Result<IndexKeyItemSpec, DarlingError> {
    if let Some(expression) = parse_index_expression_item(item, literal)? {
        return Ok(IndexKeyItemSpec::Expression(expression));
    }

    let field = syn::parse_str::<Ident>(item).map_err(|_| {
        DarlingError::custom(format!(
            "unsupported index key item '{item}'; expected a field name or one supported expression form"
        ))
        .with_span(literal)
    })?;

    Ok(IndexKeyItemSpec::Field(field))
}

fn parse_index_expression_item(
    item: &str,
    literal: &LitStr,
) -> Result<Option<IndexExpressionSpec>, DarlingError> {
    if !item.contains('(') {
        return Ok(None);
    }

    if let Some(field) = parse_single_argument_function(item, "LOWER")? {
        if let Some(inner_field) = parse_single_argument_function(field, "TRIM")? {
            return Ok(Some(IndexExpressionSpec::LowerTrim(
                parse_index_field_ident(inner_field, literal)?,
            )));
        }

        return Ok(Some(IndexExpressionSpec::Lower(parse_index_field_ident(
            field, literal,
        )?)));
    }
    if let Some(field) = parse_single_argument_function(item, "UPPER")? {
        return Ok(Some(IndexExpressionSpec::Upper(parse_index_field_ident(
            field, literal,
        )?)));
    }
    if let Some(field) = parse_single_argument_function(item, "TRIM")? {
        return Ok(Some(IndexExpressionSpec::Trim(parse_index_field_ident(
            field, literal,
        )?)));
    }
    if let Some(field) = parse_single_argument_function(item, "DATE")? {
        return Ok(Some(IndexExpressionSpec::Date(parse_index_field_ident(
            field, literal,
        )?)));
    }
    if let Some(field) = parse_single_argument_function(item, "YEAR")? {
        return Ok(Some(IndexExpressionSpec::Year(parse_index_field_ident(
            field, literal,
        )?)));
    }
    if let Some(field) = parse_single_argument_function(item, "MONTH")? {
        return Ok(Some(IndexExpressionSpec::Month(parse_index_field_ident(
            field, literal,
        )?)));
    }
    if let Some(field) = parse_single_argument_function(item, "DAY")? {
        return Ok(Some(IndexExpressionSpec::Day(parse_index_field_ident(
            field, literal,
        )?)));
    }

    Err(
        DarlingError::custom(format!("unsupported index key item expression '{item}'"))
            .with_span(literal),
    )
}

fn parse_single_argument_function<'a>(
    input: &'a str,
    function_name: &str,
) -> Result<Option<&'a str>, DarlingError> {
    let trimmed = input.trim();
    if !trimmed.starts_with(function_name) {
        return Ok(None);
    }

    let open_index = function_name.len();
    if trimmed.as_bytes().get(open_index) != Some(&b'(') || !trimmed.ends_with(')') {
        return Err(DarlingError::custom(format!(
            "index key item expression '{trimmed}' must use canonical {function_name}(...) syntax"
        )));
    }

    Ok(Some(trimmed[open_index + 1..trimmed.len() - 1].trim()))
}

fn parse_index_field_ident(field: &str, literal: &LitStr) -> Result<Ident, DarlingError> {
    syn::parse_str::<Ident>(field).map_err(|_| {
        DarlingError::custom(format!(
            "index key item field '{field}' must be one bare field identifier"
        ))
        .with_span(literal)
    })
}

///
/// TESTS
///

#[cfg(test)]
mod tests {
    use crate::node::index::{Index, IndexExpressionSpec, IndexKeyItemSpec};
    use proc_macro2::Span;
    use syn::LitStr;

    #[test]
    fn parsed_key_items_accept_supported_expression_and_field_mix() {
        let index = Index {
            fields: LitStr::new("tenant_id, LOWER(email)", Span::call_site()),
            unique: true,
            predicate: None,
        };

        let key_items = index
            .parsed_key_items()
            .expect("supported index fields should parse");

        assert_eq!(
            key_items,
            vec![
                IndexKeyItemSpec::Field(syn::parse_quote!(tenant_id)),
                IndexKeyItemSpec::Expression(IndexExpressionSpec::Lower(syn::parse_quote!(email))),
            ],
        );
    }

    #[test]
    fn generated_name_uses_expression_key_item_canonical_text() {
        let index = Index {
            fields: LitStr::new("LOWER(email)", Span::call_site()),
            unique: false,
            predicate: None,
        };

        assert_eq!(index.generated_name("User"), "User|LOWER(email)");
    }
}
