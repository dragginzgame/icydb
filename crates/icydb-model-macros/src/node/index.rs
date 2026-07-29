//! Module: node::index
//! Responsibility: derive-side node parsing.
//! Does not own: runtime schema semantics.
//! Boundary: macro metadata to node models.

use crate::{
    node::{
        Entity,
        field_list_arg::{
            field_or_fields_duplicate_message, parse_field_list_arg, parse_scalar_field_arg,
        },
    },
    predicate::{self, CompareOp, CompareOperand, Literal, Predicate},
    prelude::*,
};
use darling::ast::NestedMeta;
use icydb_schema::canonical_index_name_slug;

///
/// Index
///

#[derive(Debug)]
pub struct Index {
    pub(crate) fields: Vec<LitStr>,

    pub(crate) unique: bool,

    // Raw SQL predicate text is accepted at the derive boundary and lowered
    // into canonical predicate semantics during macro expansion.
    pub(crate) predicate: Option<String>,
}

impl FromMeta for Index {
    fn from_list(items: &[NestedMeta]) -> Result<Self, DarlingError> {
        let mut fields = None;
        let mut unique = false;
        let mut unique_seen = false;
        let mut predicate = None;

        for item in items {
            match item {
                NestedMeta::Meta(syn::Meta::Path(path)) if path.is_ident("unique") => {
                    if unique_seen {
                        return Err(DarlingError::custom(
                            "index(...) accepts only one unique argument",
                        )
                        .with_span(path));
                    }
                    unique = true;
                    unique_seen = true;
                }
                NestedMeta::Meta(syn::Meta::NameValue(name_value)) => {
                    if name_value.path.is_ident("field") {
                        set_index_arg_once(
                            &mut fields,
                            vec![parse_scalar_field_arg("index", &name_value.value)?],
                            field_or_fields_duplicate_message("index"),
                            &name_value.path,
                        )?;
                        continue;
                    }

                    if name_value.path.is_ident("fields") {
                        set_index_arg_once(
                            &mut fields,
                            parse_field_list_arg("index", &name_value.value)?,
                            field_or_fields_duplicate_message("index"),
                            &name_value.path,
                        )?;
                        continue;
                    }

                    if name_value.path.is_ident("unique") {
                        if unique_seen {
                            return Err(DarlingError::custom(
                                "index(...) accepts only one unique argument",
                            )
                            .with_span(&name_value.path));
                        }
                        unique = parse_index_bool_arg(&name_value.value)?;
                        unique_seen = true;
                        continue;
                    }

                    if name_value.path.is_ident("predicate") {
                        set_index_arg_once(
                            &mut predicate,
                            parse_index_string_arg(&name_value.value)?,
                            "index(...) accepts only one predicate = \"...\" argument",
                            &name_value.path,
                        )?;
                        continue;
                    }

                    return Err(DarlingError::custom(
                        "index(...) supports field = \"...\", fields = [...], unique, and predicate = \"...\"",
                    )
                    .with_span(&name_value.path));
                }
                NestedMeta::Meta(syn::Meta::Path(path)) => {
                    return Err(DarlingError::custom(
                        "index(...) supports field = \"...\", fields = [...], unique, and predicate = \"...\"",
                    )
                    .with_span(path));
                }
                _ => {
                    return Err(DarlingError::custom(
                        "index(...) supports field = \"...\", fields = [...], unique, and predicate = \"...\"",
                    ));
                }
            }
        }

        let fields = fields.ok_or_else(|| {
            DarlingError::custom("index(...) requires field = \"...\" or fields = [...]")
        })?;

        if fields.is_empty() {
            return Err(DarlingError::custom(
                "index(fields = []) must contain at least one field",
            ));
        }

        Ok(Self {
            fields,
            unique,
            predicate,
        })
    }
}

fn set_index_arg_once<T>(
    target: &mut Option<T>,
    value: T,
    duplicate_message: impl std::fmt::Display,
    span: &syn::Path,
) -> Result<(), DarlingError> {
    if target.replace(value).is_some() {
        return Err(DarlingError::custom(duplicate_message).with_span(span));
    }
    Ok(())
}

fn parse_index_string_arg(expr: &syn::Expr) -> Result<String, DarlingError> {
    let syn::Expr::Lit(expr_lit) = expr else {
        return Err(
            DarlingError::custom("index(predicate = ...) requires a string literal")
                .with_span(expr),
        );
    };
    let syn::Lit::Str(literal) = &expr_lit.lit else {
        return Err(
            DarlingError::custom("index(predicate = ...) requires a string literal")
                .with_span(expr),
        );
    };

    Ok(literal.value())
}

fn parse_index_bool_arg(expr: &syn::Expr) -> Result<bool, DarlingError> {
    let syn::Expr::Lit(expr_lit) = expr else {
        return Err(
            DarlingError::custom("index(unique = ...) requires true or false").with_span(expr),
        );
    };
    let syn::Lit::Bool(literal) = &expr_lit.lit else {
        return Err(
            DarlingError::custom("index(unique = ...) requires true or false").with_span(expr),
        );
    };

    Ok(literal.value)
}

impl HasSchemaPart for Index {
    fn schema_part(&self) -> TokenStream {
        TokenStream::new()
    }
}

impl Index {
    pub(crate) fn schema_part_for_entity(
        &self,
        entity: &Entity,
        entity_name: &str,
    ) -> Result<TokenStream, DarlingError> {
        let name = self.generated_name(entity_name);
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
        let predicate_expression = self
            .validated_generated_predicate(entity)?
            .map(|predicate| {
                predicate_source_expression_tokens(&predicate, entity)
                    .map(|expression| quote! { Some(|_schema| #expression) })
            })
            .transpose()?
            .unwrap_or_else(|| quote! { None });

        Ok(quote! {
            ::icydb_model::node::Index::new_with_key_items_and_predicate(
                #name,
                #fields,
                #key_items,
                #unique,
                #predicate,
                #predicate_expression,
            )
        })
    }

    /// Build the canonical index name (`idx_entity__key_item...`) shared across
    /// validation and codegen.
    pub fn generated_name(&self, entity_name: &str) -> String {
        let prefix = if self.unique { "uniq" } else { "idx" };
        let entity = canonical_index_name_slug(entity_name);
        let fields = self
            .generated_name_segments()
            .iter()
            .map(|field| canonical_index_name_slug(field))
            .collect::<Vec<_>>()
            .join("_");
        format!("{prefix}_{entity}__{fields}")
    }

    pub(crate) fn parsed_key_items(&self) -> Result<Vec<IndexKeyItemSpec>, DarlingError> {
        parse_index_key_items(self.fields.as_slice())
    }

    pub(crate) fn referenced_field_literals(&self) -> Result<Vec<(Ident, LitStr)>, DarlingError> {
        self.fields
            .iter()
            .map(|item| {
                let key_item = parse_index_key_item(item.value().trim(), item)?;
                Ok((key_item.field_ident().clone(), item.clone()))
            })
            .collect()
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
            .map(IndexKeyItemSpec::schema_tokens)
            .collect::<Vec<_>>();

        quote! { Some(&[#(#key_items),*]) }
    }

    pub(crate) fn validated_generated_predicate(
        &self,
        entity: &Entity,
    ) -> Result<Option<Predicate>, DarlingError> {
        let Some(predicate_sql) = self.predicate.as_deref() else {
            return Ok(None);
        };

        let predicate = predicate::parse(predicate_sql)?;
        validate_predicate_fields(entity, &predicate, false)?;

        Ok(Some(predicate))
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

    fn schema_tokens(&self) -> TokenStream {
        match self {
            Self::Field(field) => {
                let field = to_str_lit(field);
                quote! { ::icydb_model::node::IndexKeyItem::Field(#field) }
            }
            Self::Expression(expression) => {
                let expression = expression.schema_tokens();
                quote! { ::icydb_model::node::IndexKeyItem::Expression(#expression) }
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

    fn schema_tokens(&self) -> TokenStream {
        let field = to_str_lit(self.field_ident());

        match self {
            Self::Lower(_) => quote! { ::icydb_model::node::IndexExpression::Lower(#field) },
            Self::Upper(_) => quote! { ::icydb_model::node::IndexExpression::Upper(#field) },
            Self::Trim(_) => quote! { ::icydb_model::node::IndexExpression::Trim(#field) },
            Self::LowerTrim(_) => {
                quote! { ::icydb_model::node::IndexExpression::LowerTrim(#field) }
            }
            Self::Date(_) => quote! { ::icydb_model::node::IndexExpression::Date(#field) },
            Self::Year(_) => quote! { ::icydb_model::node::IndexExpression::Year(#field) },
            Self::Month(_) => quote! { ::icydb_model::node::IndexExpression::Month(#field) },
            Self::Day(_) => quote! { ::icydb_model::node::IndexExpression::Day(#field) },
        }
    }
}

fn parse_index_key_items(fields: &[LitStr]) -> Result<Vec<IndexKeyItemSpec>, DarlingError> {
    if fields.is_empty() {
        return Err(DarlingError::custom(
            "index fields must reference at least one key item",
        ));
    }

    fields
        .iter()
        .map(|item| parse_index_key_item(item.value().trim(), item))
        .collect()
}

pub(crate) fn validate_predicate_fields(
    entity: &Entity,
    predicate: &Predicate,
    custom_fields_are_enums: bool,
) -> Result<(), DarlingError> {
    for field_name in predicate::referenced_fields(predicate) {
        let field = predicate_field(entity, field_name.as_str())?;
        if field.value.cardinality() == Cardinality::Many {
            return Err(DarlingError::custom(format!(
                "generated schema predicate field '{field_name}' cannot have many cardinality"
            )));
        }
        if field.value.item.is.is_some() && !custom_fields_are_enums {
            return Err(DarlingError::custom(
                "filtered index predicates on custom field types are not supported at build time",
            ));
        }
    }
    validate_predicate_shape(entity, predicate, custom_fields_are_enums)
}

fn validate_predicate_shape(
    entity: &Entity,
    predicate: &Predicate,
    custom_fields_are_enums: bool,
) -> Result<(), DarlingError> {
    match predicate {
        Predicate::Bool(_) => Ok(()),
        Predicate::And(children) | Predicate::Or(children) => {
            for child in children {
                validate_predicate_shape(entity, child, custom_fields_are_enums)?;
            }
            Ok(())
        }
        Predicate::Not(inner) => validate_predicate_shape(entity, inner, custom_fields_are_enums),
        Predicate::Compare { field, op, operand } => {
            let left = predicate_field(entity, field)?;
            validate_comparison_operand(entity, left, *op, operand, custom_fields_are_enums)
        }
        Predicate::IsNull { field, .. } => {
            let _ = predicate_field(entity, field)?;
            Ok(())
        }
        Predicate::In { field, values, .. } => {
            let field = predicate_field(entity, field)?;
            for value in values {
                validate_literal_for_field(field, value, custom_fields_are_enums)?;
            }
            Ok(())
        }
    }
}

fn validate_comparison_operand(
    entity: &Entity,
    field: &Field,
    op: CompareOp,
    operand: &CompareOperand,
    custom_fields_are_enums: bool,
) -> Result<(), DarlingError> {
    if field.value.item.is.is_some() && op.is_ordering() {
        return Err(DarlingError::custom(
            "generated check enum fields support only equality or bounded membership",
        ));
    }
    if let Some(primitive) = field.value.item.primitive
        && op.is_ordering()
        && !primitive.supports_ord()
    {
        return Err(DarlingError::custom(format!(
            "generated schema predicate cannot order primitive {primitive:?}"
        )));
    }
    match operand {
        CompareOperand::Field(other) => {
            let other = predicate_field(entity, other)?;
            if field.value.cardinality() != other.value.cardinality()
                || field.value.item.primitive != other.value.item.primitive
                || field.value.item.is != other.value.item.is
            {
                return Err(DarlingError::custom(
                    "generated schema field comparison requires matching field types",
                ));
            }
            Ok(())
        }
        CompareOperand::Literal(literal) => {
            validate_literal_for_field(field, literal, custom_fields_are_enums)
        }
    }
}

fn validate_literal_for_field(
    field: &Field,
    literal: &Literal,
    custom_fields_are_enums: bool,
) -> Result<(), DarlingError> {
    if field.value.item.is.is_some() {
        if custom_fields_are_enums && matches!(literal, Literal::Text(_)) {
            return Ok(());
        }
        return Err(DarlingError::custom(
            "generated custom-field predicates require exact enum text literals",
        ));
    }
    let primitive = field.value.item.primitive.unwrap_or(Primitive::Unit);
    let compatible = match primitive {
        Primitive::Bool => matches!(literal, Literal::Bool(_)),
        Primitive::Decimal
        | Primitive::Float32
        | Primitive::Float64
        | Primitive::Int8
        | Primitive::Int16
        | Primitive::Int32
        | Primitive::Int64
        | Primitive::Int128
        | Primitive::Nat8
        | Primitive::Nat16
        | Primitive::Nat32
        | Primitive::Nat64
        | Primitive::Nat128 => matches!(literal, Literal::Number(_)),
        Primitive::Text => matches!(literal, Literal::Text(_)),
        Primitive::Account
        | Primitive::Blob
        | Primitive::Date
        | Primitive::Duration
        | Primitive::IntBig
        | Primitive::NatBig
        | Primitive::Principal
        | Primitive::Subaccount
        | Primitive::Timestamp
        | Primitive::Ulid
        | Primitive::Unit => false,
    };
    compatible.then_some(()).ok_or_else(|| {
        DarlingError::custom(format!(
            "generated schema predicate literal is incompatible with primitive {primitive:?}"
        ))
    })
}

fn predicate_field<'a>(entity: &'a Entity, field_name: &str) -> Result<&'a Field, DarlingError> {
    entity
        .fields
        .iter()
        .find(|field| field.name == field_name)
        .ok_or_else(|| DarlingError::custom(format!("unknown schema field '{field_name}'")))
}

pub(crate) fn predicate_source_expression_tokens(
    predicate: &Predicate,
    entity: &Entity,
) -> Result<TokenStream, DarlingError> {
    let instructions = source_instruction_tokens(predicate, entity)?;
    Ok(quote! {
        ::icydb_model::schema::SourceCheckExpr::try_new(vec![#(#instructions),*])
    })
}

fn source_instruction_tokens(
    predicate: &Predicate,
    entity: &Entity,
) -> Result<Vec<TokenStream>, DarlingError> {
    match predicate {
        Predicate::Bool(value) => Ok(vec![
            quote! {
                ::icydb_model::schema::SourceCheckInstruction::Literal(
                    ::icydb_model::schema::ScalarLiteral::Bool(#value),
                )
            },
            quote! {
                ::icydb_model::schema::SourceCheckInstruction::Literal(
                    ::icydb_model::schema::ScalarLiteral::Bool(true),
                )
            },
            quote! { ::icydb_model::schema::SourceCheckInstruction::Equal },
        ]),
        Predicate::And(children) | Predicate::Or(children) => {
            let operator = if matches!(predicate, Predicate::And(_)) {
                quote! { ::icydb_model::schema::SourceCheckInstruction::And }
            } else {
                quote! { ::icydb_model::schema::SourceCheckInstruction::Or }
            };
            fold_source_children(children, entity, operator)
        }
        Predicate::Not(inner) => {
            let mut instructions = source_instruction_tokens(inner, entity)?;
            instructions.push(quote! { ::icydb_model::schema::SourceCheckInstruction::Not });
            Ok(instructions)
        }
        Predicate::Compare { field, op, operand } => {
            let mut instructions = vec![source_field_instruction(entity, field)?];
            instructions.push(match operand {
                CompareOperand::Field(other) => source_field_instruction(entity, other)?,
                CompareOperand::Literal(literal) => {
                    source_literal_instruction(entity, field, literal)?
                }
            });
            instructions.push(source_compare_operator(*op));
            Ok(instructions)
        }
        Predicate::IsNull { field, negated } => {
            let mut instructions = vec![source_field_instruction(entity, field)?];
            instructions.push(if *negated {
                quote! { ::icydb_model::schema::SourceCheckInstruction::IsNotNull }
            } else {
                quote! { ::icydb_model::schema::SourceCheckInstruction::IsNull }
            });
            Ok(instructions)
        }
        Predicate::In {
            field,
            values,
            negated,
        } => {
            let mut values = values.iter();
            let first = values
                .next()
                .ok_or_else(|| DarlingError::custom("membership predicate cannot be empty"))?;
            let mut instructions = source_equality_instruction_tokens(entity, field, first)?;
            for value in values {
                instructions.extend(source_equality_instruction_tokens(entity, field, value)?);
                instructions.push(quote! { ::icydb_model::schema::SourceCheckInstruction::Or });
            }
            if *negated {
                instructions.push(quote! { ::icydb_model::schema::SourceCheckInstruction::Not });
            }
            Ok(instructions)
        }
    }
}

fn fold_source_children(
    children: &[Predicate],
    entity: &Entity,
    operator: TokenStream,
) -> Result<Vec<TokenStream>, DarlingError> {
    let mut children = children.iter();
    let first = children
        .next()
        .ok_or_else(|| DarlingError::custom("generated predicate boolean group is empty"))?;
    let mut instructions = source_instruction_tokens(first, entity)?;
    for child in children {
        instructions.extend(source_instruction_tokens(child, entity)?);
        instructions.push(operator.clone());
    }
    Ok(instructions)
}

fn source_equality_instruction_tokens(
    entity: &Entity,
    field: &str,
    value: &Literal,
) -> Result<Vec<TokenStream>, DarlingError> {
    Ok(vec![
        source_field_instruction(entity, field)?,
        source_literal_instruction(entity, field, value)?,
        quote! { ::icydb_model::schema::SourceCheckInstruction::Equal },
    ])
}

fn source_field_instruction(
    entity: &Entity,
    field_name: &str,
) -> Result<TokenStream, DarlingError> {
    let field = predicate_field(entity, field_name)?;
    let name = quote_one(&field.name, to_str_lit);
    Ok(quote! {
        ::icydb_model::schema::SourceCheckInstruction::Field(
            ::icydb_model::schema::FieldSourceKey::try_new(#name)?
        )
    })
}

fn source_literal_instruction(
    entity: &Entity,
    field_name: &str,
    value: &Literal,
) -> Result<TokenStream, DarlingError> {
    let field = predicate_field(entity, field_name)?;
    let literal = if let Some(enum_path) = field.value.item.is.as_ref() {
        let Literal::Text(variant) = value else {
            return Err(DarlingError::custom(
                "generated enum predicate literals must name unit variants",
            ));
        };
        quote! {
            _schema.enum_unit_literal(
                <#enum_path as ::icydb_model::node::Path>::PATH,
                #variant,
            )?
        }
    } else {
        source_scalar_literal(field, value)?
    };
    Ok(quote! {
        ::icydb_model::schema::SourceCheckInstruction::Literal(#literal)
    })
}

fn source_scalar_literal(field: &Field, value: &Literal) -> Result<TokenStream, DarlingError> {
    let primitive = field.value.item.primitive.unwrap_or(Primitive::Unit);
    Ok(match (primitive, value) {
        (Primitive::Bool, Literal::Bool(value)) => {
            quote! { ::icydb_model::schema::ScalarLiteral::Bool(#value) }
        }
        (Primitive::Text, Literal::Text(value)) => {
            quote! { ::icydb_model::schema::ScalarLiteral::Text(#value.to_string()) }
        }
        (Primitive::Decimal, Literal::Number(value)) => quote! {
            ::icydb_model::schema::ScalarLiteral::Decimal(
                <::icydb_model::schema::Decimal as ::std::str::FromStr>::from_str(#value)
                    .map_err(|_| ::icydb_model::schema::SchemaContractError::InvalidLiteral)?
            )
        },
        (
            Primitive::Int8
            | Primitive::Int16
            | Primitive::Int32
            | Primitive::Int64
            | Primitive::Int128,
            Literal::Number(value),
        ) => {
            let value = value.parse::<i128>().map_err(|_| {
                DarlingError::custom("generated signed integer predicate literal is out of range")
            })?;
            quote! { ::icydb_model::schema::ScalarLiteral::Int(#value) }
        }
        (
            Primitive::Nat8
            | Primitive::Nat16
            | Primitive::Nat32
            | Primitive::Nat64
            | Primitive::Nat128,
            Literal::Number(value),
        ) => {
            let value = value.parse::<u128>().map_err(|_| {
                DarlingError::custom("generated unsigned integer predicate literal is out of range")
            })?;
            quote! { ::icydb_model::schema::ScalarLiteral::Nat(#value) }
        }
        (Primitive::Float32, Literal::Number(value)) => {
            let value = value.parse::<f32>().map_err(|_| {
                DarlingError::custom("generated Float32 predicate literal is invalid")
            })?;
            quote! {
                ::icydb_model::schema::ScalarLiteral::Float32(
                    ::icydb_model::schema::Float32::try_new(#value)
                        .ok_or(::icydb_model::schema::SchemaContractError::InvalidLiteral)?
                )
            }
        }
        (Primitive::Float64, Literal::Number(value)) => {
            let value = value.parse::<f64>().map_err(|_| {
                DarlingError::custom("generated Float64 predicate literal is invalid")
            })?;
            quote! {
                ::icydb_model::schema::ScalarLiteral::Float64(
                    ::icydb_model::schema::Float64::try_new(#value)
                        .ok_or(::icydb_model::schema::SchemaContractError::InvalidLiteral)?
                )
            }
        }
        _ => {
            return Err(DarlingError::custom(format!(
                "generated schema predicate does not support a {primitive:?} literal"
            )));
        }
    })
}

fn source_compare_operator(op: CompareOp) -> TokenStream {
    match op {
        CompareOp::Eq => quote! { ::icydb_model::schema::SourceCheckInstruction::Equal },
        CompareOp::Ne => quote! { ::icydb_model::schema::SourceCheckInstruction::NotEqual },
        CompareOp::Lt => quote! { ::icydb_model::schema::SourceCheckInstruction::LessThan },
        CompareOp::Lte => {
            quote! { ::icydb_model::schema::SourceCheckInstruction::LessThanOrEqual }
        }
        CompareOp::Gt => quote! { ::icydb_model::schema::SourceCheckInstruction::GreaterThan },
        CompareOp::Gte => {
            quote! { ::icydb_model::schema::SourceCheckInstruction::GreaterThanOrEqual }
        }
    }
}

fn parse_index_key_item(item: &str, literal: &LitStr) -> Result<IndexKeyItemSpec, DarlingError> {
    if item.is_empty() {
        return Err(
            DarlingError::custom("index fields contains an empty key item").with_span(literal),
        );
    }
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
    use darling::{FromMeta, ast::NestedMeta};
    use proc_macro2::Span;
    use quote::quote;
    use syn::LitStr;

    fn field_list(values: &[&str]) -> Vec<LitStr> {
        values
            .iter()
            .map(|value| LitStr::new(value, Span::call_site()))
            .collect()
    }

    fn parse_index(tokens: proc_macro2::TokenStream) -> Result<Index, darling::Error> {
        let args = NestedMeta::parse_meta_list(tokens).expect("test meta should parse");
        Index::from_list(&args)
    }

    #[test]
    fn from_list_parses_scalar_field_shorthand() {
        let index =
            parse_index(quote!(field = "email")).expect("scalar index shorthand should parse");

        assert_eq!(
            index.fields.iter().map(LitStr::value).collect::<Vec<_>>(),
            ["email"],
        );
        assert!(!index.unique);
        assert_eq!(index.predicate, None);
    }

    #[test]
    fn from_list_parses_fields_unique_and_predicate() {
        let index = parse_index(quote!(
            fields = ["tenant_id", "LOWER(email)"],
            unique,
            predicate = "active = true"
        ))
        .expect("index fields syntax should parse");

        assert_eq!(
            index.fields.iter().map(LitStr::value).collect::<Vec<_>>(),
            ["tenant_id", "LOWER(email)"],
        );
        assert!(index.unique);
        assert_eq!(index.predicate.as_deref(), Some("active = true"));
    }

    #[test]
    fn from_list_rejects_mixed_field_and_fields_syntax() {
        let err = parse_index(quote!(field = "email", fields = ["tenant_id", "email"]))
            .expect_err("index field and fields syntax should be mutually exclusive");

        assert!(
            err.to_string().contains(
                "index(...) accepts either one field = \"...\" argument or one fields = [...] argument"
            ),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn from_list_rejects_comma_string_fields() {
        let err = parse_index(quote!(fields = "tenant_id, email"))
            .expect_err("index fields should reject comma strings");

        assert!(
            err.to_string().contains("not a comma-string"),
            "unexpected error: {err}",
        );
    }

    #[test]
    fn parsed_key_items_accept_supported_expression_and_field_mix() {
        let index = Index {
            fields: field_list(&["tenant_id", "LOWER(email)"]),
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
            fields: field_list(&["LOWER(email)"]),
            unique: false,
            predicate: None,
        };

        assert_eq!(index.generated_name("User"), "idx_user__lower_email");
    }
}
