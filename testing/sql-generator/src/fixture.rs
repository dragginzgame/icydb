//! Module: sql_generator::fixture
//! Responsibility: bounded canonical rows paired with generated SELECT cases.
//! Does not own: accepted snapshot semantics or SQL expression generation.
//! Boundary: stores values by durable field identity and validates them against snapshot facts.

use crate::{SelectSnapshot, SelectValueKind, SqlGeneratorError, SqlGeneratorErrorKind};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;

/// Reviewed lower signed-integer boundary required in scheduled fixture evidence.
pub(crate) const REVIEWED_INTEGER_MIN_BOUNDARY: i64 = -2_147_483_648;
/// Reviewed upper signed-integer boundary required in scheduled fixture evidence.
pub(crate) const REVIEWED_INTEGER_MAX_BOUNDARY: i64 = 2_147_483_647;

///
/// GeneratedValue
///
/// Exact scalar value admitted by the maintained SELECT generator and bundled
/// SQLite overlap. Integer payloads use tagged strings in replay JSON.
///

#[derive(Clone, Debug, Deserialize, Eq, Ord, PartialEq, PartialOrd, Serialize)]
#[serde(tag = "kind", content = "value", rename_all = "snake_case")]
pub enum GeneratedValue {
    /// A strict boolean value.
    Boolean(bool),

    /// A signed 64-bit integer serialized with an explicit type tag.
    Integer(#[serde(with = "tagged_i64")] i64),

    /// SQL `NULL` with the scalar type required by its expression context.
    Null(SelectValueKind),

    /// Valid UTF-8 text.
    Text(String),
}

impl GeneratedValue {
    /// Return the scalar type carried by this generated value.
    #[must_use]
    pub const fn value_kind(&self) -> SelectValueKind {
        match self {
            Self::Boolean(_) => SelectValueKind::Boolean,
            Self::Integer(_) => SelectValueKind::Integer,
            Self::Null(kind) => *kind,
            Self::Text(_) => SelectValueKind::Text,
        }
    }

    /// Return whether this value is SQL `NULL`.
    #[must_use]
    pub const fn is_null(&self) -> bool {
        matches!(self, Self::Null(_))
    }

    pub(crate) fn shrink_candidates(&self) -> Vec<Self> {
        match self {
            Self::Boolean(true) => vec![Self::Boolean(false)],
            Self::Boolean(false) | Self::Null(_) => Vec::new(),
            Self::Integer(value) => [0, value.signum()]
                .into_iter()
                .filter(|candidate| candidate != value)
                .map(Self::Integer)
                .collect(),
            Self::Text(value) if value.is_empty() => Vec::new(),
            Self::Text(value) => {
                let first = value.chars().next().map(|character| character.to_string());
                std::iter::once(String::new())
                    .chain(first)
                    .filter(|candidate| candidate != value)
                    .map(Self::Text)
                    .collect()
            }
        }
    }
}

///
/// GeneratedFixtureRow
///
/// One generated row represented in canonical durable-field-ID order.
/// Generated or unsupported accepted fields may be absent from the row.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratedFixtureRow {
    values: Vec<GeneratedFieldValue>,
}

impl GeneratedFixtureRow {
    pub(crate) const fn new(values: Vec<GeneratedFieldValue>) -> Self {
        Self { values }
    }

    /// Borrow the value assigned to one durable accepted field identity.
    #[must_use]
    pub fn value_by_field_id(&self, field_id: u32) -> Option<&GeneratedValue> {
        self.values
            .iter()
            .find(|value| value.field_id == field_id)
            .map(|value| &value.value)
    }

    /// Borrow the value assigned to one accepted field name.
    #[must_use]
    pub fn value_by_field_name<'a>(
        &'a self,
        snapshot: &SelectSnapshot,
        field_name: &str,
    ) -> Option<&'a GeneratedValue> {
        let field_id = snapshot
            .fields()
            .iter()
            .find(|field| field.name() == field_name)?
            .id();
        self.value_by_field_id(field_id)
    }

    pub(crate) const fn values(&self) -> &[GeneratedFieldValue] {
        self.values.as_slice()
    }
}

///
/// GeneratedFixture
///
/// Bounded canonical row set embedded in every generated replay case.
/// The generator owns row order; query result order remains a separate fact.
///

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub struct GeneratedFixture {
    rows: Vec<GeneratedFixtureRow>,
}

impl GeneratedFixture {
    pub(crate) const fn new(rows: Vec<GeneratedFixtureRow>) -> Self {
        Self { rows }
    }

    /// Borrow generated fixture rows in canonical construction order.
    #[must_use]
    pub const fn rows(&self) -> &[GeneratedFixtureRow] {
        self.rows.as_slice()
    }

    /// Return the number of embedded fixture rows.
    #[must_use]
    pub const fn len(&self) -> usize {
        self.rows.len()
    }

    /// Return whether the generated fixture is empty.
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.rows.is_empty()
    }

    /// Return whether any fixture cell contains a stored SQL null.
    pub(crate) fn has_stored_null(&self) -> bool {
        self.rows.iter().any(|row| {
            row.values()
                .iter()
                .any(|field_value| field_value.value.is_null())
        })
    }

    /// Return whether a field repeats one non-null value across fixture rows.
    pub(crate) fn has_duplicate_non_null_field_value(&self) -> bool {
        let mut observed = BTreeSet::new();
        for row in &self.rows {
            for field_value in row.values() {
                if field_value.value.is_null() {
                    continue;
                }
                if !observed.insert((field_value.field_id, &field_value.value)) {
                    return true;
                }
            }
        }

        false
    }

    /// Return whether the fixture contains either reviewed integer boundary.
    pub(crate) fn has_reviewed_numeric_boundary(&self) -> bool {
        self.rows.iter().any(|row| {
            row.values().iter().any(|field_value| {
                matches!(
                    field_value.value,
                    GeneratedValue::Integer(
                        REVIEWED_INTEGER_MIN_BOUNDARY | REVIEWED_INTEGER_MAX_BOUNDARY
                    )
                )
            })
        })
    }

    /// Return whether direct order keys tie while a direct projected field differs.
    pub(crate) fn has_observable_ordering_tie(
        &self,
        order_field_ids: &[u32],
        projected_field_ids: &[u32],
    ) -> bool {
        if order_field_ids.is_empty() || projected_field_ids.is_empty() {
            return false;
        }

        for (left_index, left) in self.rows.iter().enumerate() {
            for right in self.rows.iter().skip(left_index.saturating_add(1)) {
                let same_order_key = order_field_ids.iter().all(|field_id| {
                    matches!(
                        (
                            left.value_by_field_id(*field_id),
                            right.value_by_field_id(*field_id),
                        ),
                        (Some(left_value), Some(right_value)) if left_value == right_value
                    )
                });
                let different_projection = projected_field_ids.iter().any(|field_id| {
                    left.value_by_field_id(*field_id) != right.value_by_field_id(*field_id)
                });
                if same_order_key && different_projection {
                    return true;
                }
            }
        }

        false
    }

    /// Return the canonical BLAKE3 fingerprint of this fixture.
    ///
    /// # Errors
    ///
    /// Returns a typed serialization error if canonical JSON construction fails.
    pub fn fingerprint(&self) -> Result<String, SqlGeneratorError> {
        let bytes = crate::replay::canonical_json_bytes(self)?;
        Ok(blake3::hash(&bytes).to_hex().to_string())
    }

    pub(crate) fn validate(
        &self,
        snapshot: &SelectSnapshot,
        max_rows: u32,
    ) -> Result<(), SqlGeneratorError> {
        let row_count = u32::try_from(self.rows.len()).map_err(|_| {
            SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                "generated fixture row count exceeds u32",
            )
        })?;
        if row_count > max_rows {
            return Err(SqlGeneratorError::new(
                SqlGeneratorErrorKind::Budget,
                format!(
                    "generated fixture has {row_count} rows, exceeding the {max_rows}-row budget"
                ),
            ));
        }

        for (row_index, row) in self.rows.iter().enumerate() {
            let mut previous_id = None;
            for field_value in row.values() {
                if previous_id.is_some_and(|previous| previous >= field_value.field_id) {
                    return Err(SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidCase,
                        format!(
                            "generated fixture row {row_index} is not in unique field-ID order"
                        ),
                    ));
                }
                previous_id = Some(field_value.field_id);
                let field = snapshot.field_by_id(field_value.field_id).ok_or_else(|| {
                    SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidSnapshot,
                        format!(
                            "generated fixture row {row_index} references unknown field {}",
                            field_value.field_id
                        ),
                    )
                })?;
                if field.primary_key() || field.generated() || !field.kind().is_generated_scalar() {
                    return Err(SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidCase,
                        format!(
                            "generated fixture row {row_index} writes ineligible field {:?}",
                            field.name()
                        ),
                    ));
                }
                if field_value.value.is_null() && !field.nullable() {
                    return Err(SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidCase,
                        format!(
                            "generated fixture row {row_index} assigns NULL to non-null field {:?}",
                            field.name()
                        ),
                    ));
                }
                if field.kind().value_kind() != Some(field_value.value.value_kind()) {
                    return Err(SqlGeneratorError::new(
                        SqlGeneratorErrorKind::InvalidCase,
                        format!(
                            "generated fixture row {row_index} assigns {:?} to {:?}",
                            field_value.value.value_kind(),
                            field.kind()
                        ),
                    ));
                }
            }
        }

        Ok(())
    }

    pub(crate) fn without_row(&self, index: usize) -> Option<Self> {
        if index >= self.rows.len() {
            return None;
        }
        let mut rows = self.rows.clone();
        rows.remove(index);
        Some(Self { rows })
    }
}

#[derive(Clone, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GeneratedFieldValue {
    #[serde(with = "crate::model::tagged_u32")]
    field_id: u32,
    value: GeneratedValue,
}

impl GeneratedFieldValue {
    pub(crate) const fn new(field_id: u32, value: GeneratedValue) -> Self {
        Self { field_id, value }
    }
}

mod tagged_i64 {
    use serde::{Deserialize, Deserializer, Serializer, de::Error as _};

    #[expect(
        clippy::trivially_copy_pass_by_ref,
        reason = "Serde with-module serializers receive borrowed field values"
    )]
    pub(super) fn serialize<S>(value: &i64, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&format!("i64:{value}"))
    }

    pub(super) fn deserialize<'de, D>(deserializer: D) -> Result<i64, D::Error>
    where
        D: Deserializer<'de>,
    {
        let tagged = String::deserialize(deserializer)?;
        tagged
            .strip_prefix("i64:")
            .ok_or_else(|| D::Error::custom("expected i64: tagged integer"))?
            .parse::<i64>()
            .map_err(D::Error::custom)
    }
}
