//! Module: sqlite_reference::value
//! Responsibility: exact common value and row-result representation.
//! Does not own: IcyDB output conversion or SQLite connection policy.
//! Boundary: keeps comparisons typed and preserves nulls, bytes, order, and duplicates.

use crate::{SqliteAdapterError, SqliteAdapterErrorKind};

/// Domain separator for canonical typed differential-result fingerprints.
const SQLITE_REFERENCE_RESULT_FINGERPRINT_DOMAIN: &[u8] = b"icydb-sqlite-reference-result/v1";

///
/// SqliteReferenceColumnKind
///
/// Exact SQLite/IcyDB value family admitted by one reference-result column.
/// The initial overlap deliberately excludes floating point values.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqliteReferenceColumnKind {
    /// Arbitrary byte strings.
    Blob,
    /// Boolean values encoded by SQLite as zero or one integers.
    Boolean,
    /// Exact decimal values represented by SQLite integer results at scale zero.
    Decimal,
    /// Signed 64-bit integers.
    Integer,
    /// Valid UTF-8 text.
    Text,
}

///
/// SqliteReferenceRowOrder
///
/// Declares whether row position participates in the comparison contract.
/// Unordered results are canonicalized as a typed row multiset.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum SqliteReferenceRowOrder {
    /// Row position participates in equality.
    Ordered,
    /// Row position is ignored after typed canonicalization.
    Unordered,
}

///
/// SqliteReferenceValue
///
/// Lossless value overlap shared by bundled SQLite and IcyDB correctness runs.
/// Values are orderable solely to canonicalize unordered result multisets.
///

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub enum SqliteReferenceValue {
    /// Arbitrary bytes preserved without text coercion.
    Blob(Vec<u8>),
    /// A strict boolean value.
    Boolean(bool),
    /// An exact decimal coefficient and scale.
    Decimal { mantissa: i128, scale: u32 },
    /// A signed 64-bit integer.
    Integer(i64),
    /// SQL `NULL`.
    Null,
    /// Valid UTF-8 text.
    Text(String),
}

///
/// SqliteReferenceResult
///
/// Typed columns, rows, and ordering contract for one differential result.
/// Construction validates rectangular row shape before comparison.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct SqliteReferenceResult {
    columns: Vec<String>,
    rows: Vec<Vec<SqliteReferenceValue>>,
    row_order: SqliteReferenceRowOrder,
}

impl SqliteReferenceResult {
    /// Build and validate one typed reference result.
    ///
    /// # Errors
    ///
    /// Returns a typed result error when any row width differs from the
    /// declared projection width.
    pub fn try_new(
        columns: Vec<String>,
        mut rows: Vec<Vec<SqliteReferenceValue>>,
        row_order: SqliteReferenceRowOrder,
    ) -> Result<Self, SqliteAdapterError> {
        if let Some((row_index, row)) = rows
            .iter()
            .enumerate()
            .find(|(_, row)| row.len() != columns.len())
        {
            return Err(SqliteAdapterError::new(
                SqliteAdapterErrorKind::Result,
                format!(
                    "row {row_index} has {} values for {} columns",
                    row.len(),
                    columns.len(),
                ),
            ));
        }

        if row_order == SqliteReferenceRowOrder::Unordered {
            rows.sort();
        }

        Ok(Self {
            columns,
            rows,
            row_order,
        })
    }

    /// Borrow the ordered projection labels.
    #[must_use]
    pub const fn columns(&self) -> &[String] {
        self.columns.as_slice()
    }

    /// Borrow the validated typed rows.
    #[must_use]
    pub const fn rows(&self) -> &[Vec<SqliteReferenceValue>] {
        self.rows.as_slice()
    }

    /// Return the row-order comparison contract.
    #[must_use]
    pub const fn row_order(&self) -> SqliteReferenceRowOrder {
        self.row_order
    }

    /// Compute the canonical typed result fingerprint embedded in failure replay.
    ///
    /// # Errors
    ///
    /// Returns a typed result error if an in-memory collection length cannot be
    /// represented by the fixed 64-bit canonical encoding.
    pub fn fingerprint(&self) -> Result<String, SqliteAdapterError> {
        let mut hasher = blake3::Hasher::new();
        hasher.update(SQLITE_REFERENCE_RESULT_FINGERPRINT_DOMAIN);
        hasher.update(&[match self.row_order {
            SqliteReferenceRowOrder::Ordered => 0,
            SqliteReferenceRowOrder::Unordered => 1,
        }]);
        update_length(&mut hasher, self.columns.len())?;
        for column in &self.columns {
            update_bytes(&mut hasher, column.as_bytes())?;
        }
        update_length(&mut hasher, self.rows.len())?;
        for row in &self.rows {
            update_length(&mut hasher, row.len())?;
            for value in row {
                update_value(&mut hasher, value)?;
            }
        }

        Ok(format!("blake3.{}", hasher.finalize().to_hex()))
    }
}

fn update_value(
    hasher: &mut blake3::Hasher,
    value: &SqliteReferenceValue,
) -> Result<(), SqliteAdapterError> {
    match value {
        SqliteReferenceValue::Blob(value) => {
            hasher.update(&[0]);
            update_bytes(hasher, value)
        }
        SqliteReferenceValue::Boolean(value) => {
            hasher.update(&[1, u8::from(*value)]);
            Ok(())
        }
        SqliteReferenceValue::Decimal { mantissa, scale } => {
            hasher.update(&[2]);
            hasher.update(&mantissa.to_be_bytes());
            hasher.update(&scale.to_be_bytes());
            Ok(())
        }
        SqliteReferenceValue::Integer(value) => {
            hasher.update(&[3]);
            hasher.update(&value.to_be_bytes());
            Ok(())
        }
        SqliteReferenceValue::Null => {
            hasher.update(&[4]);
            Ok(())
        }
        SqliteReferenceValue::Text(value) => {
            hasher.update(&[5]);
            update_bytes(hasher, value.as_bytes())
        }
    }
}

fn update_bytes(hasher: &mut blake3::Hasher, bytes: &[u8]) -> Result<(), SqliteAdapterError> {
    update_length(hasher, bytes.len())?;
    hasher.update(bytes);
    Ok(())
}

fn update_length(hasher: &mut blake3::Hasher, length: usize) -> Result<(), SqliteAdapterError> {
    let length = u64::try_from(length).map_err(|_| {
        SqliteAdapterError::new(
            SqliteAdapterErrorKind::Result,
            "typed reference result length exceeds its canonical 64-bit encoding",
        )
    })?;
    hasher.update(&length.to_be_bytes());
    Ok(())
}
