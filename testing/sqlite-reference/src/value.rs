//! Module: sqlite_reference::value
//! Responsibility: exact common value and row-result representation.
//! Does not own: IcyDB output conversion or SQLite connection policy.
//! Boundary: keeps comparisons typed and preserves nulls, bytes, order, and duplicates.

use crate::{SqliteAdapterError, SqliteAdapterErrorKind};

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
}
