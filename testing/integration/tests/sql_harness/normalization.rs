//! Module: sql_harness::normalization
//! Responsibility: lossless typed result normalization for correctness comparisons.
//! Does not own: SQL value semantics, query execution, or verdict attribution.
//! Boundary: compares runner results while preserving row order, shape, and value identity.

use crate::sql_harness::RowOrder;

///
/// NormalizedCell
///
/// Lossless test-harness representation of one SQL result cell.
/// Owned by result normalization and populated by correctness-aware runners.
///

#[derive(Clone, Debug, Eq, Ord, PartialEq, PartialOrd)]
pub(crate) enum NormalizedCell {
    Null,
    Bool(bool),
    Int(i128),
    Nat(u128),
    Decimal { coefficient: i128, scale: u32 },
    FloatBits(u64),
    Text(String),
    Bytes(Vec<u8>),
}

///
/// NormalizedResult
///
/// Column and row data normalized under an explicit row-order contract.
/// Owned by result normalization and compared by the shared verdict layer.
///

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct NormalizedResult {
    /// Ordered output-column names.
    pub(crate) columns: Vec<String>,

    /// Normalized rows in observed execution order.
    pub(crate) rows: Vec<Vec<NormalizedCell>>,

    /// Whether observed row position is part of the result contract.
    pub(crate) row_order: RowOrder,
}

impl NormalizedResult {
    /// Return rows in comparison order without mutating the observed result.
    fn comparison_rows(&self) -> Vec<Vec<NormalizedCell>> {
        let mut rows = self.rows.clone();
        if self.row_order == RowOrder::Unordered {
            rows.sort();
        }
        rows
    }
}

///
/// NormalizationMismatchKind
///
/// Stable category for a mismatch between two normalized results.
/// Owned by result normalization and translated into verdict categories by the harness.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) enum NormalizationMismatchKind {
    ColumnShape,
    OrderingContract,
    RowCount,
    RowShape,
    Value,
}

///
/// NormalizationMismatch
///
/// Typed location and category of a normalized-result mismatch.
/// Owned by result normalization and consumed by correctness verdict attribution.
///

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct NormalizationMismatch {
    /// Category of the first detected mismatch.
    pub(crate) kind: NormalizationMismatchKind,

    /// Zero-based mismatching row, when the category has a row location.
    pub(crate) row: Option<usize>,

    /// Zero-based mismatching column, when the category has a cell location.
    pub(crate) column: Option<usize>,
}

impl NormalizationMismatch {
    /// Build one typed mismatch at its optional row and column location.
    const fn new(
        kind: NormalizationMismatchKind,
        row: Option<usize>,
        column: Option<usize>,
    ) -> Self {
        Self { kind, row, column }
    }
}

/// Compare two normalized results under their declared row-order contract.
pub(crate) fn compare_normalized_results(
    subject: &NormalizedResult,
    reference: &NormalizedResult,
) -> Result<(), NormalizationMismatch> {
    if subject.columns != reference.columns {
        return Err(NormalizationMismatch::new(
            NormalizationMismatchKind::ColumnShape,
            None,
            None,
        ));
    }
    if subject.row_order != reference.row_order {
        return Err(NormalizationMismatch::new(
            NormalizationMismatchKind::OrderingContract,
            None,
            None,
        ));
    }

    let subject_rows = subject.comparison_rows();
    let reference_rows = reference.comparison_rows();
    if subject_rows.len() != reference_rows.len() {
        return Err(NormalizationMismatch::new(
            NormalizationMismatchKind::RowCount,
            None,
            None,
        ));
    }

    for (row_index, (subject_row, reference_row)) in
        subject_rows.iter().zip(&reference_rows).enumerate()
    {
        if subject_row.len() != reference_row.len() {
            return Err(NormalizationMismatch::new(
                NormalizationMismatchKind::RowShape,
                Some(row_index),
                None,
            ));
        }
        for (column_index, (subject_cell, reference_cell)) in
            subject_row.iter().zip(reference_row).enumerate()
        {
            if subject_cell != reference_cell {
                return Err(NormalizationMismatch::new(
                    NormalizationMismatchKind::Value,
                    Some(row_index),
                    Some(column_index),
                ));
            }
        }
    }

    Ok(())
}
