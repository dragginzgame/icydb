//! Test-only constructors and adapters for the parent module.

use super::*;

impl RowLayout {
    /// Decode one full structural row through the scalar-or-composite row-key
    /// boundary. Composite-key callers use this path so row validation and
    /// primary-key component materialization do not reopen the scalar
    /// scalar-key accessor.
    pub(in crate::db) fn decode_full_value_row_from_data_key_into(
        &self,
        data_key: &DecodedDataStoreKey,
        row: &RawRow,
        values: &mut Vec<Value>,
    ) -> Result<(), InternalError> {
        values.clear();

        let slots = StructuralSlotReader::from_raw_row_with_borrowed_contract(row, &self.contract)?;
        // Identity output needs every value. Retain complete field admission
        // before key validation, then move those cached values into the output.
        slots.materialize_all_values()?;
        slots.validate_primary_key(data_key)?;

        slots.decode_all_values_into(values)
    }
}
