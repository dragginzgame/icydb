//! Move admitted reader state with its immutable row between executor phases.
//! Saved borrows are offsets; no generated schema or validation fallback exists.

use crate::{
    db::{
        data::{
            DataRow, DecodedDataStoreKey, RawRow, ScalarSlotValueRef, ScalarValueRef,
            StructuralRowContract, StructuralRowFieldBytes,
            persisted_row::reader::{
                cache::CachedSlotValue, structural_slot_reader::StructuralSlotReader,
            },
            structural_row::{StructuralRowFieldOffsets, admitted_subslice_range},
        },
        resource::budget::{charge_current_execution_budget, runtime_value_work},
    },
    error::InternalError,
    value::Value,
};
use icydb_diagnostic_code::DiagnosticExecutionBudgetResource as Resource;
use std::{borrow::Cow, cell::OnceCell, ops::Range};

/// A canonical row and its optional, inseparable accepted decoding state.
/// The state never enters stored bytes or marks a partially read row as full.
pub(in crate::db) struct ReadDataRow {
    row: DataRow,
    state: Option<Box<ReadState>>,
}

struct ReadState {
    contract: StructuralRowContract,
    offsets: StructuralRowFieldOffsets,
    slots: Vec<SavedSlot>,
}

struct SavedSlot {
    slot: usize,
    scalar: Option<SavedScalar>,
    materialized: Option<Value>,
}

enum SavedScalar {
    Copy(ScalarSlotValueRef<'static>),
    Text(Range<usize>),
    Blob(Range<usize>),
}

impl ReadDataRow {
    /// Wrap bytes that have not acquired a reader yet, without allocating.
    pub(in crate::db) const fn raw(row: DataRow) -> Self {
        Self { row, state: None }
    }

    /// Keep the exact reader state only when the filter accepts this owned row.
    pub(in crate::db) fn filter(
        row: DataRow,
        contract: &StructuralRowContract,
        filter: impl FnOnce(&StructuralSlotReader<'_>) -> Result<bool, InternalError>,
    ) -> Result<Option<Self>, InternalError> {
        let reader = StructuralSlotReader::from_raw_row_with_borrowed_contract(&row.1, contract)?;
        if !filter(&reader)? {
            return Ok(None);
        }
        let state = ReadState::save(reader)?;
        Ok(Some(Self {
            row,
            state: Some(state),
        }))
    }

    /// Iterator bytes are copied only after acceptance; saved offsets refer to
    /// that exact byte-for-byte copy, created inside this owning boundary.
    pub(in crate::db) fn filter_borrowed(
        key: DecodedDataStoreKey,
        row: &RawRow,
        contract: &StructuralRowContract,
        filter: impl FnOnce(&StructuralSlotReader<'_>) -> Result<bool, InternalError>,
    ) -> Result<Option<Self>, InternalError> {
        let reader = StructuralSlotReader::from_raw_row_with_borrowed_contract(row, contract)?;
        if !filter(&reader)? {
            return Ok(None);
        }
        let state = ReadState::save(reader)?;
        Ok(Some(Self {
            row: (key, row.clone()),
            state: Some(state),
        }))
    }

    /// Borrow canonical bytes for cursor/key consumers without exposing state.
    pub(in crate::db) const fn data_row(&self) -> &DataRow {
        &self.row
    }

    /// Account owned row/cache backing when a window retains this row.
    pub(in crate::db) fn retained_backing_bytes(&self) -> u64 {
        let state = self.state.as_ref().map_or(0, |state| {
            (size_of::<ReadState>() as u64)
                .saturating_add(
                    (state.slots.capacity() as u64).saturating_mul(size_of::<SavedSlot>() as u64),
                )
                .saturating_add(
                    state
                        .slots
                        .iter()
                        .filter_map(|slot| slot.materialized.as_ref())
                        .fold(0_u64, |total, value| {
                            total.saturating_add(runtime_value_work(value).0)
                        }),
                )
        });
        (self.row.1.len() as u64).saturating_add(state)
    }

    /// Explicitly discard decoding state when another row family owns output.
    pub(in crate::db) fn into_data_row(self) -> DataRow {
        self.row
    }

    /// Reborrow the same immutable row, retaining envelope and touched slots.
    /// A different accepted contract is an invariant refusal, never a rebind.
    pub(in crate::db) fn with_reader<R>(
        self,
        expected: &StructuralRowContract,
        evaluate: impl FnOnce(
            &DecodedDataStoreKey,
            StructuralSlotReader<'_>,
        ) -> Result<R, InternalError>,
    ) -> Result<R, InternalError> {
        let (key, raw) = self.row;
        let reader = match self.state {
            None => StructuralSlotReader::from_raw_row_with_borrowed_contract(&raw, expected)?,
            Some(state) => (*state).restore(&raw, expected)?,
        };
        evaluate(&key, reader)
    }
}

impl ReadState {
    fn save(reader: StructuralSlotReader<'_>) -> Result<Box<Self>, InternalError> {
        let offsets = reader.field_bytes.offsets()?;
        let bytes = reader.field_bytes.row_bytes();
        let cached = reader.cached_values.into_inner().unwrap_or_default();
        let count = cached
            .iter()
            .filter(|slot| slot.validated.get().is_some() || slot.materialized.get().is_some())
            .count();
        charge_backing::<SavedSlot>(count)?;
        let mut slots = Vec::with_capacity(count);
        for (slot, cached) in cached.into_iter().enumerate() {
            let scalar = cached
                .validated
                .into_inner()
                .map(|value| SavedScalar::save(value, bytes))
                .transpose()?;
            let materialized = cached.materialized.into_inner();
            if scalar.is_some() || materialized.is_some() {
                slots.push(SavedSlot {
                    slot,
                    scalar,
                    materialized,
                });
            }
        }
        charge_backing::<Self>(1)?;
        Ok(Box::new(Self {
            contract: reader.contract.into_owned(),
            offsets,
            slots,
        }))
    }

    fn restore<'row>(
        self,
        row: &'row RawRow,
        expected: &StructuralRowContract,
    ) -> Result<StructuralSlotReader<'row>, InternalError> {
        let Self {
            contract,
            offsets,
            slots,
        } = self;
        if !contract.has_same_decode_authority(expected) {
            return Err(InternalError::query_executor_invariant());
        }
        let field_bytes = StructuralRowFieldBytes::from_offsets(row.as_bytes(), offsets)?;
        let cached_values = OnceCell::new();
        if !slots.is_empty() {
            charge_backing::<CachedSlotValue<'_>>(contract.field_count())?;
            let mut cached: Vec<_> = std::iter::repeat_with(CachedSlotValue::default)
                .take(contract.field_count())
                .collect();
            for saved in slots {
                let target = cached
                    .get_mut(saved.slot)
                    .ok_or_else(InternalError::query_executor_invariant)?;
                if let Some(scalar) = saved.scalar {
                    let _ = target.validated.set(scalar.restore(row.as_bytes())?);
                }
                if let Some(value) = saved.materialized {
                    let _ = target.materialized.set(value);
                }
            }
            let _ = cached_values.set(cached);
        }
        Ok(StructuralSlotReader {
            contract: Cow::Owned(contract),
            field_bytes,
            cached_values,
        })
    }
}

impl SavedScalar {
    fn save(value: ScalarSlotValueRef<'_>, bytes: &[u8]) -> Result<Self, InternalError> {
        let value = match value {
            ScalarSlotValueRef::Null => return Ok(Self::Copy(ScalarSlotValueRef::Null)),
            ScalarSlotValueRef::Value(value) => value,
        };
        // Enumerating copy variants keeps this conversion exhaustive when the
        // scalar owner adds a family. Text/blob payloads remain borrowed bytes.
        let copy = match value {
            ScalarValueRef::Text(value) => {
                return admitted_subslice_range(bytes, value.as_bytes()).map(Self::Text);
            }
            ScalarValueRef::Blob(value) => {
                return admitted_subslice_range(bytes, value).map(Self::Blob);
            }
            ScalarValueRef::Bool(v) => ScalarValueRef::Bool(v),
            ScalarValueRef::Date(v) => ScalarValueRef::Date(v),
            ScalarValueRef::Duration(v) => ScalarValueRef::Duration(v),
            ScalarValueRef::Float32(v) => ScalarValueRef::Float32(v),
            ScalarValueRef::Float64(v) => ScalarValueRef::Float64(v),
            ScalarValueRef::Int(v) => ScalarValueRef::Int(v),
            ScalarValueRef::Principal(v) => ScalarValueRef::Principal(v),
            ScalarValueRef::Subaccount(v) => ScalarValueRef::Subaccount(v),
            ScalarValueRef::Timestamp(v) => ScalarValueRef::Timestamp(v),
            ScalarValueRef::Nat(v) => ScalarValueRef::Nat(v),
            ScalarValueRef::Ulid(v) => ScalarValueRef::Ulid(v),
            ScalarValueRef::Unit => ScalarValueRef::Unit,
            ScalarValueRef::U256(v) => ScalarValueRef::U256(v),
        };
        Ok(Self::Copy(ScalarSlotValueRef::Value(copy)))
    }

    fn restore(self, bytes: &[u8]) -> Result<ScalarSlotValueRef<'_>, InternalError> {
        match self {
            Self::Copy(value) => Ok(value),
            Self::Text(range) => {
                let value = bytes
                    .get(range)
                    .and_then(|bytes| std::str::from_utf8(bytes).ok())
                    .ok_or_else(InternalError::query_executor_invariant)?;
                Ok(ScalarSlotValueRef::Value(ScalarValueRef::Text(value)))
            }
            Self::Blob(range) => {
                let value = bytes
                    .get(range)
                    .ok_or_else(InternalError::query_executor_invariant)?;
                Ok(ScalarSlotValueRef::Value(ScalarValueRef::Blob(value)))
            }
        }
    }
}

fn charge_backing<T>(count: usize) -> Result<(), InternalError> {
    charge_current_execution_budget(
        Resource::TemporaryBytes,
        (count as u64).saturating_mul(size_of::<T>() as u64),
    )
}
