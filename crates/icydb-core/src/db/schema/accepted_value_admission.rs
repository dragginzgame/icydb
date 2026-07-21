//! Module: db::schema::accepted_value_admission
//! Responsibility: carry complete accepted authority for one value admission boundary.
//! Does not own: row codecs, field lookup, recursive value semantics, or schema publication.
//! Boundary: accepted catalog/value/nullability facts -> admitted canonical value proof.

use crate::{
    db::schema::{
        AcceptedFieldKind, AcceptedValueCatalogHandle,
        enum_catalog::{
            AcceptedValueContract, AcceptedValueRef, AdmittedOwnedValue, CanonicalValue,
            ValueAdmissionBudget, ValueAdmissionError, admit_canonical_value,
            normalize_and_admit_nullable_value, validate_nullable_canonical_value,
            with_normalized_accepted_value,
        },
    },
    value::{InputValue, Value},
};
use std::borrow::Cow;

/// Complete accepted authority for normalizing or validating one value.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(in crate::db) struct AcceptedValueAdmissionContract<'a> {
    catalogs: &'a AcceptedValueCatalogHandle,
    value_contract: Cow<'a, AcceptedValueContract>,
    nullable: bool,
}

impl<'a> AcceptedValueAdmissionContract<'a> {
    /// Borrow a recursive value contract already owned by accepted schema metadata.
    pub(in crate::db::schema) const fn borrowed(
        catalogs: &'a AcceptedValueCatalogHandle,
        value_contract: &'a AcceptedValueContract,
        nullable: bool,
    ) -> Self {
        Self {
            catalogs,
            value_contract: Cow::Borrowed(value_contract),
            nullable,
        }
    }

    /// Retain a recursive value contract derived for a runtime persistence boundary.
    pub(in crate::db::schema) const fn owned(
        catalogs: &'a AcceptedValueCatalogHandle,
        value_contract: AcceptedValueContract,
        nullable: bool,
    ) -> Self {
        Self {
            catalogs,
            value_contract: Cow::Owned(value_contract),
            nullable,
        }
    }

    /// Borrow the immutable catalog authority for this value contract.
    #[cfg(test)]
    #[must_use]
    pub(in crate::db) const fn catalogs(&self) -> &'a AcceptedValueCatalogHandle {
        self.catalogs
    }

    /// Borrow the recursively validated accepted value contract.
    #[must_use]
    pub(in crate::db) fn value_contract(&self) -> &AcceptedValueContract {
        self.value_contract.as_ref()
    }

    /// Borrow the accepted top-level value kind.
    #[must_use]
    pub(in crate::db) fn kind(&self) -> &AcceptedFieldKind {
        self.value_contract().kind()
    }

    /// Derive a non-null collection-element admission contract under the same catalog.
    #[must_use]
    pub(in crate::db) fn collection_element_contract(&self) -> Option<Self> {
        Some(Self::owned(
            self.catalogs,
            self.value_contract().collection_element_contract()?,
            false,
        ))
    }

    /// Normalize authored input into an owned value pinned to this accepted authority.
    pub(in crate::db) fn normalize_and_admit(
        &self,
        input: InputValue,
        budget: &mut ValueAdmissionBudget,
    ) -> Result<AdmittedOwnedValue, ValueAdmissionError> {
        normalize_and_admit_nullable_value(
            self.catalogs,
            self.value_contract(),
            self.nullable,
            input,
            budget,
        )
    }

    /// Admit an owned canonical value after strict validation against this authority.
    pub(in crate::db) fn admit_canonical(
        &self,
        value: CanonicalValue,
        budget: &mut ValueAdmissionBudget,
    ) -> Result<AdmittedOwnedValue, ValueAdmissionError> {
        admit_canonical_value(
            self.catalogs,
            self.value_contract(),
            self.nullable,
            value,
            budget,
        )
    }

    /// Normalize authored input into the runtime value domain.
    pub(in crate::db) fn normalize_input_to_runtime(
        &self,
        input: InputValue,
        budget: &mut ValueAdmissionBudget,
    ) -> Result<Value, ValueAdmissionError> {
        self.normalize_and_admit(input, budget)
            .map(|admitted| admitted.value().clone())
    }

    /// Normalize authored input and expose its short-lived accepted proof.
    pub(in crate::db) fn with_normalized<R>(
        &self,
        input: InputValue,
        budget: &mut ValueAdmissionBudget,
        use_value: impl for<'value> FnOnce(AcceptedValueRef<'value>) -> R,
    ) -> Result<R, ValueAdmissionError> {
        with_normalized_accepted_value(
            self.catalogs,
            self.value_contract(),
            self.nullable,
            input,
            budget,
            use_value,
        )
    }

    /// Strictly validate a canonical value and expose its accepted proof.
    pub(in crate::db) fn with_validated<R>(
        &self,
        value: &CanonicalValue,
        budget: &mut ValueAdmissionBudget,
        use_value: impl for<'value> FnOnce(AcceptedValueRef<'value>) -> R,
    ) -> Result<R, ValueAdmissionError> {
        let accepted = validate_nullable_canonical_value(
            self.catalogs,
            self.value_contract(),
            self.nullable,
            value,
            budget,
        )?;
        Ok(use_value(accepted))
    }
}
