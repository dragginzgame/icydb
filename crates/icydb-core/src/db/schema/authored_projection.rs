//! Module: db::schema::authored_projection
//! Responsibility: bind generated authored field inputs to accepted field and catalog authority.
//! Does not own: runtime value projection, persisted encoding, or mutation execution.
//! Boundary: stable generated field slots -> admitted owned values.
use crate::{
    db::{
        data::encode_input_value_for_accepted_field_contract,
        schema::{
            AcceptedFieldDecodeContract, AcceptedRowDecodeContract,
            enum_catalog::{
                AcceptedEnumCatalogHandle, AdmittedOwnedValue, ValueAdmissionBudget,
                ValueAdmissionError, normalize_and_admit_persisted_field_value,
            },
        },
    },
    traits::AuthoredFieldProjection,
    value::InputValue,
};

/// Failure to bind or admit one generated authored field slot.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) enum AuthoredFieldAdmissionError {
    MissingCatalogAuthority,
    MissingFieldContract { slot: usize },
    FieldNotGenerated { slot: usize },
    MissingAuthoredValue { slot: usize },
    PersistenceEncoding { slot: usize },
    Admission(ValueAdmissionError),
}

/// Accepted-schema binding for one generated entity's authored field projection.
pub(in crate::db) struct AcceptedAuthoredFieldProjection<'a> {
    row_contract: &'a AcceptedRowDecodeContract,
    catalog: &'a AcceptedEnumCatalogHandle,
}

impl<'a> AcceptedAuthoredFieldProjection<'a> {
    pub(in crate::db) fn new(
        row_contract: &'a AcceptedRowDecodeContract,
    ) -> Result<Self, AuthoredFieldAdmissionError> {
        let catalog = row_contract
            .enum_catalog_handle()
            .ok_or(AuthoredFieldAdmissionError::MissingCatalogAuthority)?;
        Ok(Self {
            row_contract,
            catalog,
        })
    }

    pub(in crate::db) fn admit_field<E>(
        &self,
        entity: &E,
        slot: usize,
        budget: &mut ValueAdmissionBudget,
    ) -> Result<AdmittedOwnedValue, AuthoredFieldAdmissionError>
    where
        E: AuthoredFieldProjection,
    {
        let (decode, input) = self.authored_field_input(entity, slot)?;

        normalize_and_admit_persisted_field_value(
            self.catalog,
            decode.kind(),
            decode.storage_decode(),
            decode.nullable(),
            input,
            budget,
        )
        .map_err(AuthoredFieldAdmissionError::Admission)
    }

    pub(in crate::db) fn encode_field<E>(
        &self,
        entity: &E,
        slot: usize,
        budget: &mut ValueAdmissionBudget,
    ) -> Result<Vec<u8>, AuthoredFieldAdmissionError>
    where
        E: AuthoredFieldProjection,
    {
        let (field, input) = self.authored_field_input(entity, slot)?;

        encode_input_value_for_accepted_field_contract(self.catalog, field, input, budget)
            .map_err(|_| AuthoredFieldAdmissionError::PersistenceEncoding { slot })
    }

    fn authored_field_input<E>(
        &self,
        entity: &E,
        slot: usize,
    ) -> Result<(AcceptedFieldDecodeContract<'_>, InputValue), AuthoredFieldAdmissionError>
    where
        E: AuthoredFieldProjection,
    {
        let field = self
            .row_contract
            .field_for_slot(slot)
            .ok_or(AuthoredFieldAdmissionError::MissingFieldContract { slot })?;
        if !field.generated() {
            return Err(AuthoredFieldAdmissionError::FieldNotGenerated { slot });
        }
        let input = entity
            .get_input_value_by_index(slot)
            .ok_or(AuthoredFieldAdmissionError::MissingAuthoredValue { slot })?;

        Ok((field.decode_contract(), input))
    }
}
