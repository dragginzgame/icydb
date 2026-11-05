//! Test-only constructors and adapters for the parent module.

use super::*;

impl<K: Copy + From<[u8; 32]> + Into<[u8; 32]>> SchemaControlMap<K> {
    pub(in crate::db::schema) fn corrupt_header_for_tests(&mut self) -> Result<(), InternalError> {
        self.map
            .insert(HEADER_KEY, SchemaControlBytes(vec![0xff]))
            .map_err(InternalError::from)?;

        Ok(())
    }
}

pub(in crate::db) fn corrupt_schema_control_header_for_tests() -> Result<(), InternalError> {
    let mut map =
        SchemaControlMap::<[u8; 32]>::open(control_memory()?, ControlRecordFamily::Checkpoint)?;
    map.corrupt_header_for_tests()
        .expect("fixture storage operation should succeed");
    Ok(())
}
