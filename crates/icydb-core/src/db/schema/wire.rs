//! Module: db::schema::wire
//! Responsibility: checked primitive reading for current schema-owned binary envelopes.
//! Does not own: any persisted format, semantic validation, or version policy.
//! Boundary: schema codecs -> checked big-endian reads.

use crate::{db::codec::ByteReader, error::InternalError};

///
/// SchemaWireReader
///
/// Shared fallible reader for schema-owned binary envelopes. Format decoders
/// must enforce their byte ceiling before construction and call `finish` after
/// consuming the current form.
///

pub(in crate::db::schema) struct SchemaWireReader<'a> {
    reader: ByteReader<'a>,
}

impl<'a> SchemaWireReader<'a> {
    pub(in crate::db::schema) const fn new(bytes: &'a [u8]) -> Self {
        Self {
            reader: ByteReader::new(bytes),
        }
    }

    const fn remaining(&self) -> usize {
        self.reader.remaining()
    }

    pub(in crate::db::schema) fn read_u8(&mut self) -> Result<u8, InternalError> {
        self.reader
            .read_u8()
            .map_err(|_| InternalError::store_corruption())
    }

    pub(in crate::db::schema) fn read_u16(&mut self) -> Result<u16, InternalError> {
        self.reader
            .read_u16()
            .map_err(|_| InternalError::store_corruption())
    }

    pub(in crate::db::schema) fn read_u32(&mut self) -> Result<u32, InternalError> {
        self.reader
            .read_u32()
            .map_err(|_| InternalError::store_corruption())
    }

    pub(in crate::db::schema) fn read_u64(&mut self) -> Result<u64, InternalError> {
        self.reader
            .read_u64()
            .map_err(|_| InternalError::store_corruption())
    }

    pub(in crate::db::schema) fn read_bool(&mut self) -> Result<bool, InternalError> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(InternalError::store_corruption()),
        }
    }

    pub(in crate::db::schema) fn read_count(&mut self) -> Result<usize, InternalError> {
        let count = self.read_u32()? as usize;
        if count > self.remaining() {
            return Err(InternalError::store_corruption());
        }
        Ok(count)
    }

    pub(in crate::db::schema) fn read_bounded_count(
        &mut self,
        max: usize,
    ) -> Result<usize, InternalError> {
        let count = self.read_count()?;
        if count > max {
            return Err(InternalError::store_corruption());
        }
        Ok(count)
    }

    pub(in crate::db::schema) fn read_string(&mut self) -> Result<String, InternalError> {
        self.reader
            .read_string()
            .map_err(|_| InternalError::store_corruption())
    }

    pub(in crate::db::schema) fn read_bounded_string(
        &mut self,
        max_bytes: usize,
    ) -> Result<String, InternalError> {
        self.reader
            .read_bounded_string(max_bytes)
            .map_err(|_| InternalError::store_corruption())
    }

    pub(in crate::db::schema) fn read_len_prefixed_bytes(
        &mut self,
    ) -> Result<&'a [u8], InternalError> {
        self.reader
            .read_len_prefixed_bytes()
            .map_err(|_| InternalError::store_corruption())
    }

    pub(in crate::db::schema) fn read_bounded_len_prefixed_bytes(
        &mut self,
        max_bytes: usize,
    ) -> Result<&'a [u8], InternalError> {
        self.reader
            .read_bounded_len_prefixed_bytes(max_bytes)
            .map_err(|_| InternalError::store_corruption())
    }

    pub(in crate::db::schema) fn read_optional_u32(
        &mut self,
    ) -> Result<Option<u32>, InternalError> {
        match self.read_u8()? {
            0 => Ok(None),
            1 => self.read_u32().map(Some),
            _ => Err(InternalError::store_corruption()),
        }
    }

    pub(in crate::db::schema) fn read_array<const N: usize>(
        &mut self,
    ) -> Result<[u8; N], InternalError> {
        self.reader
            .read_array()
            .map_err(|_| InternalError::store_corruption())
    }

    pub(in crate::db::schema) fn finish(self) -> Result<(), InternalError> {
        self.reader
            .finish()
            .map_err(|_| InternalError::store_corruption())
    }
}
