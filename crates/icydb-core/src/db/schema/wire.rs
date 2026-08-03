//! Module: db::schema::wire
//! Responsibility: bounded primitive encoding for current schema-owned binary envelopes.
//! Does not own: any persisted format, semantic validation, or version policy.
//! Boundary: schema codecs -> checked big-endian bytes.

use crate::error::InternalError;

///
/// SchemaWireWriter
///
/// Shared bounded writer for schema-owned current-form binary envelopes. Each
/// format supplies its own compile-time byte ceiling and remains responsible
/// for its magic, version, ordering, and semantic validation.
///

pub(in crate::db::schema) struct SchemaWireWriter<const MAX_BYTES: usize> {
    bytes: Vec<u8>,
    overflowed: bool,
}

impl<const MAX_BYTES: usize> SchemaWireWriter<MAX_BYTES> {
    pub(in crate::db::schema) const fn new() -> Self {
        Self {
            bytes: Vec::new(),
            overflowed: false,
        }
    }

    pub(in crate::db::schema) fn push_u8(&mut self, value: u8) {
        self.push_bytes(&[value]);
    }

    pub(in crate::db::schema) fn push_u16(&mut self, value: u16) {
        self.push_bytes(&value.to_be_bytes());
    }

    pub(in crate::db::schema) fn push_u32(&mut self, value: u32) {
        self.push_bytes(&value.to_be_bytes());
    }

    pub(in crate::db::schema) fn push_u64(&mut self, value: u64) {
        self.push_bytes(&value.to_be_bytes());
    }

    pub(in crate::db::schema) fn push_bool(&mut self, value: bool) {
        self.push_u8(u8::from(value));
    }

    pub(in crate::db::schema) fn push_len(&mut self, value: usize) -> Result<(), InternalError> {
        self.push_u32(u32::try_from(value).map_err(|_| InternalError::store_unsupported())?);
        Ok(())
    }

    pub(in crate::db::schema) fn push_string(&mut self, value: &str) -> Result<(), InternalError> {
        self.push_len_prefixed_bytes(value.as_bytes())
    }

    pub(in crate::db::schema) fn push_bounded_string(
        &mut self,
        value: &str,
        max_bytes: usize,
    ) -> Result<(), InternalError> {
        self.push_bounded_len_prefixed_bytes(value.as_bytes(), max_bytes)
    }

    pub(in crate::db::schema) fn push_len_prefixed_bytes(
        &mut self,
        value: &[u8],
    ) -> Result<(), InternalError> {
        self.push_len(value.len())?;
        self.push_bytes(value);
        Ok(())
    }

    pub(in crate::db::schema) fn push_bounded_len_prefixed_bytes(
        &mut self,
        value: &[u8],
        max_bytes: usize,
    ) -> Result<(), InternalError> {
        if value.len() > max_bytes {
            return Err(InternalError::store_unsupported());
        }
        self.push_len_prefixed_bytes(value)
    }

    pub(in crate::db::schema) fn push_optional_u32(&mut self, value: Option<u32>) {
        match value {
            Some(value) => {
                self.push_u8(1);
                self.push_u32(value);
            }
            None => self.push_u8(0),
        }
    }

    pub(in crate::db::schema) fn push_bytes(&mut self, value: &[u8]) {
        if value.len() > MAX_BYTES.saturating_sub(self.bytes.len()) {
            self.overflowed = true;
            return;
        }
        self.bytes.extend_from_slice(value);
    }

    pub(in crate::db::schema) fn finish(self) -> Result<Vec<u8>, InternalError> {
        if self.overflowed {
            return Err(InternalError::store_unsupported());
        }
        Ok(self.bytes)
    }
}

///
/// SchemaWireReader
///
/// Shared fallible reader for schema-owned binary envelopes. Format decoders
/// must enforce their byte ceiling before construction and call `finish` after
/// consuming the current form.
///

pub(in crate::db::schema) struct SchemaWireReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> SchemaWireReader<'a> {
    pub(in crate::db::schema) const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    const fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    pub(in crate::db::schema) fn read_u8(&mut self) -> Result<u8, InternalError> {
        Ok(self.read_array::<1>()?[0])
    }

    pub(in crate::db::schema) fn read_u16(&mut self) -> Result<u16, InternalError> {
        Ok(u16::from_be_bytes(self.read_array()?))
    }

    pub(in crate::db::schema) fn read_u32(&mut self) -> Result<u32, InternalError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    pub(in crate::db::schema) fn read_u64(&mut self) -> Result<u64, InternalError> {
        Ok(u64::from_be_bytes(self.read_array()?))
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
        let bytes = self.read_len_prefixed_bytes()?;
        let value = std::str::from_utf8(bytes).map_err(|_| InternalError::store_corruption())?;
        Ok(value.to_string())
    }

    pub(in crate::db::schema) fn read_bounded_string(
        &mut self,
        max_bytes: usize,
    ) -> Result<String, InternalError> {
        let bytes = self.read_bounded_len_prefixed_bytes(max_bytes)?;
        let value = std::str::from_utf8(bytes).map_err(|_| InternalError::store_corruption())?;
        Ok(value.to_string())
    }

    pub(in crate::db::schema) fn read_len_prefixed_bytes(
        &mut self,
    ) -> Result<&'a [u8], InternalError> {
        let len = self.read_u32()? as usize;
        self.read_slice(len)
    }

    pub(in crate::db::schema) fn read_bounded_len_prefixed_bytes(
        &mut self,
        max_bytes: usize,
    ) -> Result<&'a [u8], InternalError> {
        let len = self.read_u32()? as usize;
        if len > max_bytes {
            return Err(InternalError::store_corruption());
        }
        self.read_slice(len)
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
        let bytes = self.read_slice(N)?;
        let mut value = [0_u8; N];
        value.copy_from_slice(bytes);
        Ok(value)
    }

    fn read_slice(&mut self, len: usize) -> Result<&'a [u8], InternalError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or_else(InternalError::store_corruption)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or_else(InternalError::store_corruption)?;
        self.offset = end;
        Ok(value)
    }

    pub(in crate::db::schema) fn finish(self) -> Result<(), InternalError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(InternalError::store_corruption())
        }
    }
}
