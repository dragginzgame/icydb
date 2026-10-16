//! Checked primitive writes; callers retain format tags, limits and domain errors.

#[cfg(test)]
mod tests;

use crate::error::InternalError;

/// A length that cannot be represented by a u32 wire prefix.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ByteEncodeError;

pub(in crate::db) fn checked_len_u32(len: usize) -> Result<u32, ByteEncodeError> {
    u32::try_from(len).map_err(|_| ByteEncodeError)
}

pub(in crate::db) fn write_u16_be(out: &mut Vec<u8>, value: u16) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub(in crate::db) fn write_u32_be(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub(in crate::db) fn write_u64_be(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub(in crate::db) fn write_i64_be(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub(in crate::db) fn write_i128_be(out: &mut Vec<u8>, value: i128) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub(in crate::db) fn write_u128_be(out: &mut Vec<u8>, value: u128) {
    out.extend_from_slice(&value.to_be_bytes());
}

pub(in crate::db) fn write_len_prefixed_bytes_be(
    out: &mut Vec<u8>,
    bytes: &[u8],
) -> Result<(), ByteEncodeError> {
    write_u32_be(out, checked_len_u32(bytes.len())?);
    out.extend_from_slice(bytes);
    Ok(())
}

pub(in crate::db) fn write_string_be(
    out: &mut Vec<u8>,
    value: &str,
) -> Result<(), ByteEncodeError> {
    write_len_prefixed_bytes_be(out, value.as_bytes())
}

pub(in crate::db) fn write_len_u32_le(
    out: &mut Vec<u8>,
    len: usize,
) -> Result<(), ByteEncodeError> {
    out.extend_from_slice(&checked_len_u32(len)?.to_le_bytes());
    Ok(())
}

pub(in crate::db) fn write_len_prefixed_bytes_le(
    out: &mut Vec<u8>,
    bytes: &[u8],
) -> Result<(), ByteEncodeError> {
    write_len_u32_le(out, bytes.len())?;
    out.extend_from_slice(bytes);
    Ok(())
}

///
/// BoundedByteWriter
///
/// Shared bounded writer for database-owned binary envelopes. Each
/// format supplies its own compile-time byte ceiling and remains responsible
/// for its magic, version, ordering, and semantic validation.
///

pub(in crate::db) struct BoundedByteWriter<const MAX_BYTES: usize> {
    bytes: Vec<u8>,
    overflowed: bool,
}

impl<const MAX_BYTES: usize> BoundedByteWriter<MAX_BYTES> {
    pub(in crate::db) const fn new() -> Self {
        Self {
            bytes: Vec::new(),
            overflowed: false,
        }
    }

    pub(in crate::db) fn push_u8(&mut self, value: u8) {
        self.push_bytes(&[value]);
    }

    pub(in crate::db) fn push_u16(&mut self, value: u16) {
        self.append(size_of::<u16>(), |out| write_u16_be(out, value));
    }

    pub(in crate::db) fn push_u32(&mut self, value: u32) {
        self.append(size_of::<u32>(), |out| write_u32_be(out, value));
    }

    pub(in crate::db) fn push_u64(&mut self, value: u64) {
        self.append(size_of::<u64>(), |out| write_u64_be(out, value));
    }

    pub(in crate::db) fn push_bool(&mut self, value: bool) {
        self.push_u8(u8::from(value));
    }

    pub(in crate::db) fn push_len(&mut self, value: usize) -> Result<(), InternalError> {
        self.push_u32(checked_len_u32(value).map_err(|_| InternalError::store_unsupported())?);
        Ok(())
    }

    pub(in crate::db) fn push_string(&mut self, value: &str) -> Result<(), InternalError> {
        self.push_len_prefixed_bytes(value.as_bytes())
    }

    pub(in crate::db) fn push_bounded_string(
        &mut self,
        value: &str,
        max_bytes: usize,
    ) -> Result<(), InternalError> {
        self.push_bounded_len_prefixed_bytes(value.as_bytes(), max_bytes)
    }

    pub(in crate::db) fn push_len_prefixed_bytes(
        &mut self,
        value: &[u8],
    ) -> Result<(), InternalError> {
        self.push_len(value.len())?;
        self.push_bytes(value);
        Ok(())
    }

    pub(in crate::db) fn push_bounded_len_prefixed_bytes(
        &mut self,
        value: &[u8],
        max_bytes: usize,
    ) -> Result<(), InternalError> {
        if value.len() > max_bytes {
            return Err(InternalError::store_unsupported());
        }
        self.push_len_prefixed_bytes(value)
    }

    pub(in crate::db) fn push_optional_u32(&mut self, value: Option<u32>) {
        match value {
            Some(value) => {
                self.push_u8(1);
                self.push_u32(value);
            }
            None => self.push_u8(0),
        }
    }

    pub(in crate::db) fn push_bytes(&mut self, value: &[u8]) {
        self.append(value.len(), |out| out.extend_from_slice(value));
    }

    // Admit every append before invoking primitive writers. Overflow stays sticky
    // so ignored intermediate writes cannot yield an accepted truncated envelope.
    fn append(&mut self, len: usize, write: impl FnOnce(&mut Vec<u8>)) {
        if len > MAX_BYTES.saturating_sub(self.bytes.len()) {
            self.overflowed = true;
            return;
        }
        write(&mut self.bytes);
    }

    pub(in crate::db) fn finish(self) -> Result<Vec<u8>, InternalError> {
        if self.overflowed {
            return Err(InternalError::store_unsupported());
        }
        Ok(self.bytes)
    }
}
