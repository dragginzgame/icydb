//! Module: db::codec::reader
//! Responsibility: checked primitive reads from borrowed binary payloads.
//! Does not own: format tags, envelope limits, or domain error classification.
//! Boundary: format decoders -> checked byte extraction with explicit endianness.

/// A truncated, oversized, malformed, or incompletely consumed primitive payload.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(in crate::db) struct ByteDecodeError;

/// Borrowed reader shared by format decoders, which retain their own byte ceilings
/// and map primitive failures to their domain errors.
pub(in crate::db) struct ByteReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> ByteReader<'a> {
    pub(in crate::db) const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    pub(in crate::db) const fn remaining(&self) -> usize {
        self.bytes.len().saturating_sub(self.offset)
    }

    // Advance only after both offset arithmetic and the byte span have checked.
    pub(in crate::db) fn read_exact(&mut self, len: usize) -> Result<&'a [u8], ByteDecodeError> {
        let end = self.offset.checked_add(len).ok_or(ByteDecodeError)?;
        let value = self.bytes.get(self.offset..end).ok_or(ByteDecodeError)?;
        self.offset = end;
        Ok(value)
    }

    pub(in crate::db) fn read_array<const N: usize>(&mut self) -> Result<[u8; N], ByteDecodeError> {
        self.read_exact(N)?.try_into().map_err(|_| ByteDecodeError)
    }

    pub(in crate::db) fn read_u8(&mut self) -> Result<u8, ByteDecodeError> {
        Ok(self.read_array::<1>()?[0])
    }

    pub(in crate::db) fn read_u16(&mut self) -> Result<u16, ByteDecodeError> {
        Ok(u16::from_be_bytes(self.read_array()?))
    }

    pub(in crate::db) fn read_u32(&mut self) -> Result<u32, ByteDecodeError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    pub(in crate::db) fn read_u64(&mut self) -> Result<u64, ByteDecodeError> {
        Ok(u64::from_be_bytes(self.read_array()?))
    }

    pub(in crate::db) fn read_i64(&mut self) -> Result<i64, ByteDecodeError> {
        Ok(i64::from_be_bytes(self.read_array()?))
    }

    pub(in crate::db) fn read_i128(&mut self) -> Result<i128, ByteDecodeError> {
        Ok(i128::from_be_bytes(self.read_array()?))
    }

    pub(in crate::db) fn read_u128(&mut self) -> Result<u128, ByteDecodeError> {
        Ok(u128::from_be_bytes(self.read_array()?))
    }

    pub(in crate::db) fn read_u16_le(&mut self) -> Result<u16, ByteDecodeError> {
        Ok(u16::from_le_bytes(self.read_array()?))
    }

    pub(in crate::db) fn read_u32_le(&mut self) -> Result<u32, ByteDecodeError> {
        Ok(u32::from_le_bytes(self.read_array()?))
    }

    pub(in crate::db) fn read_u64_le(&mut self) -> Result<u64, ByteDecodeError> {
        Ok(u64::from_le_bytes(self.read_array()?))
    }

    pub(in crate::db) fn read_u128_le(&mut self) -> Result<u128, ByteDecodeError> {
        Ok(u128::from_le_bytes(self.read_array()?))
    }

    pub(in crate::db) fn read_len_prefixed_bytes_le(
        &mut self,
    ) -> Result<&'a [u8], ByteDecodeError> {
        let len = usize::try_from(self.read_u32_le()?).map_err(|_| ByteDecodeError)?;
        self.read_exact(len)
    }

    pub(in crate::db) fn read_len_prefixed_bytes(&mut self) -> Result<&'a [u8], ByteDecodeError> {
        self.read_bounded_len_prefixed_bytes(usize::MAX)
    }

    pub(in crate::db) fn read_bounded_len_prefixed_bytes(
        &mut self,
        max: usize,
    ) -> Result<&'a [u8], ByteDecodeError> {
        let len = usize::try_from(self.read_u32()?).map_err(|_| ByteDecodeError)?;
        if len > max {
            return Err(ByteDecodeError);
        }
        self.read_exact(len)
    }

    pub(in crate::db) fn read_string(&mut self) -> Result<String, ByteDecodeError> {
        self.read_bounded_string(usize::MAX)
    }

    pub(in crate::db) fn read_bounded_string(
        &mut self,
        max: usize,
    ) -> Result<String, ByteDecodeError> {
        let bytes = self.read_bounded_len_prefixed_bytes(max)?;
        std::str::from_utf8(bytes)
            .map(str::to_string)
            .map_err(|_| ByteDecodeError)
    }

    pub(in crate::db) const fn finish(self) -> Result<(), ByteDecodeError> {
        if self.remaining() == 0 {
            Ok(())
        } else {
            Err(ByteDecodeError)
        }
    }
}

///
/// TESTS
///

#[cfg(test)]
mod tests;
