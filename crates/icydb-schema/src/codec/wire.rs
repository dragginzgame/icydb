use crate::SchemaContractError;

pub(super) struct WireWriter {
    bytes: Vec<u8>,
    max: usize,
}

impl WireWriter {
    pub(super) const fn new(max: usize) -> Self {
        Self {
            bytes: Vec::new(),
            max,
        }
    }

    pub(super) fn finish(self) -> Vec<u8> {
        self.bytes
    }

    pub(super) fn push_u8(&mut self, value: u8) -> Result<(), SchemaContractError> {
        self.push_raw(&[value])
    }

    pub(super) fn push_bool(&mut self, value: bool) -> Result<(), SchemaContractError> {
        self.push_u8(u8::from(value))
    }

    pub(super) fn push_u16(&mut self, value: u16) -> Result<(), SchemaContractError> {
        self.push_raw(&value.to_be_bytes())
    }

    pub(super) fn push_u32(&mut self, value: u32) -> Result<(), SchemaContractError> {
        self.push_raw(&value.to_be_bytes())
    }

    pub(super) fn push_i32(&mut self, value: i32) -> Result<(), SchemaContractError> {
        self.push_raw(&value.to_be_bytes())
    }

    pub(super) fn push_u64(&mut self, value: u64) -> Result<(), SchemaContractError> {
        self.push_raw(&value.to_be_bytes())
    }

    pub(super) fn push_i64(&mut self, value: i64) -> Result<(), SchemaContractError> {
        self.push_raw(&value.to_be_bytes())
    }

    pub(super) fn push_u128(&mut self, value: u128) -> Result<(), SchemaContractError> {
        self.push_raw(&value.to_be_bytes())
    }

    pub(super) fn push_i128(&mut self, value: i128) -> Result<(), SchemaContractError> {
        self.push_raw(&value.to_be_bytes())
    }

    pub(super) fn push_len(&mut self, len: usize) -> Result<(), SchemaContractError> {
        let len = u32::try_from(len).map_err(|_| SchemaContractError::Encode)?;
        self.push_u32(len)
    }

    pub(super) fn push_bytes(&mut self, value: &[u8]) -> Result<(), SchemaContractError> {
        self.push_len(value.len())?;
        self.push_raw(value)
    }

    pub(super) fn push_string(&mut self, value: &str) -> Result<(), SchemaContractError> {
        self.push_bytes(value.as_bytes())
    }

    pub(super) fn push_raw(&mut self, value: &[u8]) -> Result<(), SchemaContractError> {
        let len = self
            .bytes
            .len()
            .checked_add(value.len())
            .ok_or(SchemaContractError::Encode)?;
        if len > self.max {
            return Err(SchemaContractError::EncodedTooLarge { len, max: self.max });
        }
        self.bytes.extend_from_slice(value);
        Ok(())
    }
}

pub(super) struct WireReader<'a> {
    bytes: &'a [u8],
    offset: usize,
}

impl<'a> WireReader<'a> {
    pub(super) const fn new(bytes: &'a [u8]) -> Self {
        Self { bytes, offset: 0 }
    }

    pub(super) const fn remaining(&self) -> usize {
        self.bytes.len() - self.offset
    }

    pub(super) const fn finish(self) -> Result<(), SchemaContractError> {
        if self.offset == self.bytes.len() {
            Ok(())
        } else {
            Err(SchemaContractError::Decode)
        }
    }

    pub(super) fn read_u8(&mut self) -> Result<u8, SchemaContractError> {
        Ok(self.take(1)?[0])
    }

    pub(super) fn read_bool(&mut self) -> Result<bool, SchemaContractError> {
        match self.read_u8()? {
            0 => Ok(false),
            1 => Ok(true),
            _ => Err(SchemaContractError::Decode),
        }
    }

    pub(super) fn read_u16(&mut self) -> Result<u16, SchemaContractError> {
        Ok(u16::from_be_bytes(self.read_array()?))
    }

    pub(super) fn read_u32(&mut self) -> Result<u32, SchemaContractError> {
        Ok(u32::from_be_bytes(self.read_array()?))
    }

    pub(super) fn read_i32(&mut self) -> Result<i32, SchemaContractError> {
        Ok(i32::from_be_bytes(self.read_array()?))
    }

    pub(super) fn read_u64(&mut self) -> Result<u64, SchemaContractError> {
        Ok(u64::from_be_bytes(self.read_array()?))
    }

    pub(super) fn read_i64(&mut self) -> Result<i64, SchemaContractError> {
        Ok(i64::from_be_bytes(self.read_array()?))
    }

    pub(super) fn read_u128(&mut self) -> Result<u128, SchemaContractError> {
        Ok(u128::from_be_bytes(self.read_array()?))
    }

    pub(super) fn read_i128(&mut self) -> Result<i128, SchemaContractError> {
        Ok(i128::from_be_bytes(self.read_array()?))
    }

    pub(super) fn read_count(
        &mut self,
        kind: &'static str,
        max: usize,
    ) -> Result<usize, SchemaContractError> {
        let len = usize::try_from(self.read_u32()?).map_err(|_| SchemaContractError::Decode)?;
        if len > max {
            return Err(SchemaContractError::TooManyItems { kind, len, max });
        }
        // Every current collection member consumes at least one tag or byte.
        // Reject impossible counts before reserving memory.
        if len > self.remaining() {
            return Err(SchemaContractError::Decode);
        }
        Ok(len)
    }

    pub(super) fn read_bytes(&mut self, max: usize) -> Result<&'a [u8], SchemaContractError> {
        let len = usize::try_from(self.read_u32()?).map_err(|_| SchemaContractError::Decode)?;
        if len > max {
            return Err(SchemaContractError::Decode);
        }
        self.take(len)
    }

    pub(super) fn read_string(&mut self, max: usize) -> Result<String, SchemaContractError> {
        let bytes = self.read_bytes(max)?;
        let value = std::str::from_utf8(bytes).map_err(|_| SchemaContractError::Decode)?;
        Ok(value.to_owned())
    }

    pub(super) fn read_array<const N: usize>(&mut self) -> Result<[u8; N], SchemaContractError> {
        self.take(N)?
            .try_into()
            .map_err(|_| SchemaContractError::Decode)
    }

    pub(super) fn expect_raw(&mut self, expected: &[u8]) -> Result<(), SchemaContractError> {
        if self.take(expected.len())? == expected {
            Ok(())
        } else {
            Err(SchemaContractError::Decode)
        }
    }

    fn take(&mut self, len: usize) -> Result<&'a [u8], SchemaContractError> {
        let end = self
            .offset
            .checked_add(len)
            .ok_or(SchemaContractError::Decode)?;
        let value = self
            .bytes
            .get(self.offset..end)
            .ok_or(SchemaContractError::Decode)?;
        self.offset = end;
        Ok(value)
    }
}
