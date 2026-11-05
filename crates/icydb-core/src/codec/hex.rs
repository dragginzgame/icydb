//! Formatting-free hexadecimal byte encoding shared by values and database surfaces.

/// Encode bytes as lowercase hexadecimal text.
#[must_use]
pub fn encode_hex_lower(bytes: &[u8]) -> String {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    // Keep common hex emission allocation-bounded and formatting-free.
    // Formatting each byte with `"{byte:02x}"` is equivalent on the wire, but
    // it routes through the formatter for every byte on hot cursor/fingerprint
    // display paths.
    let mut out = String::with_capacity(bytes.len().saturating_mul(2));

    for byte in bytes {
        out.push(HEX[(byte >> 4) as usize] as char);
        out.push(HEX[(byte & 0x0f) as usize] as char);
    }

    out
}

#[cfg(test)]
mod tests;
