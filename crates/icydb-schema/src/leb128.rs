//! Borrowed bigint LEB128 encoding shared by signed and unsigned atoms.

// The atom owns the exact length/sign contract. Pack borrowed magnitude limbs
// into seven-bit groups; negative values form two's complement one limb at a
// time. Zero magnitude limbs supply the required positive/negative extension.
pub(crate) fn bytes(
    mut limbs: impl Iterator<Item = u32>,
    negative: bool,
    mut remaining: u64,
) -> impl Iterator<Item = u8> {
    let mut pending = 0_u64;
    let mut pending_bits = 0;
    let mut carry = negative;
    std::iter::from_fn(move || {
        if remaining == 0 {
            return None;
        }
        if pending_bits < 7 {
            let magnitude = limbs.next().unwrap_or(0);
            let limb = if negative {
                let (limb, overflow) = (!magnitude).overflowing_add(u32::from(carry));
                carry = overflow;
                limb
            } else {
                magnitude
            };
            pending |= u64::from(limb) << pending_bits;
            pending_bits += 32;
        }
        let byte = pending.to_le_bytes()[0] & 0x7f;
        pending >>= 7;
        pending_bits -= 7;
        remaining -= 1;
        Some(byte | if remaining == 0 { 0 } else { 0x80 })
    })
}
