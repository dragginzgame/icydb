pub(super) const VALUE_BINARY_TAG_ACCOUNT: u8 = 0x80;
pub(super) const VALUE_BINARY_TAG_DATE: u8 = 0x81;
pub(super) const VALUE_BINARY_TAG_DECIMAL: u8 = 0x82;
pub(super) const VALUE_BINARY_TAG_DURATION: u8 = 0x83;
pub(super) const VALUE_BINARY_TAG_ENUM: u8 = 0x84;
pub(super) const VALUE_BINARY_TAG_FLOAT32: u8 = 0x85;
pub(super) const VALUE_BINARY_TAG_FLOAT64: u8 = 0x86;
pub(super) const VALUE_BINARY_TAG_INT128: u8 = 0x87;
pub(super) const VALUE_BINARY_TAG_INT_BIG: u8 = 0x88;
pub(super) const VALUE_BINARY_TAG_PRINCIPAL: u8 = 0x89;
pub(super) const VALUE_BINARY_TAG_SUBACCOUNT: u8 = 0x8A;
pub(super) const VALUE_BINARY_TAG_TIMESTAMP: u8 = 0x8B;
pub(super) const VALUE_BINARY_TAG_NAT128: u8 = 0x8C;
pub(super) const VALUE_BINARY_TAG_NAT_BIG: u8 = 0x8D;
pub(super) const VALUE_BINARY_TAG_ULID: u8 = 0x8E;

// Keep the locally owned extension tag set explicit. These tags all carry one
// nested structural value-storage payload immediately after the tag byte.
pub(super) const fn is_local_value_storage_tag(tag: u8) -> bool {
    matches!(
        tag,
        VALUE_BINARY_TAG_ACCOUNT
            | VALUE_BINARY_TAG_DATE
            | VALUE_BINARY_TAG_DECIMAL
            | VALUE_BINARY_TAG_DURATION
            | VALUE_BINARY_TAG_ENUM
            | VALUE_BINARY_TAG_FLOAT32
            | VALUE_BINARY_TAG_FLOAT64
            | VALUE_BINARY_TAG_INT128
            | VALUE_BINARY_TAG_INT_BIG
            | VALUE_BINARY_TAG_PRINCIPAL
            | VALUE_BINARY_TAG_SUBACCOUNT
            | VALUE_BINARY_TAG_TIMESTAMP
            | VALUE_BINARY_TAG_NAT128
            | VALUE_BINARY_TAG_NAT_BIG
            | VALUE_BINARY_TAG_ULID
    )
}
