use super::*;

#[test]
fn exact_fixed_scale_metadata_clamps_zero_scale_width() {
    let rendered =
        render_projection_value_text(Some(4_000_000_000), &OutputValue::decimal(Decimal::ZERO));

    assert_eq!(rendered, "0.0000000000000000000000000000");
}
