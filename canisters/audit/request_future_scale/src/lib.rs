//!
//! Large async endpoint surface used to expose request-future specialization.
//!

macro_rules! request_future_queries {
    ($(($name:ident, $value:literal)),+ $(,)?) => {
        $(
            #[icydb::request_execution]
            #[ic_cdk::query]
            async fn $name() -> u32 {
                std::future::ready($value).await
            }
        )+
    };
}

request_future_queries!(
    (request_future_00, 0),
    (request_future_01, 1),
    (request_future_02, 2),
    (request_future_03, 3),
    (request_future_04, 4),
    (request_future_05, 5),
    (request_future_06, 6),
    (request_future_07, 7),
    (request_future_08, 8),
    (request_future_09, 9),
    (request_future_10, 10),
    (request_future_11, 11),
    (request_future_12, 12),
    (request_future_13, 13),
    (request_future_14, 14),
    (request_future_15, 15),
    (request_future_16, 16),
    (request_future_17, 17),
    (request_future_18, 18),
    (request_future_19, 19),
    (request_future_20, 20),
    (request_future_21, 21),
    (request_future_22, 22),
    (request_future_23, 23),
    (request_future_24, 24),
    (request_future_25, 25),
    (request_future_26, 26),
    (request_future_27, 27),
    (request_future_28, 28),
    (request_future_29, 29),
    (request_future_30, 30),
    (request_future_31, 31),
    (request_future_32, 32),
    (request_future_33, 33),
    (request_future_34, 34),
    (request_future_35, 35),
    (request_future_36, 36),
    (request_future_37, 37),
    (request_future_38, 38),
    (request_future_39, 39),
    (request_future_40, 40),
    (request_future_41, 41),
    (request_future_42, 42),
    (request_future_43, 43),
    (request_future_44, 44),
    (request_future_45, 45),
    (request_future_46, 46),
    (request_future_47, 47),
    (request_future_48, 48),
    (request_future_49, 49),
    (request_future_50, 50),
    (request_future_51, 51),
    (request_future_52, 52),
    (request_future_53, 53),
    (request_future_54, 54),
    (request_future_55, 55),
    (request_future_56, 56),
    (request_future_57, 57),
    (request_future_58, 58),
    (request_future_59, 59),
    (request_future_60, 60),
    (request_future_61, 61),
    (request_future_62, 62),
    (request_future_63, 63),
);

icydb::start!();

#[cfg(feature = "candid-export")]
ic_cdk::export_candid!();
