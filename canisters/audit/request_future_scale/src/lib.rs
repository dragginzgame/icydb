//!
//! Large async endpoint surface used to expose request-future specialization.
//!

macro_rules! request_future_queries {
    ($(($name:ident, $marker:ident, $method:literal, $value:literal)),+ $(,)?) => {
        $(
            #[icydb::request_execution]
            async fn $name() -> u32 {
                std::future::ready($value).await
            }
            canic_icydb::wire_endpoint!(query, $method,
                icydb_testing_wasm_helpers::fixture_wire::$marker::ID,
                {
                    canic_icydb::__private::cdk::futures::spawn(async {
                        icydb_testing_canic_adapter::serve_async::<_, _, _>(icydb_testing_wasm_helpers::fixture_wire::$marker::ID, |()| $name()).await;
                    });
                }
            );
        )+
    };
}

request_future_queries!(
    (request_future_00, RequestFuture00, "request_future_00", 0),
    (request_future_01, RequestFuture01, "request_future_01", 1),
    (request_future_02, RequestFuture02, "request_future_02", 2),
    (request_future_03, RequestFuture03, "request_future_03", 3),
    (request_future_04, RequestFuture04, "request_future_04", 4),
    (request_future_05, RequestFuture05, "request_future_05", 5),
    (request_future_06, RequestFuture06, "request_future_06", 6),
    (request_future_07, RequestFuture07, "request_future_07", 7),
    (request_future_08, RequestFuture08, "request_future_08", 8),
    (request_future_09, RequestFuture09, "request_future_09", 9),
    (request_future_10, RequestFuture10, "request_future_10", 10),
    (request_future_11, RequestFuture11, "request_future_11", 11),
    (request_future_12, RequestFuture12, "request_future_12", 12),
    (request_future_13, RequestFuture13, "request_future_13", 13),
    (request_future_14, RequestFuture14, "request_future_14", 14),
    (request_future_15, RequestFuture15, "request_future_15", 15),
    (request_future_16, RequestFuture16, "request_future_16", 16),
    (request_future_17, RequestFuture17, "request_future_17", 17),
    (request_future_18, RequestFuture18, "request_future_18", 18),
    (request_future_19, RequestFuture19, "request_future_19", 19),
    (request_future_20, RequestFuture20, "request_future_20", 20),
    (request_future_21, RequestFuture21, "request_future_21", 21),
    (request_future_22, RequestFuture22, "request_future_22", 22),
    (request_future_23, RequestFuture23, "request_future_23", 23),
    (request_future_24, RequestFuture24, "request_future_24", 24),
    (request_future_25, RequestFuture25, "request_future_25", 25),
    (request_future_26, RequestFuture26, "request_future_26", 26),
    (request_future_27, RequestFuture27, "request_future_27", 27),
    (request_future_28, RequestFuture28, "request_future_28", 28),
    (request_future_29, RequestFuture29, "request_future_29", 29),
    (request_future_30, RequestFuture30, "request_future_30", 30),
    (request_future_31, RequestFuture31, "request_future_31", 31),
    (request_future_32, RequestFuture32, "request_future_32", 32),
    (request_future_33, RequestFuture33, "request_future_33", 33),
    (request_future_34, RequestFuture34, "request_future_34", 34),
    (request_future_35, RequestFuture35, "request_future_35", 35),
    (request_future_36, RequestFuture36, "request_future_36", 36),
    (request_future_37, RequestFuture37, "request_future_37", 37),
    (request_future_38, RequestFuture38, "request_future_38", 38),
    (request_future_39, RequestFuture39, "request_future_39", 39),
    (request_future_40, RequestFuture40, "request_future_40", 40),
    (request_future_41, RequestFuture41, "request_future_41", 41),
    (request_future_42, RequestFuture42, "request_future_42", 42),
    (request_future_43, RequestFuture43, "request_future_43", 43),
    (request_future_44, RequestFuture44, "request_future_44", 44),
    (request_future_45, RequestFuture45, "request_future_45", 45),
    (request_future_46, RequestFuture46, "request_future_46", 46),
    (request_future_47, RequestFuture47, "request_future_47", 47),
    (request_future_48, RequestFuture48, "request_future_48", 48),
    (request_future_49, RequestFuture49, "request_future_49", 49),
    (request_future_50, RequestFuture50, "request_future_50", 50),
    (request_future_51, RequestFuture51, "request_future_51", 51),
    (request_future_52, RequestFuture52, "request_future_52", 52),
    (request_future_53, RequestFuture53, "request_future_53", 53),
    (request_future_54, RequestFuture54, "request_future_54", 54),
    (request_future_55, RequestFuture55, "request_future_55", 55),
    (request_future_56, RequestFuture56, "request_future_56", 56),
    (request_future_57, RequestFuture57, "request_future_57", 57),
    (request_future_58, RequestFuture58, "request_future_58", 58),
    (request_future_59, RequestFuture59, "request_future_59", 59),
    (request_future_60, RequestFuture60, "request_future_60", 60),
    (request_future_61, RequestFuture61, "request_future_61", 61),
    (request_future_62, RequestFuture62, "request_future_62", 62),
    (request_future_63, RequestFuture63, "request_future_63", 63),
);

// The final host owns grants and exclusions; components request permanent keys.
fn icydb_memory_pool()
-> Result<icydb::db::MemoryAllocationPool, icydb::db::MemoryAllocationPoolError> {
    icydb::db::MemoryAllocationPool::new(
        vec![icydb::db::MemoryAuthority::new(
            "icydb.default_empty",
            "icydb.default_empty.",
        )?],
        vec![],
    )
}

canic_icydb::start!(icydb);
