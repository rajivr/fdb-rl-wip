//! TODO

pub(crate) mod error;
pub(crate) mod tuple_schema;

use fdb::error::FdbResult;
use fdb::tuple::Tuple;

use std::collections::HashMap;

use crate::protobuf::WellFormedDynamicMessage;

pub use tuple_schema::{
    IndexSchema, IndexSchemaElement, PrimaryKeySchema, PrimaryKeySchemaElement,
};

pub(crate) use tuple_schema::{IndexSchemaKey, IndexSchemaValue};

// /// TODO
// pub(crate) struct KeyAndIndexFunctions<FnPK, FnIdx>
// where
//     FnPK: Fn(WellFormedDynamicMessage) -> FdbResult<FdbTuple>,
//     FnIdx: Fn(WellFormedDynamicMessage) -> FdbResult<Vec<FdbTuple>>,
// {
//     primary_key_fn: FnPK,
//     indexes_fn: HashMap<String, FnIdx>,
// }
//
// https://stackoverflow.com/q/37370120

/// TODO
pub(crate) struct PrimaryKeyAndIndexFunctions {
    primary_key_fn: fn(WellFormedDynamicMessage) -> FdbResult<Tuple>,
    index_fns: HashMap<
        String,
        fn(WellFormedDynamicMessage, Option<u64>, u16) -> FdbResult<Vec<(Tuple, Option<Tuple>)>>,
    >,
}

#[cfg(test)]
mod tests {
    use fdb::error::{FdbError, FdbResult};
    use fdb::tuple::Tuple;

    use partiql_catalog::context::SystemContext;
    use partiql_catalog::PartiqlCatalog;
    use partiql_eval::env::basic::MapBindings;
    use partiql_eval::eval::BasicContext;
    use partiql_eval::plan::{EvaluationMode, EvaluatorPlanner};
    use partiql_logical_planner::LogicalPlanner;
    use partiql_parser::Parser;
    use partiql_value::{DateTime, Value};

    use prost_reflect::ReflectMessage;

    use std::collections::HashMap;
    use std::convert::TryFrom;
    use std::ops::Deref;
    use std::sync::LazyLock;

    use crate::partiql::fdb_tuple::{index_value, primary_key_value};
    use crate::protobuf::{WellFormedDynamicMessage, WellFormedMessageDescriptor};

    use super::PrimaryKeyAndIndexFunctions;

    fn primary_key_fn(well_formed_dynamic_message: WellFormedDynamicMessage) -> FdbResult<Tuple> {
        // { 'fdb_rl_type': 'message_HelloWorldWktV1Uuid', 'fdb_rl_value': { 'primary_key': { 'fdb_rl_type': 'message_fdb_rl.field.v1.UUID', 'fdb_rl_value': { 'uuid_value': '[255, 255, 255, 255, 186, 94, 186, 17, 0, 0, 0, 0, 92, 161, 171, 30]' } }, 'hello': { 'fdb_rl_type': 'string', 'fdb_rl_value': 'hello' }, 'world': { 'fdb_rl_type': 'string', 'fdb_rl_value': NULL } } }
        let mut bindings = MapBindings::default();
        bindings.insert("record", Value::try_from(well_formed_dynamic_message)?);

        let catalog = PartiqlCatalog::default();

        let logical_planner = LogicalPlanner::new(&catalog);
        let mut eval_planner = EvaluatorPlanner::new(EvaluationMode::Permissive, &catalog);

        let parser = Parser::default();
        let parsed_ast = parser.parse("SELECT VALUE [ { 'fdb_type': 'v1_uuid', 'fdb_value': r.fdb_rl_value.primary_key.fdb_rl_value } ] FROM record AS r").unwrap();

        let logical_plan = logical_planner.lower(&parsed_ast).unwrap();
        let mut eval_plan = eval_planner.compile(&logical_plan).unwrap();

        let context = BasicContext::new(
            bindings,
            SystemContext {
                now: DateTime::from_system_now_utc(),
            },
        );

        let evaluated = eval_plan.execute_mut(&context).unwrap();

        // <<[{ 'fdb_type': 'integer', 'fdb_value': 108 }]>>
        println!("{:?}", primary_key_value(evaluated.result));

        Err(FdbError::new(123))
    }

    // // String
    // fn primary_key_fn(well_formed_dynamic_message: WellFormedDynamicMessage) -> FdbResult<Tuple> {
    //     let mut bindings = MapBindings::default();
    //     bindings.insert("record", Value::try_from(well_formed_dynamic_message)?);

    //     let catalog = PartiqlCatalog::default();

    //     let logical_planner = LogicalPlanner::new(&catalog);
    //     let mut eval_planner = EvaluatorPlanner::new(EvaluationMode::Permissive, &catalog);

    //     let parser = Parser::default();
    //     let parsed_ast = parser.parse("SELECT VALUE [ { 'fdb_type': 'string', 'fdb_value': r.fdb_rl_value.hello.fdb_rl_value } ] FROM record AS r").unwrap();

    //     let logical_plan = logical_planner.lower(&parsed_ast).unwrap();
    //     let mut eval_plan = eval_planner.compile(&logical_plan).unwrap();

    //     let context = BasicContext::new(
    //         bindings,
    //         SystemContext {
    //             now: DateTime::from_system_now_utc(),
    //         },
    //     );

    //     let evaluated = eval_plan.execute_mut(&context).unwrap();

    //     // <<[{ 'fdb_type': 'string', 'fdb_value': 'hello' }]>>
    //     println!("{:?}", primary_key_value(evaluated.result));

    //     Err(FdbError::new(123))
    // }

    fn index_fn_1(
        well_formed_dynamic_message: WellFormedDynamicMessage,
        incarnation_version: Option<u64>,
        local_version: u16,
    ) -> FdbResult<Vec<(Tuple, Option<Tuple>)>> {
        println!("{:?}", Value::try_from(well_formed_dynamic_message)?);

        // let mut bindings = MapBindings::default();
        // bindings.insert("record", Value::try_from(well_formed_dynamic_message)?);

        // let catalog = PartiqlCatalog::default();

        // let logical_planner = LogicalPlanner::new(&catalog);
        // let mut eval_planner = EvaluatorPlanner::new(EvaluationMode::Permissive, &catalog);

        // let parser = Parser::default();

        // let parsed_ast = parser.parse("SELECT VALUE { 'repeat_me': ( SELECT VALUE fdb_rl_value FROM repeat_me.fdb_rl_value AS x AT i ORDER BY i ) } FROM record AS r, r.fdb_rl_value.repeat_me AS repeat_me").unwrap();

        // let parsed_ast = parser.parse("SELECT VALUE { 'attr': CAST(repeat_me.fdb_rl_value[*].fdb_rl_value AS LIST) } FROM record AS r, r.fdb_rl_value.repeat_me AS repeat_me").unwrap();

        // // let parsed_ast = parser.parse("SELECT VALUE { 'key': [ { 'fdb_type': 'v1_uuid', 'fdb_value': r.fdb_rl_value.primary_key.fdb_rl_value }, { 'fdb_type': 'versionstamp', 'fdb_value': {'incarnation_version': 10, 'local_version': 20} } ], 'value': [ { 'fdb_type': 'string', 'fdb_value':  r.fdb_rl_value.hello.fdb_rl_value } ] } FROM record AS r").unwrap();

        // let query = format!(
        //     "SELECT VALUE {{ 'key': [ {{ 'fdb_type': 'v1_uuid', 'fdb_value': r.fdb_rl_value.primary_key.fdb_rl_value }}, {{ 'fdb_type': 'versionstamp', 'fdb_value': {{ 'incarnation_version': {incarnation_version}, 'local_version': {local_version} }} }} ], 'value': [ {{ 'fdb_type': 'string', 'fdb_value':  r.fdb_rl_value.world.fdb_rl_value }} ] }} FROM record AS r",
        //     incarnation_version=incarnation_version
        // 	.map(|x| format!("{}", x))
        // 	.unwrap_or_else(|| "NULL".to_string()),
        //     local_version=local_version
        // );
        // let parsed_ast = parser.parse(query.as_str()).unwrap();

        // let logical_plan = logical_planner.lower(&parsed_ast).unwrap();
        // let mut eval_plan = eval_planner.compile(&logical_plan).unwrap();

        // let context = BasicContext::new(
        //     bindings,
        //     SystemContext {
        //         now: DateTime::from_system_now_utc(),
        //     },
        // );

        // let evaluated = eval_plan.execute_mut(&context).unwrap();

        // println!("{:?}", evaluated.result);
        // // println!("{:?}", index_value(evaluated.result));

        Err(FdbError::new(123))
    }

    fn index_fn_2(
        _: WellFormedDynamicMessage,
        _: Option<u64>,
        _: u16,
    ) -> FdbResult<Vec<(Tuple, Option<Tuple>)>> {
        todo!();
    }

    fn index_fn_3(
        _: WellFormedDynamicMessage,
        _: Option<u64>,
        _: u16,
    ) -> FdbResult<Vec<(Tuple, Option<Tuple>)>> {
        todo!();
    }

    static ABCD_RECORD_STORE_METADATA_PRIMARY_KEY_AND_INDEX_FUNCTIONS: LazyLock<
        PrimaryKeyAndIndexFunctions,
    > = LazyLock::new(|| PrimaryKeyAndIndexFunctions {
        primary_key_fn: primary_key_fn,
        index_fns: HashMap::from([
            (
                "one".to_string(),
                index_fn_1
                    as fn(
                        WellFormedDynamicMessage,
                        Option<u64>,
                        u16,
                    ) -> FdbResult<Vec<(Tuple, Option<Tuple>)>>,
            ),
            (
                "two".to_string(),
                index_fn_2
                    as fn(
                        WellFormedDynamicMessage,
                        Option<u64>,
                        u16,
                    ) -> FdbResult<Vec<(Tuple, Option<Tuple>)>>,
            ),
            (
                "three".to_string(),
                index_fn_3
                    as fn(
                        WellFormedDynamicMessage,
                        Option<u64>,
                        u16,
                    ) -> FdbResult<Vec<(Tuple, Option<Tuple>)>>,
            ),
        ]),
    });

    #[test]
    fn wip() {
        use fdb_rl_proto::fdb_rl_test::java::proto::expression_tests::v1::TestScalarFieldAccess;
        let test_scalar_field_access = TestScalarFieldAccess {
            field: Some("Plants".to_string()),
            repeat_me: vec!["Boxes".to_string(), "Bowls".to_string()],
            bytes_field: None,
            uuid_field: None,
        };

        let well_formed_message_descriptor =
            WellFormedMessageDescriptor::try_from(test_scalar_field_access.descriptor()).unwrap();

        let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
            well_formed_message_descriptor,
            &test_scalar_field_access,
        ))
        .unwrap();

        // invoke secondary key function
        let _ = ((ABCD_RECORD_STORE_METADATA_PRIMARY_KEY_AND_INDEX_FUNCTIONS
            .deref()
            .index_fns)
            .get("one")
            // .unwrap())(well_formed_dynamic_message, Some(10), 20);
            .unwrap())(well_formed_dynamic_message, None, 20);

        // // invoke primary key function
        // let _ = (ABCD_RECORD_STORE_METADATA_PRIMARY_KEY_AND_INDEX_FUNCTIONS
        //     .deref()
        //     .primary_key_fn)(well_formed_dynamic_message);
    }

    // #[test]
    // fn wip() {
    //     use fdb_rl_proto::fdb_rl_test::java::proto::expression_tests::v1::TestScalarFieldAccess;

    //     let plants_boxes_and_bowls = {
    //         let mut x = TestScalarFieldAccess::default();
    //         x.field = Some("Plants".to_string());
    //         x.repeat_me.push("Boxes".to_string());
    //         x.repeat_me.push("Bowls".to_string());

    //         x
    //     };

    //     let well_formed_message_descriptor =
    //         WellFormedMessageDescriptor::try_from(plants_boxes_and_bowls.descriptor()).unwrap();

    //     let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
    //         well_formed_message_descriptor,
    //         &plants_boxes_and_bowls,
    //     ))
    //     .unwrap();

    //     // invoke secondary key function
    //     let _ = ((ABCD_RECORD_STORE_METADATA_PRIMARY_KEY_AND_INDEX_FUNCTIONS
    //         .deref()
    //         .index_fns)
    //         .get("one")
    //         .unwrap())(well_formed_dynamic_message);

    //     // // invoke primary key function
    //     // let _ = (ABCD_RECORD_STORE_METADATA_PRIMARY_KEY_AND_INDEX_FUNCTIONS
    //     //     .deref()
    //     //     .primary_key_fn)(well_formed_dynamic_message);
    // }

    // // string
    // #[test]
    // fn wip() {
    //     use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldString;

    //     let hello_world_string = HelloWorldString {
    //         hello: Some("hello".to_string()),
    //         world: None,
    //     };

    //     let well_formed_message_descriptor =
    //         WellFormedMessageDescriptor::try_from(hello_world_string.descriptor()).unwrap();

    //     let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
    //         well_formed_message_descriptor,
    //         &hello_world_string,
    //     ))
    //     .unwrap();

    //     // invoke primary key function
    //     let _ = (ABCD_RECORD_STORE_METADATA_PRIMARY_KEY_AND_INDEX_FUNCTIONS
    //         .deref()
    //         .primary_key_fn)(well_formed_dynamic_message);

    //     // // invoke index function
    //     // let _ = (ABCD_RECORD_STORE_METADATA_PRIMARY_KEY_AND_INDEX_FUNCTIONS
    //     //     .deref()
    //     //     .index_fns
    //     //     .get(&"one".to_string())
    //     //     .unwrap())(well_formed_dynamic_message);

    //     // SELECT VALUE [ { 'fdb_type': 'string', 'fdb_value': r.fdb_rl_value.hello.fdb_rl_value } ] FROM record AS r
    //     // SELECT VALUE [ r.fdb_rl_value.hello.fdb_rl_value ] FROM record AS r

    //     // SELECT VALUE [ { 'fdb_type': 'maybe_string', 'fdb_value': r.fdb_rl_value.world.fdb_rl_value}, { 'fdb_type': 'string', 'fdb_value': r.fdb_rl_value.hello.fdb_rl_value } ] FROM record AS r
    //     // SELECT VALUE [ r.fdb_rl_value.world.fdb_rl_value, r.fdb_rl_value.hello.fdb_rl_value ] FROM record AS r
    // }
}
