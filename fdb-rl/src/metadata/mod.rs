//! TODO

pub(crate) mod error;

use fdb::error::FdbResult;
use fdb::tuple::Tuple;

use std::collections::HashMap;

use crate::protobuf::WellFormedDynamicMessage;

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
    index_fns: HashMap<String, fn(WellFormedDynamicMessage) -> FdbResult<Vec<Tuple>>>,
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

    use crate::protobuf::{WellFormedDynamicMessage, WellFormedMessageDescriptor};

    use super::PrimaryKeyAndIndexFunctions;

    fn primary_key_fn(well_formed_dynamic_message: WellFormedDynamicMessage) -> FdbResult<Tuple> {
        let mut bindings = MapBindings::default();
        bindings.insert("record", Value::try_from(well_formed_dynamic_message)?);

        let catalog = PartiqlCatalog::default();

        let logical_planner = LogicalPlanner::new(&catalog);
        let mut eval_planner = EvaluatorPlanner::new(EvaluationMode::Permissive, &catalog);

        let parser = Parser::default();
        let parsed_ast = parser.parse("SELECT VALUE [ { 'fdb_type': 'string', 'fdb_value': r.fdb_rl_value.hello.fdb_rl_value } ] FROM record AS r").unwrap();

        let logical_plan = logical_planner.lower(&parsed_ast).unwrap();
        let mut eval_plan = eval_planner.compile(&logical_plan).unwrap();

        let context = BasicContext::new(
            bindings,
            SystemContext {
                now: DateTime::from_system_now_utc(),
            },
        );

        let evaluated = eval_plan.execute_mut(&context).unwrap();

        println!("{:?}", evaluated.result);

        Err(FdbError::new(123))
    }

    fn index_fn_1(_: WellFormedDynamicMessage) -> FdbResult<Vec<Tuple>> {
        todo!();
    }

    fn index_fn_2(_: WellFormedDynamicMessage) -> FdbResult<Vec<Tuple>> {
        todo!();
    }

    fn index_fn_3(_: WellFormedDynamicMessage) -> FdbResult<Vec<Tuple>> {
        todo!();
    }

    static ABCD_RECORD_STORE_METADATA_PRIMARY_KEY_AND_INDEX_FUNCTIONS: LazyLock<
        PrimaryKeyAndIndexFunctions,
    > = LazyLock::new(|| PrimaryKeyAndIndexFunctions {
        primary_key_fn: primary_key_fn,
        index_fns: HashMap::from([
            (
                "one".to_string(),
                index_fn_1 as fn(WellFormedDynamicMessage) -> FdbResult<Vec<Tuple>>,
            ),
            (
                "two".to_string(),
                index_fn_2 as fn(WellFormedDynamicMessage) -> FdbResult<Vec<Tuple>>,
            ),
            (
                "three".to_string(),
                index_fn_3 as fn(WellFormedDynamicMessage) -> FdbResult<Vec<Tuple>>,
            ),
        ]),
    });

    #[test]
    fn wip() {
        use fdb_rl_proto::fdb_rl_test::protobuf::well_formed_dynamic_message::v1::HelloWorldString;

        let hello_world_string = HelloWorldString {
            hello: Some("hello".to_string()),
            world: None,
        };

        let well_formed_message_descriptor =
            WellFormedMessageDescriptor::try_from(hello_world_string.descriptor()).unwrap();

        let well_formed_dynamic_message = WellFormedDynamicMessage::try_from((
            well_formed_message_descriptor,
            &hello_world_string,
        ))
        .unwrap();

        // invoke primary key function
        let _ = (ABCD_RECORD_STORE_METADATA_PRIMARY_KEY_AND_INDEX_FUNCTIONS
            .deref()
            .primary_key_fn)(well_formed_dynamic_message);

        // // invoke index function
        // let _ = (ABCD_RECORD_STORE_METADATA_PRIMARY_KEY_AND_INDEX_FUNCTIONS
        //     .deref()
        //     .index_fns
        //     .get(&"one".to_string())
        //     .unwrap())(well_formed_dynamic_message);

        // SELECT VALUE [ { 'fdb_type': 'string', 'fdb_value': r.fdb_rl_value.hello.fdb_rl_value } ] FROM record AS r
        // SELECT VALUE [ r.fdb_rl_value.hello.fdb_rl_value ] FROM record AS r

        // SELECT VALUE [ { 'fdb_type': 'maybe_string', 'fdb_value': r.fdb_rl_value.world.fdb_rl_value}, { 'fdb_type': 'string', 'fdb_value': r.fdb_rl_value.hello.fdb_rl_value } ] FROM record AS r
        // SELECT VALUE [ r.fdb_rl_value.world.fdb_rl_value, r.fdb_rl_value.hello.fdb_rl_value ] FROM record AS r
    }
}
