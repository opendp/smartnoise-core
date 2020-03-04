use yarrow_validator::errors::*;

use crate::base::NodeArguments;
use yarrow_validator::base::{Value, ArrayND};
use crate::components::Evaluable;
use std::collections::HashMap;
use yarrow_validator::utilities::serial::parse_value;
use ndarray::Array;
use yarrow_validator::proto;

impl Evaluable for proto::Materialize {
    fn evaluate(&self, _arguments: &NodeArguments) -> Result<Value> {
        match self.value.as_ref().unwrap() {
            proto::materialize::Value::Literal(value) => parse_value(value),
            proto::materialize::Value::FilePath(path) => {
                let mut response = HashMap::<String, Vec<String>>::new();
                csv::Reader::from_path(path).unwrap().deserialize()
                    .for_each(|result| {
                        // parse each record into the yarrow internal format
                        let record: HashMap<String, String> = result.unwrap();
                        record.iter().for_each(|(k, v)| response
                            .entry(k.to_owned()).or_insert_with(Vec::new)
                            .push(v.clone()));
                    });
                Ok(Value::HashmapString(response.iter()
                    .map(|(k, v): (&String, &Vec<String>)| (
                        k.clone(), Value::ArrayND(ArrayND::Str(Array::from(v.to_owned()).into_dyn()))
                    ))
                    .collect::<HashMap<String, Value>>()))
            }
            _ => Err("the selected table reference format is not implemented".into())
        }
    }
}
