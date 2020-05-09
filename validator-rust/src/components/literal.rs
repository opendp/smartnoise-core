
use crate::errors::*;
use crate::components::Named;
use std::collections::HashMap;
use crate::utilities::get_ith_column;
use ndarray::ArrayD;
use crate::{proto};
use crate::base::{Value, Array};


impl Named for proto::Literal {
    fn get_names(
        &self,
        _public_arguments: &HashMap<String, Value>,
        _argument_variables: &HashMap<String, Vec<String>>,
        release: &Option<&Value>
    ) -> Result<Vec<String>> {

        fn array_to_names<T: ToString + Clone + Default>(array: &ArrayD<T>, num_columns: i64) -> Result<Vec<String>> {
            (0..num_columns as usize)
                .map(|index| {
                    let array = get_ith_column(array, &index)?;
                    match array.ndim() {
                        0 => match array.first() {
                            Some(value) => Ok(value.to_string()),
                            None => Err("array may not be empty".into())
                        },
                        1 => Ok("[Literal Column]".into()),
                        _ => Err("array has too great of a dimension".into())
                    }
                })
                .collect::<Result<Vec<String>>>()
        }

        match release {
            Some(release) => match release {
                Value::Jagged(jagged) => Ok((0..jagged.num_columns()).map(|_| "[Literal vector]".to_string()).collect()),
                Value::Hashmap(_) => Err("names for hashmap literals are not supported".into()),  // (or necessary)
                Value::Array(value) => match value {
                    Array::F64(array) => array_to_names(array, value.num_columns()?),
                    Array::I64(array) => array_to_names(array, value.num_columns()?),
                    Array::Str(array) => array_to_names(array, value.num_columns()?),
                    Array::Bool(array) => array_to_names(array, value.num_columns()?),
                }
            },
            None => Err("Literals must always be accompanied by a release".into())
        }
    }
}