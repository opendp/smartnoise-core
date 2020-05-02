use whitenoise_validator::errors::*;

use ndarray::prelude::*;
use crate::NodeArguments;
use whitenoise_validator::base::{Value, Array, Hashmap, ReleaseNode};
use crate::components::Evaluable;
use ndarray;
use whitenoise_validator::proto;
use whitenoise_validator::utilities::{get_ith_column, get_argument};
use crate::utilities::standardize_columns;
use indexmap::map::IndexMap;

impl Evaluable for proto::Dataframe {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let data = get_argument(arguments, "data")?.array()?;
        let column_names = arguments.get("column_names")
            .and_then(|column_names| column_names.array().ok()?.string().ok()).cloned();

        let num_columns = arguments.get("num_columns")
            .and_then(|num_columns| num_columns.first_i64().ok());

        // num columns is sufficient shared information to build the dataframes
        let num_columns = match (column_names.clone(), num_columns) {
            (Some(column_names), None) => match column_names.into_dimensionality::<Ix1>() {
                Ok(column_names) => column_names,
                Err(_) => return Err("column names must be one-dimensional".into())
            }.to_vec().len(),
            (None, Some(num_columns)) => num_columns as usize,
            _ => return Err("either column_names or num_columns must be provided".into())
        };

        // force the input to be an array- reject hashmap and jagged
        Ok(ReleaseNode::new(match column_names {
            Some(column_names) => Value::Hashmap(Hashmap::<Value>::Str(match data {
                Array::F64(array) => {
                    let standardized = standardize_columns(array, num_columns)?;
                    column_names.into_iter().enumerate()
                        .map(|(idx, name)| Ok((name.clone(), get_ith_column(&standardized, &idx)?.into())))
                        .collect::<Result<IndexMap<String, Value>>>()?
                }
                Array::I64(array) => {
                    let standardized = standardize_columns(array, num_columns)?;
                    column_names.into_iter().enumerate()
                        .map(|(idx, name)| Ok((name.clone(), get_ith_column(&standardized, &idx)?.into())))
                        .collect::<Result<IndexMap<String, Value>>>()?
                }
                Array::Bool(array) => {
                    let standardized = standardize_columns(array, num_columns)?;
                    column_names.into_iter().enumerate()
                        .map(|(idx, name)| Ok((name.clone(), get_ith_column(&standardized, &idx)?.into())))
                        .collect::<Result<IndexMap<String, Value>>>()?
                }
                Array::Str(array) => {
                    let standardized = standardize_columns(array, num_columns)?;
                    column_names.into_iter().enumerate()
                        .map(|(idx, name)| Ok((name.clone(), get_ith_column(&standardized, &idx)?.into())))
                        .collect::<Result<IndexMap<String, Value>>>()?
                }
            })),
            None => match data {
                Array::F64(array) => standardize_columns(array, num_columns)?.into(),
                Array::I64(array) => standardize_columns(array, num_columns)?.into(),
                Array::Bool(array) => standardize_columns(array, num_columns)?.into(),
                Array::Str(array) => standardize_columns(array, num_columns)?.into(),
            }
        }))
    }
}