use whitenoise_validator::errors::*;

use ndarray::prelude::*;
use crate::NodeArguments;
use whitenoise_validator::base::{Value, Array, Indexmap, ReleaseNode};
use crate::components::Evaluable;
use ndarray;
use whitenoise_validator::proto;
use whitenoise_validator::utilities::{get_ith_column, get_argument};
use crate::utilities::standardize_columns;
use indexmap::map::IndexMap;

impl Evaluable for proto::Rename {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let data = get_argument(arguments, "data")?.array()?;

        let column_names  = get_argument(arguments, "column_names")?
            .array()?.string()?;

        // num columns is sufficient shared information to build the dataframe
        let num_columns = match column_names.clone().into_dimensionality::<Ix1>() {
            Ok(column_names) => column_names,
            Err(_) => return Err("column names must be one-dimensional".into())
        }.to_vec().len();

        // force the input to be an array- reject indexmap and jagged
        Ok(ReleaseNode::new(Value::Indexmap(Indexmap::<Value>::Str(match data {
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
        }))))
    }
}