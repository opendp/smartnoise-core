use whitenoise_validator::errors::*;

use ndarray::prelude::*;
use crate::NodeArguments;
use whitenoise_validator::base::{Value, Array, ReleaseNode, IndexKey};
use crate::components::Evaluable;
use ndarray;
use whitenoise_validator::proto;
use whitenoise_validator::utilities::{array::get_ith_column, get_argument};
use crate::utilities::standardize_columns;
use indexmap::map::IndexMap;

impl Evaluable for proto::Dataframe {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, arguments: &NodeArguments) -> Result<ReleaseNode> {
        // force the input to be an array- reject indexmap and jagged
        let data = get_argument(arguments, "data")?.array()?;

        let column_names  = get_argument(arguments, "names")?
            .array()?.string()?;

        // num columns is sufficient shared information to build the dataframe
        let num_columns = match column_names.clone().into_dimensionality::<Ix1>() {
            Ok(column_names) => column_names,
            Err(_) => return Err("column names must be one-dimensional".into())
        }.to_vec().len();

        // split each column name into its own column
        Ok(ReleaseNode::new(Value::Indexmap(match data {
            Array::Float(array) => {
                let standardized = standardize_columns(array, num_columns)?;
                column_names.into_iter().enumerate()
                    .map(|(idx, name)| Ok((name.to_string().into(), get_ith_column(&standardized, idx)?.into())))
                    .collect::<Result<IndexMap<IndexKey, Value>>>()?
            }
            Array::Int(array) => {
                let standardized = standardize_columns(array, num_columns)?;
                column_names.into_iter().enumerate()
                    .map(|(idx, name)| Ok((name.to_string().into(), get_ith_column(&standardized, idx)?.into())))
                    .collect::<Result<IndexMap<IndexKey, Value>>>()?
            }
            Array::Bool(array) => {
                let standardized = standardize_columns(array, num_columns)?;
                column_names.into_iter().enumerate()
                    .map(|(idx, name)| Ok((name.to_string().into(), get_ith_column(&standardized, idx)?.into())))
                    .collect::<Result<IndexMap<IndexKey, Value>>>()?
            }
            Array::Str(array) => {
                let standardized = standardize_columns(array, num_columns)?;
                column_names.into_iter().enumerate()
                    .map(|(idx, name)| Ok((name.to_string().into(), get_ith_column(&standardized, idx)?.into())))
                    .collect::<Result<IndexMap<IndexKey, Value>>>()?
            }
        })))
    }
}