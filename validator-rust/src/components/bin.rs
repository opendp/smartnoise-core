use crate::errors::*;

use std::collections::HashMap;
use crate::base::{Nature, NodeProperties, NatureCategorical, Jagged, ValueProperties, Array, DataType};

use crate::proto;
use crate::utilities::{prepend, standardize_categorical_argument, standardize_null_target_argument, standardize_float_argument, deduplicate};
use crate::components::Component;

use crate::base::Value;
use std::iter::Sum;
use std::ops::Div;
use noisy_float::prelude::n64;

impl Component for proto::Bin {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or_else(|| Error::from("data: missing"))?.array()
            .map_err(prepend("data:"))?.clone();

        let num_columns = data_property.num_columns()
            .map_err(prepend("data:"))?;

        let null_values = public_arguments.get("null_value")
            .ok_or_else(|| Error::from("null: missing, must be public"))?.array()?;

        data_property.assert_is_not_aggregated()?;
        if data_property.data_type != DataType::F64 && data_property.data_type != DataType::I64 {
            return Err("data: atomic type must be numeric".into())
        }

        public_arguments.get("edges")
            .ok_or_else(|| Error::from("edges: missing, must be public"))
            .and_then(|v| v.jagged())
            .and_then(|v| match (v, null_values) {
                (Jagged::F64(jagged), Array::F64(null)) => {
                    let null = standardize_null_target_argument(null, &num_columns)?;
                    let edges = standardize_float_argument(jagged, &num_columns)?;

                    if null.iter().any(|v| !v.is_finite()) {
                        return Err("the replacement for null values must be finite".into())
                    }
                    data_property.nature = Some(Nature::Categorical(NatureCategorical {
                        categories: Jagged::F64(edges.into_iter().zip(null.into_iter())
                            .map(|(mut col, null)| {
                                // mandate that edges be sorted
                                if !col.windows(2).all(|w| w[0] <= w[1]) {
                                    return Err("edges must be sorted".into());
                                }
                                col = nature_from_edges(&self.side, col)?;
                                col.push(null);

                                Ok(Some(deduplicate(col.into_iter().map(n64).collect())
                                    .into_iter().map(|v| v.raw()).collect()))
                            }).collect::<Result<_>>()?),
                    }));
                    Ok(())
                }
                (Jagged::I64(jagged), Array::I64(null)) => {
                    let null = standardize_null_target_argument(null, &num_columns)?;
                    let edges = standardize_categorical_argument(jagged, &num_columns)?;
                    data_property.nature = Some(Nature::Categorical(NatureCategorical {
                        categories: Jagged::I64(edges.into_iter().zip(null.into_iter())
                            .map(|(mut col, null)| {
                                // mandate that edges be sorted
                                if !col.windows(2).all(|w| w[0] <= w[1]) {
                                    return Err("edges must be sorted".into());
                                }
                                col = nature_from_edges(&self.side, col)?;
                                col.push(null);

                                Ok(Some(deduplicate(col)))
                            }).collect::<Result<_>>()?),
                    }));
                    Ok(())
                }
                _ => Err("edges: must be numeric".into())
            })?;

        Ok(data_property.into())
    }
}


fn nature_from_edges<T: Clone + Sum + Div<Output=T> + From<i32>>(side: &str, mut edges: Vec<T>) -> Result<Vec<T>> {
    Ok(match side {
        "lower" => {
            edges.pop();
            edges
        },
        "midpoint" => edges.windows(2)
            .map(|slice| slice.iter().cloned().sum::<T>() / T::from(2)).collect(),
        "upper" => {
            edges.remove(0);
            edges
        },
        _ => bail!("side: must be lower, midpoint or upper")
    })
}