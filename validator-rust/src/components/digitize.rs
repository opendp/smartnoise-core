use crate::errors::*;

use std::collections::HashMap;
use crate::base::{Nature, NodeProperties, NatureCategorical, Jagged, ValueProperties, DataType, Array};

use crate::proto;
use crate::utilities::{prepend, standardize_categorical_argument, standardize_null_target_argument, deduplicate, standardize_float_argument};
use crate::components::Component;

use crate::base::Value;

impl Component for proto::Digitize {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or_else(|| Error::from("data: missing"))?.clone().array()
            .map_err(prepend("data:"))?.clone();

        let num_columns = data_property.num_columns()
            .map_err(prepend("data:"))?;

        let null_values = public_arguments.get("null")
            .ok_or_else(|| Error::from("null: missing, must be public"))?.array()?;

        public_arguments.get("edges")
            .ok_or_else(|| Error::from("edges: missing, must be public"))
            .and_then(|v| v.jagged())
            .and_then(|v| match (v, null_values) {
                (Jagged::F64(jagged), Array::I64(null)) => {
                    let null = standardize_null_target_argument(null, &num_columns)?;
                    let edges = standardize_float_argument(jagged, &num_columns)?;
                    data_property.nature = Some(Nature::Categorical(NatureCategorical {
                        categories: Jagged::I64(edges.into_iter().zip(null.into_iter())
                            .map(|(col, null)| {
                                // mandate that edges be sorted
                                if !col.windows(2).all(|w| w[0] <= w[1]) {
                                    return Err("edges must be sorted".into());
                                }

                                let mut categories = (0..(col.len() - 1) as i64).collect::<Vec<i64>>();
                                categories.push(null);
                                Ok(Some(deduplicate(categories)))
                            }).collect::<Result<_>>()?),
                    }));
                    Ok(())
                }
                (Jagged::I64(jagged), Array::I64(null)) => {
                    let null = standardize_null_target_argument(null, &num_columns)?;
                    let edges = standardize_categorical_argument(jagged, &num_columns)?;
                    data_property.nature = Some(Nature::Categorical(NatureCategorical {
                        categories: Jagged::I64(edges.into_iter().zip(null.into_iter())
                            .map(|(col, null)| {
                                if !col.windows(2).all(|w| w[0] <= w[1]) {
                                    return Err("edges must be sorted".into());
                                }

                                let original_length = col.len();
                                if deduplicate(col).len() < original_length {
                                    return Err("edges must not contain duplicates".into())
                                }

                                let mut categories = (0..(original_length - 1) as i64).collect::<Vec<i64>>();
                                categories.push(null);
                                Ok(Some(deduplicate(categories)))
                            }).collect::<Result<_>>()?),
                    }));
                    Ok(())
                }
                _ => Err("edges: must be numeric".into())
            })?;

        data_property.data_type = DataType::I64;
        Ok(data_property.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}
