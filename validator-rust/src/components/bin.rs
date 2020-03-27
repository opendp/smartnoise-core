use crate::errors::*;

use std::collections::HashMap;
use crate::base::{Nature, NodeProperties, NatureCategorical, Jagged, ValueProperties, DataType, Array};

use crate::proto;
use crate::utilities::{prepend, standardize_categorical_argument, standardize_null_target_argument};
use crate::components::Component;

use crate::base::Value;
use std::iter::Sum;
use std::ops::Div;

impl Component for proto::Bin {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or::<Error>("data: missing".into())?.clone().array()
            .map_err(prepend("data:"))?.clone();

        let num_columns = data_property.num_columns()
            .map_err(prepend("data:"))?;

        let null_values = public_arguments.get("null")
            .ok_or::<Error>("null: missing, must be public".into())?.array()?;

        public_arguments.get("edges")
            .ok_or::<Error>("edges: missing, must be public".into())
            .and_then(|v| v.jagged())
            .and_then(|v| match (v, null_values) {
                (Jagged::F64(jagged), Array::F64(null)) => {
                    let null = standardize_null_target_argument(null, &num_columns)?;
                    let mut edges = standardize_categorical_argument(jagged, &num_columns)?;
                    let edges = nature_from_edges(&self.side, &mut edges)?;
                    data_property.nature = Some(Nature::Categorical(NatureCategorical {
                        categories: Jagged::F64(edges.into_iter().zip(null.into_iter())
                            .map(|(mut col, null)| {
                                col.push(null);
                                Some(col)
                            }).collect()),
                    }));
                    Ok(())
                }
                (Jagged::I64(jagged), Array::I64(null)) => {
                    let null = standardize_null_target_argument(null, &num_columns)?;
                    let mut edges = standardize_categorical_argument(jagged, &num_columns)?;
                    let edges = nature_from_edges(&self.side, &mut edges)?;
                    data_property.nature = Some(Nature::Categorical(NatureCategorical {
                        categories: Jagged::I64(edges.into_iter().zip(null.into_iter())
                            .map(|(mut col, null)| {
                                col.push(null);
                                Some(col)
                            }).collect()),
                    }));
                    Ok(())
                }
                _ => Err("edges: must be numeric".into())
            })?;

//        if self.digitize {
//            data_property.nature = Some(Nature::Categorical(NatureCategorical {
//                categories: match data_property.nature.unwrap().get_categorical()?.categories {
//                    Jagged::I64(edges.into_iter().map(|column|
//                        Some(column.unwrap().into_iter().enumerate()
//                            .map(|(idx, _)| idx as i64).collect()))
//                        .collect())
//                }
//            }))
//        }

        data_property.data_type = DataType::F64;

        Ok(data_property.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}


fn nature_from_edges<T: Clone + Sum + Div<Output=T> + From<i32>>(side: &String, edges: &mut Vec<Vec<T>>) -> Result<Vec<Vec<T>>> {
    Ok(match side.as_str() {
        "lower" => edges.iter_mut().map(|col| {
            col.pop();
            col.clone()
        }).collect(),
        "midpoint" => edges.iter().map(|col|
            col.windows(2).map(|slice| slice.iter().cloned().sum::<T>() / T::from(2)).collect())
            .collect(),
        "upper" => edges.iter_mut().map(|col| {
            col.remove(0);
            col.clone()
        }).collect(),
        _ => bail!("side: must be lower, midpoint or upper")
    })
}