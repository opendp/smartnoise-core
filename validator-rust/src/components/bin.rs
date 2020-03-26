use crate::errors::*;

use std::collections::HashMap;
use crate::base::{Nature, NodeProperties, NatureCategorical, Vector2DJagged, ValueProperties, DataType};

use crate::{proto};
use crate::utilities::{prepend, standardize_categorical_argument};
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
            .ok_or::<Error>("data: missing".into())?.clone().get_arraynd()
            .map_err(prepend("data:"))?.clone();

        let num_columns = data_property.get_num_columns()
            .map_err(prepend("data:"))?;

        public_arguments.get("null")
            .ok_or::<Error>("null: missing, must be public".into())?;

        public_arguments.get("edges")
            .ok_or::<Error>("edges: missing, must be public".into())
            .and_then(|v| v.get_jagged())
            .and_then(|v| match v {
                    Vector2DJagged::F64(jagged) => {
                        let mut edges = standardize_categorical_argument(jagged, &num_columns)?;
                        let edges = nature_from_edges(&self.side, &mut edges)?;
                        data_property.nature = Some(Nature::Categorical(NatureCategorical {
                            categories: Vector2DJagged::F64(edges.iter().map(|col| Some(col.clone())).collect()),
                        }));
                        Ok(())
                    }
                    Vector2DJagged::I64(jagged) => {
                        let mut edges = standardize_categorical_argument(jagged, &num_columns)?;
                        let edges = nature_from_edges(&self.side, &mut edges)?;
                        data_property.nature = Some(Nature::Categorical(NatureCategorical {
                            categories: Vector2DJagged::I64(edges.iter().map(|col| Some(col.clone())).collect()),
                        }));
                        Ok(())
                    }
                    _ => Err("edges: must be numeric".into())
                })?;

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
        "left" => edges.iter_mut().map(|col| {
            col.pop();
            col.clone()
        }).collect(),
        "center" => edges.iter().map(|col|
            col.windows(2).map(|slice| slice.iter().cloned().sum::<T>() / T::from(2)).collect())
            .collect(),
        "right" => edges.iter_mut().map(|col| {
            col.remove(0);
            col.clone()
        }).collect(),
        _ => bail!("side: must be left, center or right")
    })
}