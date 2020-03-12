use crate::errors::*;

use std::collections::HashMap;
use crate::base::{Nature, Vector1DNull, NodeProperties, ArrayND, get_constant, NatureCategorical, standardize_categorical_argument, Vector2DJagged, ValueProperties, prepend};

use crate::{proto, base};

use crate::components::{Component, Expandable};

use ndarray::Array;
use crate::base::{Value, NatureContinuous};
use itertools::Itertools;

impl Component for proto::Bin {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or::<Error>("data: missing".into())?.clone().get_arraynd()
            .map_err(prepend("data:"))?.clone();

        let num_columns = data_property.get_num_columns()
            .map_err(prepend("data:"))?;

        public_arguments.get("null")
            .ok_or::<Error>("null: missing, must be public".into())?;

        let edges = public_arguments.get("edges")
            .ok_or::<Error>("edges missing, must be public".into())
            .and_then(|v| v.get_jagged())
            .and_then(|v| v.get_f64_option())
            .map_err(prepend("edges:"))?;

        let mut edges = standardize_categorical_argument(edges, &num_columns)
            .map_err(prepend("edges:"))?;

        let edges: Vec<Vec<f64>> = match self.side.as_str() {
            "left" => edges.iter_mut().map(|col| {col.pop(); col.clone()}).collect(),
            "center" => edges.iter().map(|col|
                col.windows(2).map(|slice| slice.iter().sum::<f64>() / 2.).collect())
                .collect(),
            "right" => edges.iter_mut().map(|col| {col.remove(0); col.clone()}).collect(),
            _ => bail!("side: must be left, center or right")
        };

        // save revised bounds
        data_property.nature = Some(Nature::Categorical(NatureCategorical {
            categories: Vector2DJagged::F64(edges.iter().map(|col| Some(col.clone())).collect()),
        }));

        Ok(data_property.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}
