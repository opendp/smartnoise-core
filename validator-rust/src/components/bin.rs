use crate::errors::*;

use std::collections::HashMap;
use crate::base::{Nature, Vector1DNull, NodeProperties, ArrayND, get_constant, NatureCategorical, standardize_categorical_argument, Vector2DJagged};

use crate::{proto, base};

use crate::components::{Component, Expandable};

use ndarray::Array;
use crate::base::{Value, Properties, NatureContinuous};
use itertools::Itertools;


impl Component for proto::Bin {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<Properties> {
        let mut data_property = properties.get("data").ok_or("data missing from Bin")?.clone();

        let num_columns = data_property.num_columns
            .ok_or("number of data columns must be known to check imputation")?;

        public_arguments.get("null")
            .ok_or::<Error>("null must be passed into the binning function".into())?;
        let edges = public_arguments.get("edges")
            .ok_or::<Error>("edges must be passed into the binning function".into())?.get_jagged()?.get_f64_option()?;
        let mut edges = standardize_categorical_argument(edges, &num_columns)?;

        let edges: Vec<Vec<f64>> = match self.side.as_str() {
            "left" => edges.iter_mut().map(|col| {col.pop(); col.clone()}).collect(),
            "center" => edges.iter().map(|col|
                col.windows(2).map(|slice| slice.iter().sum::<f64>() / 2.).collect())
                .collect(),
            "right" => edges.iter_mut().map(|col| {col.remove(0); col.clone()}).collect(),
            _ => return Err("bin side must be left, center or right".into())
        };

        // save revised bounds
        data_property.nature = Some(Nature::Categorical(NatureCategorical {
            categories: Vector2DJagged::F64(edges.iter().map(|col| Some(col.clone())).collect()),
        }));

        Ok(data_property)
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}
