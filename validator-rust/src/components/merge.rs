use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base, Warnable};

use crate::components::{Component, Sensitivity};
use crate::base::{Value, ValueProperties, ArrayProperties, AggregatorProperties, NodeProperties, SensitivitySpace, IndexmapProperties};
use crate::utilities::{get_common_value, prepend};

use itertools::Itertools;
use ndarray::{ArrayViewD, Axis, stack};


impl Component for proto::Merge {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: &IndexMap<base::IndexKey, Value>,
        properties: &base::NodeProperties,
        node_id: u32,
    ) -> Result<Warnable<ValueProperties>> {
        Ok(Warnable::new(ValueProperties::Indexmap(IndexmapProperties {
            num_records: None,
            disjoint: false,
            properties: properties.clone(),
            dataset_id: None,
            variant: Variant::Dataframe
        })))
    }
}

impl Sensitivity for proto::Merge {
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &SensitivitySpace
    ) -> Result<Value> {

        let data_property = properties.get("data")
            .ok_or("data: missing")?.indexmap()?;

        match sensitivity_type {
            SensitivitySpace::KNorm(_k) => {
                let partition_sensitivities = data_property.properties.values().iter()
                    .map(|v| v.array()?
                    .aggregator.as_ref().ok_or_else(|| "partitions must be aggregated to have sensitivity")?
                    .component.compute_sensitivity(privacy_definition, properties, sensitivity_type))
                    .collect::<Result<Vec<Value>>>()?;
                Ok(stack(Axis(0), &partition_sensitivities.iter()
                    .map(|v| Ok(v.array()?.f64()?.view()))
                    .collect::<Result<Vec<ArrayViewD<f64>>>>()?)?.into())

                    // to take max sensitivities of each partition
                    // .gencolumns().into_iter()
                    // .map(|column| column.iter().fold(std::f64::NEG_INFINITY, |a, &b| a.max(b)))
                    // .collect::<Vec<f64>>();
            },
            _ => Err("Count sensitivity is only implemented for KNorm".into())
        }
    }
}