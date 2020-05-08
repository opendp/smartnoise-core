use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Sensitivity};
use crate::base::{Value, ValueProperties, ArrayProperties, AggregatorProperties, NodeProperties, SensitivitySpace};
use crate::utilities::{get_common_value, prepend};

use itertools::Itertools;
use ndarray::{ArrayViewD, Axis, stack, arr1, ArrayD};


impl Component for proto::Merge {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
        node_id: u32,
    ) -> Result<ValueProperties> {

        let data_property = properties.get("data")
            .ok_or("data: missing")?.indexmap()
            .map_err(prepend("data:"))?.clone();

        let num_columns = get_common_value(&data_property.properties.values().iter()
            .map(|v| Some(v.array().ok()?.num_columns)).collect())
            .unwrap_or(None).ok_or_else(|| "num_columns must be known when merging")?;

        // all partitions must be arrays
        // NOTE: if you have a multilayer indexmap/partition, then step down via
        //     Map<Map<Agg>> -> Map<Merge> -> Map<Agg> -> Merge
        let array_props = data_property.properties.values().iter()
            .map(|v| v.array()).collect::<Result<Vec<&ArrayProperties>>>()?;

        Ok(ArrayProperties {
            num_records: data_property.num_records,
            num_columns,
            nullity: get_common_value(&array_props.iter().map(|v| v.nullity).collect())
                .unwrap_or(true),
            releasable: get_common_value(&array_props.iter().map(|v| v.releasable).collect())
                .unwrap_or(false),
            c_stability: array_props.iter().map(|v| v.c_stability.clone())
                .fold1(|l, r| l.iter().zip(r).map(|(l, r)| l.max(r)).collect::<Vec<f64>>())
                .ok_or_else(|| "must have at least one partition when merging")?,
            aggregator: Some(AggregatorProperties {
                component: proto::component::Variant::Merge(self.clone()),
                properties: properties.clone(),
                lipschitz_constant: vec![]
            }),
            // TODO: merge natures
            nature: None,
            data_type: get_common_value(&array_props.iter().map(|v| v.data_type.clone()).collect())
                .ok_or_else(|| "data_types must be equivalent when merging")?,
            dataset_id: Some(node_id as i64),
            is_not_empty: array_props.iter().any(|v| v.is_not_empty),
            dimensionality: Some(2)
        }.into())
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
            SensitivitySpace::KNorm(k) => {
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