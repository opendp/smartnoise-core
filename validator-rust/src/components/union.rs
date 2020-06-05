use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base, Warnable};

use crate::components::{Component, Sensitivity};
use crate::base::{Value, ValueProperties, ArrayProperties, AggregatorProperties, NodeProperties, SensitivitySpace};
use crate::utilities::{get_common_value, prepend};

use itertools::Itertools;
use ndarray::{ArrayViewD, Axis, stack};
// given a partitional indexmap, output the concatenation of all partitions

impl Component for proto::Union {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: &IndexMap<base::IndexKey, Value>,
        properties: &base::NodeProperties,
        node_id: u32,
    ) -> Result<Warnable<ValueProperties>> {

        let data_property = properties.get("data")
            .ok_or("data: missing")?.indexmap()
            .map_err(prepend("data:"))?.clone();

        // all partitions must be arrays
        let array_props = data_property.properties.values().iter()
            .map(|v| v.array()).collect::<Result<Vec<&ArrayProperties>>>()?;

        let num_columns = get_common_value(&array_props.iter()
            .map(|v| Some(v.num_columns)).collect())
            .unwrap_or(None).ok_or_else(|| "num_columns must be known when unioning")?;

        Ok(ValueProperties::Array(ArrayProperties {
            num_records: data_property.num_records,
            num_columns,
            nullity: get_common_value(&array_props.iter().map(|v| v.nullity).collect())
                .unwrap_or(true),
            releasable: get_common_value(&array_props.iter().map(|v| v.releasable).collect())
                .unwrap_or(false),
            // TODO: inflate this by group_id
            c_stability: array_props.iter().map(|v| v.c_stability.clone())
                .fold1(|l, r| l.iter().zip(r).map(|(l, r)| l.max(r)).collect::<Vec<f64>>())
                .ok_or_else(|| "must have at least one partition when merging")?,
            aggregator: Some(AggregatorProperties {
                component: proto::component::Variant::Union(self.clone()),
                properties: properties.clone(),
                // TODO: bring forth the constants from the parts
                c_stability: vec![],
                lipschitz_constant: vec![]
            }),
            // TODO: merge natures
            nature: None,
            data_type: get_common_value(&array_props.iter().map(|v| v.data_type.clone()).collect())
                .ok_or_else(|| "data_types must be equivalent when merging")?,
            dataset_id: Some(node_id as i64),
            is_not_empty: array_props.iter().any(|v| v.is_not_empty),
            dimensionality: Some(2),
        }).into())
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