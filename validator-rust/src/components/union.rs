use crate::errors::*;


use crate::{proto, base, Warnable};

use crate::components::{Component, Sensitivity};
use crate::base::{Value, ValueProperties, ArrayProperties, AggregatorProperties, NodeProperties, SensitivitySpace, IndexKey, IndexmapProperties};
use crate::utilities::{get_common_value};

use itertools::Itertools;
use ndarray::{ArrayViewD, Axis, stack};
use indexmap::map::IndexMap;
use crate::utilities::privacy::{get_group_id_path, get_c_stability_multiplier};
// given a partitional indexmap, output the concatenation of all partitions

impl Component for proto::Union {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: &IndexMap<base::IndexKey, &Value>,
        properties: &base::NodeProperties,
        node_id: u32,
    ) -> Result<Warnable<ValueProperties>> {

        Ok(Warnable::new(if self.flatten {
            // all partitions must be arrays
            let array_props = properties.values()
                .map(|v| v.array()).collect::<Result<Vec<&ArrayProperties>>>()?;

            let num_columns = get_common_value(&array_props.iter()
                .map(|v| Some(v.num_columns)).collect())
                .unwrap_or(None).ok_or_else(|| "num_columns must be known when unioning")?;

            let num_records = array_props.iter().fold(Some(0), |sum, v| match (sum, v.num_records) {
                (Some(l), Some(r)) => Some(l + r),
                _ => None
            });

            let releasable = get_common_value(&array_props.iter().map(|v| v.releasable).collect())
                .ok_or_else(|| Error::from("arguments must all be releasable, or all be private"))?;

            let c_stab_mult = get_c_stability_multiplier(
                array_props.iter().map(|prop| prop.group_id.clone()).collect())?;

            ValueProperties::Array(ArrayProperties {
                num_records,
                num_columns,
                nullity: get_common_value(&array_props.iter().map(|v| v.nullity).collect())
                    .unwrap_or(true),
                releasable,
                c_stability: array_props.iter().map(|v| v.c_stability.clone())
                    .fold1(|l, r| l.iter().zip(r).map(|(l, r)| l.max(r) * c_stab_mult).collect::<Vec<f64>>())
                    .ok_or_else(|| "must have at least one partition when unioning")?,
                aggregator: if releasable { None } else {
                    Some(AggregatorProperties {
                        component: proto::component::Variant::Union(self.clone()),
                        properties: properties.clone(),
                        lipschitz_constants: stack(
                            Axis(0),
                            &array_props.iter().map(|prop| prop.aggregator.clone())
                                .collect::<Option<Vec<AggregatorProperties>>>()
                                .ok_or_else(|| Error::from("all arguments to union must be aggregated"))?
                                .iter().map(|v| Ok(v.lipschitz_constants.array()?.f64()?.view()))
                                .collect::<Result<Vec<ArrayViewD<f64>>>>()?)?.into(),
                    })
                },
                // TODO: merge natures
                nature: None,
                data_type: get_common_value(&array_props.iter().map(|v| v.data_type.clone()).collect())
                    .ok_or_else(|| "data_types must be equivalent when merging")?,
                dataset_id: Some(node_id as i64),
                is_not_empty: array_props.iter().any(|v| v.is_not_empty),
                dimensionality: Some(2),
                group_id: get_group_id_path(array_props.iter()
                    .map(|prop| prop.group_id.clone())
                    .collect())?
            })
        } else {
            ValueProperties::Indexmap(IndexmapProperties {
                children: properties.clone(),
                variant: proto::indexmap_properties::Variant::Partition
            })
        }))
    }
}

impl Sensitivity for proto::Union {
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &SensitivitySpace
    ) -> Result<Value> {

        let partition_sensitivities = properties.values()
            .map(|v| {
                let aggregator: &AggregatorProperties = v.array()?
                    .aggregator.as_ref().ok_or_else(|| "partitions must be aggregated to have sensitivity")?;

                aggregator.component
                    .compute_sensitivity(privacy_definition, &aggregator.properties, sensitivity_type)
            })
            .collect::<Result<Vec<Value>>>()?;

        Ok(if self.flatten {
            stack(Axis(0), &partition_sensitivities.iter()
                .map(|v| Ok(v.array()?.f64()?.view()))
                .collect::<Result<Vec<ArrayViewD<f64>>>>()?)?.into()
        } else {
            properties.keys()
                .cloned().zip(partition_sensitivities)
                .collect::<IndexMap<IndexKey, Value>>().into()
        })
    }
}