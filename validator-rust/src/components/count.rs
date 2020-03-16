use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Aggregator, sensitivity_propagation_wrapper};
use crate::base::{Value, NodeProperties, AggregatorProperties, Vector2DJagged, standardize_categorical_argument, SensitivityType, ValueProperties, prepend, Sensitivity, ArrayNDProperties, Hashmap, HashmapProperties};
use itertools::Itertools;

impl Component for proto::Count {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.get_arraynd()
            .map_err(prepend("data:"))?.clone();

        // save a snapshot of the state when aggregating
        data_property.aggregator = Some(AggregatorProperties {
            component: proto::component::Variant::from(self.clone()),
            properties: properties.clone()
        });

        data_property.num_records = Some(1);
        data_property.nature = None;

        Ok(data_property.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}

impl Aggregator for proto::Count {
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &SensitivityType
    ) -> Result<Sensitivity> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.clone();

        sensitivity_propagation_wrapper(
            privacy_definition, data_property, sensitivity_type,
            &arraynd_sensitivity,
            &hashmap_sensitivity,
            &aggregate_sensitivity
        )
    }
}

// given properties for a 2d array, return the sensitivities for every element in the resulting row vector
pub fn arraynd_sensitivity(
    _privacy_definition: &proto::PrivacyDefinition,
    sensitivity_type: &SensitivityType,
    data_property: &ArrayNDProperties,
) -> Result<Sensitivity> {

    let num_columns = data_property.get_num_columns()?;

    match sensitivity_type {

        SensitivityType::KNorm(k) => {
            if k != &1 {
                return Err("Count sensitivity is only implemented for KNorm of 1".into())
            }

            let sensitivity = match data_property.get_num_records() {
                // n is publicly known, so noise is not necessary
                Ok(_) => 0.,
                // unknown n. Adding/removing a record may adjust query by at most one
                Err(_) => 1.,
            };

            Ok(vec![(0..num_columns).map(|_| sensitivity.clone()).collect()])
        },
        _ => return Err("Count sensitivity is only implemented for KNorm of 1".into())
    }
}

// given properties for a hashmap of 2d arrays, return sensitivities for every element in the resulting aggregated matrix
pub fn hashmap_sensitivity(
    _privacy_definition: &proto::PrivacyDefinition,
    sensitivity_type: &SensitivityType,
    hashmap_property: &HashmapProperties,
) -> Result<Sensitivity> {

    hashmap_property.assert_is_disjoint()?;
    hashmap_property.assert_is_not_columnar()?;

    let properties = hashmap_property.properties.get_values();
    let categories_length = hashmap_property.clone().properties.get_num_keys();

    let num_columns = properties.iter()
        .map(|prop| prop.get_arraynd().ok()?.num_columns)
        .fold1(|l, r| if l? == r? {l} else {None})
        .ok_or::<Error>("partition is empty".into())?
        .ok_or::<Error>("columns are not of equal length".into())?;

    match sensitivity_type {

        SensitivityType::KNorm(k) => {
            if k != &1 {
                return Err("Count sensitivity is only implemented for KNorm of 1".into())
            }

            let sensitivity = match hashmap_property.disjoint {
                // if n is set, and the number of categories is 2, then sensitivity is 1.
                // Otherwise, sensitivity is 2 (changing one person can alter two bins)
                true => match hashmap_property.get_num_records() {
                    // n is known
                    Ok(_num_records) => if hashmap_property.properties.get_num_keys() <= 2 { 1. } else { 2. },
                    // unknown n
                    Err(_) => 2.
                },
                // data is not necessarily disjoint, so changing any one record may change all bins
                false => hashmap_property.properties.get_num_keys() as f64
            };

            // broadcast sensitivity to all columns
            Ok(vec![(0..num_columns).map(|_| sensitivity.clone()).collect()])
        },
        _ => Err("Count sensitivity is only implemented for KNorm of 1".into())
    }
}

// given properties for an aggregation of (likely partitioned) data, return the sensitivities after aggregating the aggregations
pub fn aggregate_sensitivity(
    _privacy_definition: &proto::PrivacyDefinition,
    sensitivity_type: &SensitivityType,
    aggregator: &AggregatorProperties,
) -> Result<Sensitivity> {
    // roughly, if aggregator.properties.data.disjoint, then 1, else k
    Err("sensitivity aggregation is not implemented for Count".into())
}
