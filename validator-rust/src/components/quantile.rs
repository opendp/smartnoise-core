use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Sensitivity, Utility};
use crate::base::{Value, NodeProperties, AggregatorProperties, SensitivitySpace, ValueProperties, DataType};

use crate::utilities::{prepend, get_literal};
use ndarray::prelude::*;


impl Component for proto::Quantile {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();
        data_property.assert_is_not_aggregated()?;
        data_property.assert_is_not_empty()?;

        // save a snapshot of the state when aggregating
        data_property.aggregator = Some(AggregatorProperties {
            component: proto::component::Variant::Quantile(self.clone()),
            properties: properties.clone(),
        });

        if data_property.data_type != DataType::F64 && data_property.data_type != DataType::I64 {
            return Err("data: atomic type must be numeric".into());
        }

        data_property.num_records = Some(1);
        data_property.nature = None;

        Ok(data_property.into())
    }
}

impl Sensitivity for proto::Quantile {
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &SensitivitySpace,
    ) -> Result<Value> {
        let data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        data_property.assert_is_not_aggregated()?;
        data_property.assert_non_null()?;


        match sensitivity_type {
            SensitivitySpace::KNorm(k) => {
                if k != &1 {
                    return Err("Quantile sensitivity is only implemented for KNorm of 1".into());
                }
                let lower = data_property.lower_f64()?;
                let upper = data_property.upper_f64()?;

                let row_sensitivity = lower.iter().zip(upper.iter())
                    .map(|(min, max)| (max - min))
                    .collect::<Vec<f64>>();

                let mut array_sensitivity = Array::from(row_sensitivity).into_dyn();
                array_sensitivity.insert_axis_inplace(Axis(0));

                Ok(array_sensitivity.into())
            }
            SensitivitySpace::Exponential => {
                let num_columns = data_property.num_columns()?;

                let neighboring_type = Neighboring::from_i32(privacy_definition.neighboring)
                    .ok_or_else(|| Error::from("neighboring definition must be either \"AddRemove\" or \"Substitute\""))?;
                use proto::privacy_definition::Neighboring;
                let cell_sensitivity = match neighboring_type {
                    Neighboring::AddRemove => self.alpha.max(1. - self.alpha),
                    Neighboring::Substitute => 1.
                };
                let row_sensitivity = (0..num_columns).map(|_| cell_sensitivity).collect::<Vec<f64>>();
                let mut array_sensitivity = Array::from(row_sensitivity).into_dyn();
                array_sensitivity.insert_axis_inplace(Axis(0));

                Ok(array_sensitivity.into())
            }
            _ => Err("Quantile sensitivity is not implemented for the specified sensitivity type".into())
        }
    }
}

impl Utility for proto::Quantile {
    fn get_utility(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
    ) -> Result<proto::Utility> {
        let mut computation_graph = HashMap::new();
        let mut releases = HashMap::new();
        let candidate_id = 0;
        let mut output_id = 0;

        computation_graph.insert(output_id, proto::Component {
            arguments: HashMap::new(),
            variant: Some(proto::component::Variant::Literal(proto::Literal {})),
            omit: true,
            batch: 0,
        });
        output_id += 1;

        let (patch_node, release) = get_literal(&arr0(2.).into_dyn().into(), &0)?;
        computation_graph.insert(output_id, patch_node);
        releases.insert(output_id, release);

        Ok(proto::Utility {
            computation_graph,
            releases,
            candidate_id,
            output_id,
        })
    }
}