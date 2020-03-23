use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component, Aggregator};
use crate::base::{Value, NodeProperties, AggregatorProperties, Sensitivity, ValueProperties, prepend, DataType};

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
        data_property.num_columns = Some(1);
        data_property.nature = None;
        data_property.data_type = DataType::I64;

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
        sensitivity_type: &Sensitivity
    ) -> Result<Vec<f64>> {

        match sensitivity_type {

            Sensitivity::KNorm(k) => {
                let data_property = properties.get("data")
                    .ok_or("data: missing")?.get_arraynd()
                    .map_err(prepend("data:"))?.clone();

                data_property.assert_is_not_aggregated()?;
                let sensitivity = if data_property.get_num_records().is_ok() {
                    // sensitivity is zero, because changing records has no effect on n after data is resized
                    0.
                } else {
                    use proto::privacy_definition::Neighboring;
                    let neighboring_type = Neighboring::from_i32(privacy_definition.neighboring)
                        .ok_or::<Error>("neighboring definition must be either \"AddRemove\" or \"Substitute\"".into())?;

                    // All cases are intentionally enumerated
                    match neighboring_type {
                        Neighboring::Substitute => match k {
                            1 => 1.,
                            2 => 1.,
                            _ => return Err("Count sensitivity is only implemented for L1 and L2 spaces".into())
                        },
                        Neighboring::AddRemove => match k {
                            1 => 1.,
                            2 => 1.,
                            _ => return Err("Count sensitivity is only implemented for L1 and L2 spaces".into())
                        }
                    }
                };

                Ok(vec![sensitivity])
            },
            _ => return Err("Count sensitivity is only implemented for KNorm".into())
        }
    }
}
