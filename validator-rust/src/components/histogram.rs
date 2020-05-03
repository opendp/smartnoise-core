use crate::errors::*;

use std::collections::HashMap;

use crate::{proto};

use crate::components::{Component, Sensitivity, Expandable};
use crate::base::{Value, NodeProperties, AggregatorProperties, SensitivitySpace, ValueProperties, DataType, NatureContinuous, Nature, Vector1DNull, Jagged};
use crate::utilities::{prepend, get_literal};
use ndarray::{arr1, Array};
use crate::hashmap;


impl Component for proto::Histogram {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
        _node_id: u32
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        // this check is already guaranteed by the state space, but still included for safety
        if data_property.data_type == DataType::Unknown {
            return Err("data_type must be known".into())
        }

        let categories = data_property.categories()?;

        if categories.num_columns() != 1 {
            return Err("data must contain one column".into())
        }
        data_property.num_records = Some(categories.lengths()[0] as i64);
        let num_columns = data_property.num_columns()?;

        // save a snapshot of the state when aggregating
        data_property.aggregator = Some(AggregatorProperties {
            component: proto::component::Variant::Histogram(self.clone()),
            properties: properties.clone(),
            lipschitz_constant: (0..num_columns).map(|_| 1.).collect()
        });

        data_property.nature = Some(Nature::Continuous(NatureContinuous {
            lower: Vector1DNull::I64((0..num_columns).map(|_| Some(0)).collect()),
            upper: Vector1DNull::I64((0..num_columns).map(|_| None).collect()),
        }));
        data_property.data_type = DataType::I64;

        Ok(data_property.into())
    }
}


impl Expandable for proto::Histogram {
    /// If min and max are not supplied, but are known statically, then add them automatically
    /// Add nodes for clamp or digitize if categories or edges are passed
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        properties: &NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut current_id = *maximum_id;
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();
        let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();

        let data_id = component.arguments.get("data")
            .ok_or_else(|| Error::from("data is a required argument to Histogram"))?.to_owned();

        let mut component = component.clone();

        let mut traversal = Vec::<u32>::new();
        match (component.arguments.get("edges"), component.arguments.get("categories")) {

            (Some(edges_id), None) => {
                // digitize
                let mut arguments = hashmap![
                    "data".to_owned() => data_id,
                    "edges".to_owned() => *edges_id
                ];

                component.arguments.get("null_value")
                    .map(|v| arguments.insert("null_value".to_string(), *v));
                component.arguments.get("inclusive_left")
                    .map(|v| arguments.insert("inclusive_left".to_string(), *v));

                current_id += 1;
                let id_digitize = current_id;
                computation_graph.insert(id_digitize, proto::Component {
                    arguments,
                    variant: Some(proto::component::Variant::Digitize(proto::Digitize {})),
                    omit: true,
                    batch: component.batch,
                });
                component.arguments = hashmap!["data".to_string() => id_digitize];
                traversal.push(id_digitize);
            }

            (None, Some(categories_id)) => {
                // clamp
                let null_id = component.arguments.get("null_value")
                    .ok_or_else(|| Error::from("null_value is a required argument to Histogram when categories are not known"))?;
                current_id += 1;
                let id_clamp = current_id;
                computation_graph.insert(id_clamp, proto::Component {
                    arguments: hashmap![
                        "data".to_owned() => data_id,
                        "categories".to_owned() => *categories_id,
                        "null_value".to_owned() => *null_id
                    ],
                    variant: Some(proto::component::Variant::Clamp(proto::Clamp {})),
                    omit: true,
                    batch: component.batch,
                });
                component.arguments = hashmap!["data".to_string() => id_clamp];
                traversal.push(id_clamp);
            }

            (None, None) => {
                let data_property = properties.get("data")
                    .ok_or("data: missing")?.array()
                    .map_err(prepend("data:"))?.clone();

                if data_property.categories().is_err() {
                    return Err("either edges or categories must be supplied".into())
                }

                current_id += 1;
                let id_categories = current_id;
                let categories = properties.get("data").ok_or("data: missing")?.array()?.categories()?;
                let value = match categories {
                    Jagged::I64(jagged) => arr1(&jagged[0]).into_dyn().into(),
                    Jagged::F64(jagged) => arr1(&jagged[0]).into_dyn().into(),
                    Jagged::Bool(jagged) => arr1(&jagged[0]).into_dyn().into(),
                    Jagged::Str(jagged) => arr1(&jagged[0]).into_dyn().into(),
                };
                let (patch_node, categories_release) = get_literal(value, &component.batch)?;
                computation_graph.insert(id_categories.clone(), patch_node);
                releases.insert(id_categories.clone(), categories_release);

                component.arguments.insert("categories".to_string(), id_categories);
            }

            (Some(_), Some(_)) => return Err("either edges or categories must be supplied".into())
        }

        computation_graph.insert(component_id.clone(), component);

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases,
            traversal
        })
    }
}


impl Sensitivity for proto::Histogram {
    /// Histogram sensitivities [are backed by the the proofs here](https://github.com/opendifferentialprivacy/whitenoise-core/blob/955703e3d80405d175c8f4642597ccdf2c00332a/whitepapers/sensitivities/counts/counts.pdf).
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &SensitivitySpace
    ) -> Result<Value> {
        let data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        data_property.assert_is_not_aggregated()?;

        match sensitivity_type {
            SensitivitySpace::KNorm(_k) => {
                // k has no effect on the sensitivity, and is ignored

                use proto::privacy_definition::Neighboring;
                use proto::privacy_definition::Neighboring::{Substitute, AddRemove};
                let neighboring_type = Neighboring::from_i32(privacy_definition.neighboring)
                    .ok_or_else(|| Error::from("neighboring definition must be either \"AddRemove\" or \"Substitute\""))?;

                // when categories are defined, a disjoint group by query is performed
                let categories_length = data_property.categories()?.lengths()[0];

                let num_records = data_property.num_records;

                // SENSITIVITY DERIVATIONS
                let sensitivity: f64 = match (neighboring_type, categories_length, num_records) {
                    // one category, known N. Applies to any neighboring type.
                    (_, 1, Some(_)) => 0.,

                    // one category, unknown N. The sensitivity here is really zero-- artificially raised
                    (Substitute, 1, None) => 1.,
                    // two categories, known N. Knowing N determines the second category
                    (Substitute, 2, Some(_)) => 1.,

                    // one category, unknown N
                    (AddRemove, 1, None) => 1.,
                    // two categories, known N
                    (AddRemove, 2, Some(_)) => 1.,

                    // over two categories, N either known or unknown. Record may switch from one bin to another.
                    (Substitute, _, _) => 2.,
                    // over two categories, N either known or unknown. Only one bin may be edited.
                    (AddRemove, _, _) => 1.,
                };

                // epsilon is distributed evenly over all cells.
                let epsilon_corrected = sensitivity / categories_length as f64;

                let num_columns = data_property.num_columns()?;
                let num_records = categories_length;
                Ok(Array::from_shape_vec(
                    vec![num_records as usize, num_columns as usize],
                    (0..(num_records * num_columns)).map(|_| epsilon_corrected).collect())?.into())
            },
            _ => Err("Histogram sensitivity is only implemented for KNorm".into())
        }
    }
}
