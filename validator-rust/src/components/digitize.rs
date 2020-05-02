use crate::errors::*;

use std::collections::HashMap;
use crate::base::{Nature, NodeProperties, NatureCategorical, Jagged, ValueProperties, DataType, Array};

use crate::{proto, base};
use crate::utilities::{prepend, standardize_categorical_argument, standardize_null_target_argument, deduplicate, standardize_float_argument, get_literal};
use crate::components::{Component, Expandable};

use crate::base::Value;
use ndarray::arr0;

impl Component for proto::Digitize {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
        _node_id: u32
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or_else(|| Error::from("data: missing"))?.clone().array()
            .map_err(prepend("data:"))?.clone();

        if data_property.data_type == DataType::Unknown {
            return Err("data_type must be known".into())
        }

        let num_columns = data_property.num_columns()
            .map_err(prepend("data:"))?;

        let null_value = public_arguments.get("null_value").cloned()
            .unwrap_or_else(|| Value::Array(Array::I64(arr0(-1).into_dyn())));
        let null = null_value.array()?.i64()?;

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        public_arguments.get("edges")
            .ok_or_else(|| Error::from("edges: missing, must be public"))
            .and_then(|v| v.jagged())
            .and_then(|v| match v {
                Jagged::F64(jagged) => {
                    let null = standardize_null_target_argument(null, &num_columns)?;
                    let edges = standardize_float_argument(jagged, &num_columns)?;
                    data_property.nature = Some(Nature::Categorical(NatureCategorical {
                        categories: Jagged::I64(edges.into_iter().zip(null.into_iter())
                            .map(|(col, null)| {
                                // mandate that edges be sorted
                                if !col.windows(2).all(|w| w[0] <= w[1]) {
                                    return Err("edges must be sorted".into());
                                }

                                let mut categories = (0..(col.len() - 1) as i64).collect::<Vec<i64>>();
                                categories.push(null);
                                Ok(deduplicate(categories))
                            }).collect::<Result<_>>()?),
                    }));
                    Ok(())
                }
                Jagged::I64(jagged) => {
                    let null = standardize_null_target_argument(null, &num_columns)?;
                    let edges = standardize_categorical_argument(jagged.clone(), &num_columns)?;
                    data_property.nature = Some(Nature::Categorical(NatureCategorical {
                        categories: Jagged::I64(edges.into_iter().zip(null.into_iter())
                            .map(|(col, null)| {
                                if !col.windows(2).all(|w| w[0] <= w[1]) {
                                    return Err("edges must be sorted".into());
                                }

                                let original_length = col.len();
                                if deduplicate(col).len() < original_length {
                                    return Err("edges must not contain duplicates".into())
                                }

                                let mut categories = (0..(original_length - 1) as i64).collect::<Vec<i64>>();
                                categories.push(null);
                                Ok(deduplicate(categories))
                            }).collect::<Result<_>>()?),
                    }));
                    Ok(())
                }
                _ => Err("edges: must be numeric".into())
            })?;

        data_property.data_type = DataType::I64;
        Ok(data_property.into())
    }
}

impl Expandable for proto::Digitize {
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut current_id = *maximum_id;
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();
        let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();

        let mut component = component.clone();

        if !properties.contains_key("null_value") {
            current_id += 1;
            let id_null_value = current_id;
            let value = Value::Array(Array::I64(arr0(-1).into_dyn()));
            let (patch_node, release) = get_literal(value, &component.batch)?;
            computation_graph.insert(id_null_value.clone(), patch_node);
            releases.insert(id_null_value.clone(), release);
            component.arguments.insert("null_value".to_string(), id_null_value);
        }
        if !properties.contains_key("inclusive_left") {
            current_id += 1;
            let id_null_value = current_id;
            let value = Value::Array(Array::Bool(arr0(true).into_dyn()));
            let (patch_node, release) = get_literal(value, &component.batch)?;
            computation_graph.insert(id_null_value.clone(), patch_node);
            releases.insert(id_null_value.clone(), release);
            component.arguments.insert("inclusive_left".to_string(), id_null_value);
        }

        computation_graph.insert(component_id.clone(), component);

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases,
            traversal: Vec::new()
        })
    }
}