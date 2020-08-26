use crate::errors::*;

use crate::base::{IndexKey, Nature, NodeProperties, NatureCategorical, Jagged, ValueProperties, DataType, Array};

use crate::{proto, base, Warnable, Integer};
use crate::utilities::{prepend, standardize_categorical_argument, standardize_null_target_argument, deduplicate, standardize_float_argument, get_literal};
use crate::components::{Component, Expandable};

use crate::base::Value;
use ndarray::arr0;
use indexmap::map::IndexMap;
use crate::utilities::inference::infer_property;

impl Component for proto::Digitize {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        mut public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let mut data_property = properties.get(&IndexKey::from("data"))
            .ok_or_else(|| Error::from("data: missing"))?.clone().array()
            .map_err(prepend("data:"))?.clone();

        if data_property.data_type == DataType::Unknown {
            return Err("data_type must be known".into())
        }

        let num_columns = data_property.num_columns()
            .map_err(prepend("data:"))? as i64;

        let null_value: base::Value = match public_arguments.get::<IndexKey>(&"null_value".into()) {
            Some(&v) => v.to_owned(),
            None => Value::Array(Array::Int(arr0(-1).into_dyn()))
        };
        let null = null_value.array()?.int()
            .map_err(prepend("null_value:"))?;

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        public_arguments.remove::<IndexKey>(&"edges".into())
            .ok_or_else(|| Error::from("edges: missing, must be public"))
            .and_then(|v| v.clone().jagged())
            .and_then(|v| match v {
                Jagged::Float(edges) => {
                    let null = standardize_null_target_argument(null, num_columns)?;
                    let edges = standardize_float_argument(edges, num_columns)?;
                    data_property.nature = Some(Nature::Categorical(NatureCategorical {
                        categories: Jagged::Int(edges.into_iter().zip(null.into_iter())
                            .map(|(col, null)| {
                                // mandate that edges be sorted
                                if !col.windows(2).all(|w| w[0] <= w[1]) {
                                    return Err("edges must be sorted".into());
                                }

                                let mut categories = (0..(col.len() - 1) as Integer).collect::<Vec<Integer>>();
                                categories.push(null);
                                Ok(deduplicate(categories))
                            }).collect::<Result<_>>()?),
                    }));
                    Ok(())
                }
                Jagged::Int(edges) => {
                    let null = standardize_null_target_argument(null, num_columns)?;
                    let edges = standardize_categorical_argument(edges, num_columns)?;
                    data_property.nature = Some(Nature::Categorical(NatureCategorical {
                        categories: Jagged::Int(edges.into_iter().zip(null.into_iter())
                            .map(|(col, null)| {
                                if !col.windows(2).all(|w| w[0] <= w[1]) {
                                    return Err("edges must be sorted".into());
                                }

                                let original_length = col.len();
                                if deduplicate(col).len() < original_length {
                                    return Err("edges must not contain duplicates".into())
                                }

                                let mut categories = (0..(original_length - 1) as Integer).collect::<Vec<Integer>>();
                                categories.push(null);
                                Ok(deduplicate(categories))
                            }).collect::<Result<_>>()?),
                    }));
                    Ok(())
                }
                _ => Err("edges: must be numeric".into())
            })?;

        data_property.data_type = DataType::Int;
        Ok(ValueProperties::Array(data_property).into())
    }
}

impl Expandable for proto::Digitize {
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        _public_arguments: &IndexMap<IndexKey, &Value>,
        properties: &base::NodeProperties,
        component_id: u32,
        mut maximum_id: u32,
    ) -> Result<base::ComponentExpansion> {
        let mut component = component.clone();

        let mut expansion = base::ComponentExpansion::default();

        if !properties.contains_key(&IndexKey::from("null_value")) {
            maximum_id += 1;
            let id_null_value = maximum_id;
            let value = Value::Array(Array::Int(arr0(-1).into_dyn()));
            expansion.properties.insert(id_null_value, infer_property(&value, None)?);
            let (patch_node, release) = get_literal(value, component.submission)?;
            expansion.computation_graph.insert(id_null_value, patch_node);
            expansion.releases.insert(id_null_value, release);
            component.insert_argument(&"null_value".into(), id_null_value);
        }
        if !properties.contains_key::<IndexKey>(&"inclusive_left".into()) {
            maximum_id += 1;
            let id_inclusive_left = maximum_id;
            let value = Value::Array(Array::Bool(arr0(true).into_dyn()));
            expansion.properties.insert(id_inclusive_left, infer_property(&value, None)?);
            let (patch_node, release) = get_literal(value, component.submission)?;
            expansion.computation_graph.insert(id_inclusive_left, patch_node);
            expansion.releases.insert(id_inclusive_left, release);
            component.insert_argument(&"inclusive_left".into(), id_inclusive_left);
        }

        expansion.computation_graph.insert(component_id, component);

        Ok(expansion)
    }
}