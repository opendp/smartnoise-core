use crate::errors::*;

use std::collections::HashMap;
use crate::base::{Nature, Vector1DNull, NodeProperties, ArrayND, NatureCategorical, standardize_categorical_argument, Vector2DJagged, standardize_numeric_argument, Vector1D, ArrayNDProperties, ValueProperties, prepend, DataType, get_literal};

use crate::{proto, base};

use crate::components::{Component, Expandable};

use ndarray::Array;
use crate::base::{Value, NatureContinuous};
use itertools::Itertools;
use std::ops::{Add, Div};

impl Component for proto::Add {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut left_property = properties.get("left")
            .ok_or("left: missing")?.get_arraynd()
            .map_err(prepend("left:"))?.clone();
        let mut right_property = properties.get("right")
            .ok_or("right: missing")?.get_arraynd()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayNDProperties {
            nullity: left_property.nullity || right_property.nullity,
            releasable: left_property.releasable && right_property.releasable,
            nature: propagate_binary_nature(&left_property, &right_property, &Operators {
                f64: Some(Box::new(|l: &f64, r: &f64| l + r)),
                i64: Some(Box::new(|l: &i64, r: &i64| l + r)),
                str: None, bool: None
            }, &num_columns)?,
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records: Some(num_records),
            aggregator: None,
            data_type: left_property.data_type
        }.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}


impl Component for proto::Subtract {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut left_property = properties.get("left")
            .ok_or("left: missing")?.get_arraynd()
            .map_err(prepend("left:"))?.clone();
        let mut right_property = properties.get("right")
            .ok_or("right: missing")?.get_arraynd()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayNDProperties {
            nullity: left_property.nullity || right_property.nullity,
            releasable: left_property.releasable && right_property.releasable,
            nature: propagate_binary_nature(&left_property, &right_property, &Operators {
                f64: Some(Box::new(|l: &f64, r: &f64| l - r)),
                i64: Some(Box::new(|l: &i64, r: &i64| l - r)),
                str: None, bool: None
            }, &num_columns)?,
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records: Some(num_records),
            aggregator: None,
            data_type: left_property.data_type
        }.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}


impl Component for proto::Divide {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut left_property = properties.get("left")
            .ok_or("left: missing")?.get_arraynd()
            .map_err(prepend("left:"))?.clone();
        let mut right_property = properties.get("right")
            .ok_or("right: missing")?.get_arraynd()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;
        let float_denominator_may_span_zero = match right_property.clone().nature {
            Some(nature) => match nature {
                Nature::Continuous(nature) => nature.min.get_f64()
                    .map(|min| nature.max.get_f64()
                        .map(|max| min.iter().zip(max.iter())
                            .any(|(min, max)| min
                                .map(|min| max
                                    .map(|max| min < 0. && max > 0.)
                                    // if max is not known
                                    .unwrap_or(min > 0.))
                                // if min is not known
                                .unwrap_or(max.map(|max| max < 0.)
                                    .unwrap_or(true))))
                        // if max is not float
                        .unwrap_or(false))
                    // if min is not float
                    .unwrap_or(false),
                Nature::Categorical(nature) => nature.categories.get_f64()
                    .map(|categories| categories.iter()
                        .any(|column| column.iter()
                            .any(|category| category.is_nan() || category == &0.)))
                    // if categories are not known, a category could be zero or NAN
                    .unwrap_or(false)
            },
            // if nature is not known, data could span zero
            _ => true
        };

        Ok(ArrayNDProperties {
            nullity: left_property.nullity || right_property.nullity || float_denominator_may_span_zero,
            releasable: left_property.releasable && right_property.releasable,
            nature: sort_bounds(propagate_binary_nature(&left_property, &right_property, &Operators {
                f64: Some(Box::new(|l: &f64, r: &f64| l / r)),
                i64: Some(Box::new(|l: &i64, r: &i64| l / r)),
                str: None, bool: None
            }, &num_columns)?, &left_property.data_type)?,
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records: Some(num_records),
            aggregator: None,
           data_type: left_property.data_type
        }.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}


impl Component for proto::Multiply {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut left_property = properties.get("left")
            .ok_or("left: missing")?.get_arraynd()
            .map_err(prepend("left:"))?.clone();
        let mut right_property = properties.get("right")
            .ok_or("right: missing")?.get_arraynd()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayNDProperties {
            nullity: left_property.nullity || right_property.nullity,
            releasable: left_property.releasable && right_property.releasable,
            nature: sort_bounds(propagate_binary_nature(&left_property, &right_property, &Operators {
                f64: Some(Box::new(|l: &f64, r: &f64| l * r)),
                i64: Some(Box::new(|l: &i64, r: &i64| l * r)),
                str: None, bool: None
            }, &num_columns)?, &left_property.data_type)?,
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            data_type: left_property.data_type,
           num_records: Some(num_records),
            aggregator: None
        }.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}

impl Component for proto::Power {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut left_property = properties.get("left")
            .ok_or("left: missing")?.get_arraynd()
            .map_err(prepend("left:"))?.clone();
        let mut right_property = properties.get("right")
            .ok_or("right: missing")?.get_arraynd()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayNDProperties {
            nullity: left_property.nullity || right_property.nullity,
            releasable: left_property.releasable && right_property.releasable,
            // raising data to a power is not monotonic
            // while it is easy to derive conservative bounds (minimize/maximize a single term polynomial on an interval), for now just drop bounds
            nature: None,
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records: Some(num_records),
            data_type: left_property.data_type,
            aggregator: None
        }.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}

impl Component for proto::Log {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut left_property = properties.get("left")
            .ok_or("left: missing")?.get_arraynd()
            .map_err(prepend("left:"))?.clone();
        let mut right_property = properties.get("right")
            .ok_or("right: missing")?.get_arraynd()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayNDProperties {
            nullity: left_property.nullity || right_property.nullity,
            releasable: left_property.releasable && right_property.releasable,
            nature: propagate_binary_nature(&left_property, &right_property, &Operators {
                f64: Some(Box::new(|l: &f64, r: &f64| l.log(*r))),
                i64: None,
                str: None,
                bool: None,
            }, &num_columns)?,
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records: Some(num_records),
            data_type: left_property.data_type,
            aggregator: None,
        }.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}



impl Component for proto::Negative {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.get_arraynd()
            .map_err(prepend("data:"))?.clone();

        if let Some(nature) = data_property.nature.clone() {
            data_property.nature = match nature {
                Nature::Continuous(nature) => Some(Nature::Continuous(NatureContinuous {
                    min: match nature.max {
                        Vector1DNull::F64(max) => Vector1DNull::F64(max.iter().map(|v| match v {Some(v) => Some(-v), None => None}).collect()),
                        Vector1DNull::I64(max) => Vector1DNull::I64(max.iter().map(|v| match v {Some(v) => Some(-v), None => None}).collect()),
                        _ => return Err("nature min/max bounds must be numeric".into())
                    },
                    max: match nature.min {
                        Vector1DNull::F64(min) => Vector1DNull::F64(min.iter().map(|v| match v {Some(v) => Some(-v), None => None}).collect()),
                        Vector1DNull::I64(min) => Vector1DNull::I64(min.iter().map(|v| match v {Some(v) => Some(-v), None => None}).collect()),
                        _ => return Err("nature min/max bounds must be numeric".into())
                    },
                })),
                _ => return Err("negation propagation is not implemented for categorical nature".into())
            }
        }
        Ok(data_property.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}

impl Component for proto::Modulo {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut left_property = properties.get("left")
            .ok_or("left: missing")?.get_arraynd()
            .map_err(prepend("left:"))?.clone();
        let mut right_property = properties.get("right")
            .ok_or("right: missing")?.get_arraynd()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        let maximum = right_property.nature.and_then(|nature| match nature {
            Nature::Continuous(continuous) => Some(continuous.max),
            _ => None
        });

        Ok(ArrayNDProperties {
            nullity: true,
            releasable: left_property.releasable && right_property.releasable,
            nature: maximum.and_then(|maximum| Some(Nature::Continuous(NatureContinuous {
                min: match maximum {
                    Vector1DNull::F64(_) => Vector1DNull::F64((0..num_columns).map(|_| Some(0.)).collect()),
                    Vector1DNull::I64(_) => Vector1DNull::I64((0..num_columns).map(|_| Some(0)).collect()),
                    _ => return None
                },
                max: maximum
            }))),
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records: Some(num_records),
            data_type: left_property.data_type,
            aggregator: None
        }.into())
    }
    fn get_names(
        &self,
        _properties: &base::NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}

impl Expandable for proto::Modulo {
    /// If min and max are not supplied, but are known statically, then add them automatically
    fn expand_component(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: u32,
        maximum_id: u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut current_id = maximum_id;
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();
        let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();

        let mut component = component.clone();

        if !properties.contains_key("min") {
            current_id += 1;
            let id_min = current_id.clone();
            let value = Value::ArrayND(ArrayND::F64(
                Array::from(properties.get("data").unwrap().to_owned().get_arraynd()?.get_min_f64()?).into_dyn()));
            let (patch_node, release) = get_literal(&value, &component.batch)?;
            computation_graph.insert(id_min.clone(), patch_node);
            releases.insert(id_min.clone(), release);
            component.arguments.insert("min".to_string(), id_min);
        }

        if !properties.contains_key("max") {
            current_id += 1;
            let id_max = current_id.clone();
            let value = Value::ArrayND(ArrayND::F64(
                Array::from(properties.get("data").unwrap().to_owned().get_arraynd()?.get_max_f64()?).into_dyn()));
            let (patch_node, release) = get_literal(&value, &component.batch)?;
            computation_graph.insert(id_max.clone(), patch_node);
            releases.insert(id_max.clone(), release);
            component.arguments.insert("max".to_string(), id_max);
        }

        computation_graph.insert(component_id, component);

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases,
            traversal: Vec::new()
        })
    }
}


impl Component for proto::And {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut left_property = properties.get("left")
            .ok_or("left: missing")?.get_arraynd()
            .map_err(prepend("left:"))?.clone();
        let mut right_property = properties.get("right")
            .ok_or("right: missing")?.get_arraynd()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayNDProperties {
            nullity: false,
            releasable: left_property.releasable && right_property.releasable,
            nature: Some(Nature::Categorical(NatureCategorical {
                categories: Vector2DJagged::Bool((0..num_columns).map(|_| Some(vec![true, false])).collect())
            })),
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records: Some(num_records),
            aggregator: None,
            data_type: left_property.data_type
        }.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}


impl Component for proto::Or {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut left_property = properties.get("left")
            .ok_or("left: missing")?.get_arraynd()
            .map_err(prepend("left:"))?.clone();
        let mut right_property = properties.get("right")
            .ok_or("right: missing")?.get_arraynd()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayNDProperties {
            nullity: false,
            releasable: left_property.releasable && right_property.releasable,
            nature: Some(Nature::Categorical(NatureCategorical {
                categories: Vector2DJagged::Bool((0..num_columns).map(|_| Some(vec![true, false])).collect())
            })),
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records: Some(num_records),
            aggregator: None,
            data_type: left_property.data_type
        }.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}


impl Component for proto::Negate {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut left_property = properties.get("left")
            .ok_or("left: missing")?.get_arraynd()
            .map_err(prepend("left:"))?.clone();
        let mut right_property = properties.get("right")
            .ok_or("right: missing")?.get_arraynd()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayNDProperties {
            nullity: false,
            releasable: left_property.releasable && right_property.releasable,
            nature: Some(Nature::Categorical(NatureCategorical {
                categories: Vector2DJagged::Bool((0..num_columns).map(|_| Some(vec![true, false])).collect())
            })),
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records: Some(num_records),
            aggregator: None,
            data_type: left_property.data_type
        }.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}


impl Component for proto::Equal {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut left_property = properties.get("left")
            .ok_or("left: missing")?.get_arraynd()
            .map_err(prepend("left:"))?.clone();
        let mut right_property = properties.get("right")
            .ok_or("right: missing")?.get_arraynd()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayNDProperties {
            nullity: false,
            releasable: left_property.releasable && right_property.releasable,
            nature: Some(Nature::Categorical(NatureCategorical {
                categories: Vector2DJagged::Bool((0..num_columns).map(|_| Some(vec![true, false])).collect())
            })),
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records: Some(num_records),
            aggregator: None,
            data_type: left_property.data_type
        }.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}


impl Component for proto::LessThan {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut left_property = properties.get("left")
            .ok_or("left: missing")?.get_arraynd()
            .map_err(prepend("left:"))?.clone();
        let mut right_property = properties.get("right")
            .ok_or("right: missing")?.get_arraynd()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayNDProperties {
            nullity: false,
            releasable: left_property.releasable && right_property.releasable,
            nature: Some(Nature::Categorical(NatureCategorical {
                categories: Vector2DJagged::Bool((0..num_columns).map(|_| Some(vec![true, false])).collect())
            })),
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records: Some(num_records),
            aggregator: None,
            data_type: left_property.data_type
        }.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}


impl Component for proto::GreaterThan {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut left_property = properties.get("left")
            .ok_or("left: missing")?.get_arraynd()
            .map_err(prepend("left:"))?.clone();
        let mut right_property = properties.get("right")
            .ok_or("right: missing")?.get_arraynd()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayNDProperties {
            nullity: false,
            releasable: left_property.releasable && right_property.releasable,
            nature: Some(Nature::Categorical(NatureCategorical {
                categories: Vector2DJagged::Bool((0..num_columns).map(|_| Some(vec![true, false])).collect())
            })),
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records: Some(num_records),
            aggregator: None,
            data_type: left_property.data_type
        }.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}

pub struct Operators {
    pub f64: Option<Box<dyn Fn(&f64, &f64) -> f64>>,
    pub i64: Option<Box<dyn Fn(&i64, &i64) -> i64>>,
    pub str: Option<Box<dyn Fn(&String, &String) -> String>>,
    pub bool: Option<Box<dyn Fn(&bool, &bool) -> bool>>,
}

pub fn propagate_binary_shape(left_property: &ArrayNDProperties, right_property: &ArrayNDProperties) -> Result<(i64, i64)> {

    let left_num_columns = left_property.get_num_columns()?;
    let right_num_columns = right_property.get_num_columns()?;

    let left_is_column_broadcastable = left_property.releasable && left_num_columns == 1;
    let right_is_column_broadcastable = right_property.releasable && right_num_columns == 1;

    if !(left_is_column_broadcastable || right_is_column_broadcastable) && left_num_columns != right_num_columns {
        return Err("number of columns must be the same for left and right arguments".into());
    }

    let output_num_columns = left_num_columns.max(right_num_columns);

    // n must be known to prevent conformability attacks
    let mut left_num_records = left_property.get_num_records()?;
    let mut right_num_records = right_property.get_num_records()?;

    let left_is_row_broadcastable = left_property.releasable && left_num_records == 1;
    let right_is_row_broadcastable = right_property.releasable && right_num_records == 1;

    if !(left_is_row_broadcastable || right_is_row_broadcastable) && !(left_num_records == right_num_records) {
        return Err("number of rows must be the same for left and right arguments".into());
    }

    // either left, right or both are broadcastable, so take the largest
    let output_num_records = left_num_records.max(right_num_records);

    Ok((output_num_columns, output_num_records))
}

pub fn propagate_binary_nature(left_property: &ArrayNDProperties, right_property: &ArrayNDProperties, operator: &Operators, &output_num_columns: &i64) -> Result<Option<Nature>> {
    Ok(match (left_property.nature.clone(), right_property.nature.clone()) {
        (Some(left_nature), Some(right_nature)) => match (left_nature, right_nature) {
            (Nature::Continuous(left_nature), Nature::Continuous(right_nature)) => {

                let min = match (left_nature.min, right_nature.min) {
                    (Vector1DNull::F64(left_min), Vector1DNull::F64(right_min)) =>
                        match &operator.f64 {
                            Some(operator) => Vector1DNull::F64(broadcast(&left_min, &output_num_columns)?.iter()
                                .zip(broadcast(&right_min, &output_num_columns)?)
                                .map(|(l, r)| match (l, r) {
                                    (Some(l), Some(r)) => {
                                        let result = operator(l, &r);
                                        match result.is_finite() {
                                            true => Some(result),
                                            false => None
                                        }
                                    },
                                    _ => None
                                })
                                .collect()),
                            None => return Err("min cannot be propagated for the current data type".into())
                        },
                    (Vector1DNull::I64(left_min), Vector1DNull::I64(right_min)) =>
                        match &operator.i64 {
                            Some(operator) => Vector1DNull::I64(broadcast(&left_min, &output_num_columns)?.iter()
                                .zip(broadcast(&right_min, &output_num_columns)?)
                                .map(|(l, r)| match (l, r) {
                                    (Some(l), Some(r)) => Some(operator(l, &r)),
                                    _ => None
                                })
                                .collect()),
                            None => return Err("min cannot be propagated for the current data type".into())
                        },
                    _ => return Err("cannot propagate continuous bounds of different or non-numeric types".into())
                };

                let max = match (left_nature.max, right_nature.max) {
                    (Vector1DNull::F64(left_max), Vector1DNull::F64(right_max)) =>
                        match &operator.f64 {
                            Some(operator) => Vector1DNull::F64(broadcast(&left_max, &output_num_columns)?.iter()
                                .zip(broadcast(&right_max, &output_num_columns)?)
                                .map(|(l, r)| match (l, r) {
                                    (Some(l), Some(r)) => Some(operator(l, &r)),
                                    _ => None
                                })
                                .collect()),
                            None => return Err("max cannot be propagated for the current data type".into())
                        },
                    (Vector1DNull::I64(left_max), Vector1DNull::I64(right_max)) =>
                        match &operator.i64 {
                            Some(operator) => Vector1DNull::I64(broadcast(&left_max, &output_num_columns)?.iter()
                                .zip(broadcast(&right_max, &output_num_columns)?)
                                .map(|(l, r)| match (l, r) {
                                    (Some(l), Some(r)) => Some(operator(l, &r)),
                                    _ => None
                                })
                                .collect()),
                            None => return Err("max cannot be propagated for the current data type".into())
                        },
                    _ => return Err("cannot propagate continuous bounds of different or non-numeric types".into())
                };

                Some(Nature::Continuous(NatureContinuous { min, max }))
            }
            _ => None
        },
        _ => None
    })
}

fn broadcast<T: Clone>(data: &Vec<T>, length: &i64) -> Result<Vec<T>> {
    if data.len() as i64 == *length {
        return Ok(data.to_owned());
    }

    if data.len() != 1 {
        return Err("could not broadcast vector".into());
    }

    Ok((0..length.clone()).map(|_| data[0].clone()).collect())
}

/// Used for monotonic functions that may be either increasing or decreasing
///
/// A monotonically decreasing function may reverse the bounds. In this case, the min/max just needs to be sorted
fn sort_bounds(mut nature: Option<Nature>, datatype: &DataType) -> Result<Option<Nature>> {
    let nature = match &nature {
        Some(value) => match value {
            Nature::Continuous(continuous) => continuous,
            Nature::Categorical(categorical) => return Ok(nature)
        },
        None => return Ok(nature)
    };

    let min = match datatype {
        DataType::F64 => Vector1DNull::F64(nature.min.get_f64()?
            .into_iter().zip(nature.max.get_f64()?)
            .map(|(min, max)| match (min, max) {
                    (Some(min), Some(max)) => Some(min.min(*max)),
                    _ => *min
                }).collect()),
        DataType::I64 => Vector1DNull::I64(nature.min.get_i64()?
            .into_iter().zip(nature.max.get_i64()?)
            .map(|(min, max)| match (min, max) {
                (Some(min), Some(max)) => Some(*min.min(max)),
                _ => *min
            }).collect()),
        _ => return Err("bounds sorting requires numeric data".into())
    };

    let max = match datatype {
        DataType::F64 => Vector1DNull::F64(nature.min.get_f64()?
            .into_iter().zip(nature.max.get_f64()?)
            .map(|(min, max)| match (min, max) {
                (Some(min), Some(max)) => Some(min.max(*max)),
                _ => *min
            }).collect()),
        DataType::I64 => Vector1DNull::I64(nature.min.get_i64()?
            .into_iter().zip(nature.max.get_i64()?)
            .map(|(min, max)| match (min, max) {
                (Some(min), Some(max)) => Some(*min.max(max)),
                _ => *min
            }).collect()),
        _ => return Err("bounds sorting requires numeric data".into())
    };

    Ok(Some(Nature::Continuous(NatureContinuous {
        min, max
    })))
}