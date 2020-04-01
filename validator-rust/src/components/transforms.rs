use crate::errors::*;

use std::collections::HashMap;
use crate::base::{Nature, NodeProperties, NatureCategorical, Vector1DNull, Jagged, ArrayProperties, ValueProperties, Array};

use crate::{proto, base};

use crate::utilities::{prepend, get_literal};

use crate::components::{Component, Expandable};

use crate::base::{Value, NatureContinuous};
use ndarray;
use num::{CheckedAdd, CheckedSub};


impl Component for proto::Add {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let left_property = properties.get("left")
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get("right")
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayProperties {
            nullity: left_property.nullity || right_property.nullity,
            releasable: left_property.releasable && right_property.releasable,
            nature: propagate_binary_nature(&left_property, &right_property, &Operators {
                f64: Some(Box::new(|l: &f64, r: &f64|
                    Ok(l + r))),
                i64: Some(Box::new(|l: &i64, r: &i64|
                    l.checked_add(r).ok_or_else(|| Error::from("addition may result in underflow or overflow")))),
                str: None,
                bool: None,
            }, &OptimizeOperators {
                f64: Some(Box::new(|bounds| Ok((
                    bounds.left_min.and_then(|lmin| bounds.right_min.and_then(|rmin|
                        Some(lmin + rmin))),
                    bounds.left_max.and_then(|lmax| bounds.right_max.and_then(|rmax|
                        Some(lmax + rmax))),
                )))),
                i64: Some(Box::new(|bounds| Ok((
                    match (bounds.left_min, bounds.right_min) {
                        (Some(lmin), Some(rmin)) => Some(lmin.checked_add(rmin)
                            .ok_or_else(|| Error::from("addition may result in underflow or overflow"))?),
                        _ => None
                    },
                    match (bounds.left_max, bounds.right_max) {
                        (Some(lmax), Some(rmax)) => Some(lmax.checked_add(rmax)
                            .ok_or_else(|| Error::from("addition may result in underflow or overflow"))?),
                        _ => None
                    }))))
            }, &num_columns)?,
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            aggregator: None,
            data_type: left_property.data_type,
            dataset_id: left_property.dataset_id,
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
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let left_property = properties.get("left")
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get("right")
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayProperties {
            nullity: left_property.nullity || right_property.nullity,
            releasable: left_property.releasable && right_property.releasable,
            nature: propagate_binary_nature(&left_property, &right_property, &Operators {
                f64: Some(Box::new(|l: &f64, r: &f64|
                    Ok(l - r))),
                i64: Some(Box::new(|l: &i64, r: &i64|
                    l.checked_sub(r).ok_or_else(|| Error::from("subtraction may result in underflow or overflow")))),
                str: None,
                bool: None,
            }, &OptimizeOperators {
                f64: Some(Box::new(|bounds| Ok((
                    bounds.left_min.and_then(|lmin| bounds.right_min.and_then(|rmin|
                        Some(lmin - rmin))),
                    bounds.left_max.and_then(|lmax| bounds.right_max.and_then(|rmax|
                        Some(lmax - rmax))),
                )))),
                i64: Some(Box::new(|bounds| Ok((
                    match (bounds.left_min, bounds.right_min) {
                        (Some(lmin), Some(rmin)) => Some(lmin.checked_sub(rmin)
                            .ok_or_else(|| Error::from("subtraction may result in underflow or overflow"))?),
                        _ => None
                    },
                    match (bounds.left_max, bounds.right_max) {
                        (Some(lmax), Some(rmax)) => Some(lmax.checked_sub(rmax)
                            .ok_or_else(|| Error::from("subtraction may result in underflow or overflow"))?),
                        _ => None
                    }))))
            }, &num_columns)?,
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            aggregator: None,
            data_type: left_property.data_type,
            dataset_id: left_property.dataset_id,
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
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let left_property = properties.get("left")
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get("right")
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayProperties {
            nullity: left_property.nullity || right_property.nullity,
            releasable: left_property.releasable && right_property.releasable,
            nature: propagate_binary_nature(&left_property, &right_property, &Operators {
                f64: Some(Box::new(|l: &f64, r: &f64| {
                    let category = l * r;
                    if !category.is_finite() {
                        return Err("multiplication may result in underflow or overflow".into())
                    }
                    Ok(category)
                })),
                i64: Some(Box::new(|l: &i64, r: &i64|
                    l.checked_mul(*r).ok_or_else(|| Error::from("multiplication may result in underflow or overflow")))),
                str: None,
                bool: None,
            }, &OptimizeOperators {
                f64: Some(Box::new(|bounds| {
                    let a = match bounds.left_min {
                        Some(v) => v,
                        None => return Ok((None, None))
                    }.clone();
                    let c = match bounds.left_max {
                        Some(v) => v,
                        None => return Ok((None, None))
                    }.clone();
                    let d = match bounds.right_min {
                        Some(v) => v,
                        None => return Ok((None, None))
                    }.clone();
                    let f = match bounds.right_max {
                        Some(v) => v,
                        None => return Ok((None, None))
                    }.clone();

                    // maximize {b * d | a <= b <= c && d <= e <= f}
                    let max = match (a, c, d, f) {

                        // if either interval is a point
                        (a, c, d, f) if a == c || d == f =>
                            Some(c * f),

                        // if both intervals are not points
                        (a, c, d, f) if (d < 0. && ((c > 0. && ((f == 0. && a < 0.) || (a * d > c * f && f > 0. && d + f >= 0.))) || (a < c && f >= 0. && c <= 0.)))
                            || (a < c && c <= 0. && ((d < f && f < 0.) || (f > 0. && d + f < 0.)))
                            || (c > 0. && ((d < f && f < 0. && a <= 0.) || (f > 0. && d + f < 0. && a * d <= c * f))) =>
                            Some(a * d),
                        (a, c, d, f) if 0. <= d && d < f && c <= 0. && a < c =>
                            Some(c * d),
                        (a, c, d, f) if f < 0. && d < f && 0. < a && a < c =>
                            Some(a * f),
                        (a, c, d, f) if c > 0. && f > 0. && a < c
                            && ((a * d >= c * f && d + f >= 0. && d < 0.) || (d < f && d >= 0.) || (c * f < a * d && d + f < 0.)) =>
                            Some(c * f),

                        // Prior cases should cover all
                        _ => None
                    };

                    // minimize {b * d | a <= b <= c && d <= e <= f}
                    let min = match (a, c, d, f) {
                        // if either interval is a point
                        (a, c, d, f) if a == c || d == f =>
                            Some(a * d),

                        // if both intervals are not points
                        (a, c, d, f) if d > 0. && d < f && a > 0. && a < c =>
                            Some(a * d),
                        (a, c, d, f) if c > 0. && a < c && ((f > 0. && a * f > c * d && d < 0.)
                            || (d < f && f <= 0.)) =>
                            Some(c * d),
                        (a, c, d, f) if f > 0. && ((c > 0. && ((a < 0. && (d == 0. || (d >= 0. && d < f)
                            || (d <= 0. && a * f <= c * d))) || (d < 0. && a * f <= c * d)))
                            || (a < c && c <= 0. && (d < f || d <= 0.))) =>
                            Some(a * f),
                        (a, c, d, f) if f <= 0. && d < f && c <= 0. && a < c =>
                            Some(c * f),

                        // Prior cases should cover all
                        _ => None
                    };
                    Ok((min, max))
                })),
                // multiplicative bounds propagation for ints is not implemented
                i64: None}, &num_columns)?,
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            data_type: left_property.data_type,
            num_records,
            aggregator: None,
            dataset_id: left_property.dataset_id,
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
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let left_property = properties.get("left")
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get("right")
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;
        let float_denominator_may_span_zero = match right_property.clone().nature {
            Some(nature) => match nature {
                Nature::Continuous(nature) => nature.min.f64()
                    .map(|min| nature.max.f64()
                        .map(|max| min.iter().zip(max.iter())
                            .any(|(min, max)| min
                                .map(|min| max
                                    .map(|max| min < 0. && max > 0.)
                                    // if max is not known
                                    .unwrap_or(min > 0.))
                                // if min is not known
                                .unwrap_or_else(|| max.map(|max| max < 0.)
                                    .unwrap_or(true))))
                        // if max is not float
                        .unwrap_or(false))
                    // if min is not float
                    .unwrap_or(false),
                Nature::Categorical(nature) => nature.categories.f64()
                    .map(|categories| categories.iter()
                        .any(|column| column.iter()
                            .any(|category| category.is_nan() || category == &0.)))
                    // if categories are not known, a category could be zero or NAN
                    .unwrap_or(false)
            },
            // if nature is not known, data could span zero
            _ => true
        };

        Ok(ArrayProperties {
            nullity: left_property.nullity || right_property.nullity || float_denominator_may_span_zero,
            releasable: left_property.releasable && right_property.releasable,
            nature: propagate_binary_nature(&left_property, &right_property, &Operators {
                f64: Some(Box::new(|l: &f64, r: &f64| {
                    let category = l / r;
                    if !category.is_finite() {
                        return Err("either division by zero, underflow or overflow".into())
                    }
                    Ok(category)
                })),
                i64: Some(Box::new(|l: &i64, r: &i64|
                    l.checked_div(*r).ok_or_else(|| Error::from("either division by zero, or underflow or overflow")))),
                str: None,
                bool: None,
            }, &OptimizeOperators {
                f64: Some(Box::new(|bounds| {
                    let a = match bounds.left_min {
                        Some(v) => v,
                        None => return Ok((None, None))
                    }.clone();
                    let c = match bounds.left_max {
                        Some(v) => v,
                        None => return Ok((None, None))
                    }.clone();
                    let d = match bounds.right_min {
                        Some(v) => v,
                        None => return Ok((None, None))
                    }.clone();
                    let f = match bounds.right_max {
                        Some(v) => v,
                        None => return Ok((None, None))
                    }.clone();

                    // maximize {b * d | a <= b <= c && d <= e <= f}
                    let max = match (a, c, d, f) {

                        // if either interval is a point
                        (a, c, d, f) if a == c || d == f =>
                            Some(c * f),

                        // if both intervals are not points
                        (a, c, d, f) if a > 0. && a < c && ((f == 0. && d < 0.) && (d < f && f < 0.)) =>
                            Some(a / d),
                        (a, c, d, f) if d > 0. && d < f && c > 0. && a < c =>
                            Some(c / d),
                        (a, c, d, f) if (a < c || c > 0.) && d < f && f < 0. && (a <= 0. || c <= 0.) =>
                            Some(a / f),
                        (a, c, d, f) if f > 0. && a < c && c <= 0. && (d == 0. || (d >= 0. && d < f)) =>
                            Some(c / f),

                        _ => return Err("potential division by zero".into())
                    };

                    // minimize {b * d | a <= b <= c && d <= e <= f}
                    let min = match (a, c, d, f) {
                        // if either interval is a point
                        (a, c, d, f) if a == c || d == f =>
                            Some(a * d),

                        // if both intervals are not points
                        (a, c, d, f) if 0. < d && d < f && (a < 0. || c <= 0.) && (a < c && c > 0.) =>
                            Some(a / d),
                        (a, c, d, f) if a < c && c <= 0. && ((f == 0. && d < 0.) || (d < f && f < 0.)) =>
                            Some(c / d),
                        (a, c, d, f) if (d == 0. || (0. < d && d < f)) && 0. < a && a < c && f > 0. =>
                            Some(a / f),
                        (a, c, d, f) if f < 0. && d < f && c > 0. && a < c =>
                            Some(c / f),

                        _ => return Err("potential division by zero".into())
                    };
                    Ok((min, max))
                })),
                // multiplicative bounds propagation for ints is not implemented
                i64: None}, &num_columns)?,
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            aggregator: None,
            data_type: left_property.data_type,
            dataset_id: left_property.dataset_id,
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
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let left_property = properties.get("left")
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get("right")
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayProperties {
            nullity: left_property.nullity || right_property.nullity,
            releasable: left_property.releasable && right_property.releasable,
            // raising data to a power is not monotonic
            nature: None,
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            data_type: left_property.data_type,
            aggregator: None,
            dataset_id: left_property.dataset_id,
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
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let left_property = properties.get("left")
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get("right")
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayProperties {
            nullity: left_property.nullity || right_property.nullity,
            releasable: left_property.releasable && right_property.releasable,
            nature: None,
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            data_type: left_property.data_type,
            aggregator: None,
            dataset_id: left_property.dataset_id,
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
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if let Some(nature) = data_property.nature.clone() {
            data_property.nature = match nature {
                Nature::Continuous(nature) => Some(Nature::Continuous(NatureContinuous {
                    min: match nature.max {
                        Vector1DNull::F64(max) => Vector1DNull::F64(max.iter().map(|v| match v {
                            Some(v) => Some(-v),
                            None => None
                        }).collect()),
                        Vector1DNull::I64(max) => Vector1DNull::I64(max.iter().map(|v| match v {
                            Some(v) => Some(-v),
                            None => None
                        }).collect()),
                        _ => return Err("nature min/max bounds must be numeric".into())
                    },
                    max: match nature.min {
                        Vector1DNull::F64(min) => Vector1DNull::F64(min.iter().map(|v| match v {
                            Some(v) => Some(-v),
                            None => None
                        }).collect()),
                        Vector1DNull::I64(min) => Vector1DNull::I64(min.iter().map(|v| match v {
                            Some(v) => Some(-v),
                            None => None
                        }).collect()),
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
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let left_property = properties.get("left")
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get("right")
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        let maximum = right_property.nature.and_then(|nature| match nature {
            Nature::Continuous(continuous) => Some(continuous.max),
            _ => None
        });

        Ok(ArrayProperties {
            nullity: true,
            releasable: left_property.releasable && right_property.releasable,
            nature: maximum.and_then(|maximum| Some(Nature::Continuous(NatureContinuous {
                min: match maximum {
                    Vector1DNull::F64(_) => Vector1DNull::F64((0..num_columns).map(|_| Some(0.)).collect()),
                    Vector1DNull::I64(_) => Vector1DNull::I64((0..num_columns).map(|_| Some(0)).collect()),
                    _ => return None
                },
                max: maximum,
            }))),
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            data_type: left_property.data_type,
            aggregator: None,
            dataset_id: left_property.dataset_id,
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
        _privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut current_id = *maximum_id;
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();
        let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();

        let mut component = component.clone();

        if !properties.contains_key("min") {
            current_id += 1;
            let id_min = current_id.to_owned();
            let value = Value::Array(Array::F64(
                ndarray::Array::from(properties.get("data").unwrap().to_owned().array()?.min_f64()?).into_dyn()));
            let (patch_node, release) = get_literal(&value, &component.batch)?;
            computation_graph.insert(id_min.clone(), patch_node);
            releases.insert(id_min.clone(), release);
            component.arguments.insert("min".to_string(), id_min);
        }

        if !properties.contains_key("max") {
            current_id += 1;
            let id_max = current_id.to_owned();
            let value = Value::Array(Array::F64(
                ndarray::Array::from(properties.get("data").unwrap().to_owned().array()?.max_f64()?).into_dyn()));
            let (patch_node, release) = get_literal(&value, &component.batch)?;
            computation_graph.insert(id_max.clone(), patch_node);
            releases.insert(id_max.clone(), release);
            component.arguments.insert("max".to_string(), id_max);
        }

        computation_graph.insert(component_id.clone(), component);

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases,
            traversal: Vec::new(),
        })
    }
}


impl Component for proto::And {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let left_property = properties.get("left")
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get("right")
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayProperties {
            nullity: false,
            releasable: left_property.releasable && right_property.releasable,
            nature: Some(Nature::Categorical(NatureCategorical {
                categories: Jagged::Bool((0..num_columns).map(|_| Some(vec![true, false])).collect())
            })),
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            aggregator: None,
            data_type: left_property.data_type,
            dataset_id: left_property.dataset_id,
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
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let left_property = properties.get("left")
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get("right")
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayProperties {
            nullity: false,
            releasable: left_property.releasable && right_property.releasable,
            nature: Some(Nature::Categorical(NatureCategorical {
                categories: Jagged::Bool((0..num_columns).map(|_| Some(vec![true, false])).collect())
            })),
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            aggregator: None,
            data_type: left_property.data_type,
            dataset_id: left_property.dataset_id,
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
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let left_property = properties.get("left")
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get("right")
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayProperties {
            nullity: false,
            releasable: left_property.releasable && right_property.releasable,
            nature: Some(Nature::Categorical(NatureCategorical {
                categories: Jagged::Bool((0..num_columns).map(|_| Some(vec![true, false])).collect())
            })),
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            aggregator: None,
            data_type: left_property.data_type,
            dataset_id: left_property.dataset_id,
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
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let left_property = properties.get("left")
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get("right")
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayProperties {
            nullity: false,
            releasable: left_property.releasable && right_property.releasable,
            nature: Some(Nature::Categorical(NatureCategorical {
                categories: Jagged::Bool((0..num_columns).map(|_| Some(vec![true, false])).collect())
            })),
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            aggregator: None,
            data_type: left_property.data_type,
            dataset_id: left_property.dataset_id,
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
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let left_property = properties.get("left")
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get("right")
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayProperties {
            nullity: false,
            releasable: left_property.releasable && right_property.releasable,
            nature: Some(Nature::Categorical(NatureCategorical {
                categories: Jagged::Bool((0..num_columns).map(|_| Some(vec![true, false])).collect())
            })),
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            aggregator: None,
            data_type: left_property.data_type,
            dataset_id: left_property.dataset_id,
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
        _public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let left_property = properties.get("left")
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get("right")
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ArrayProperties {
            nullity: false,
            releasable: left_property.releasable && right_property.releasable,
            nature: Some(Nature::Categorical(NatureCategorical {
                categories: Jagged::Bool((0..num_columns).map(|_| Some(vec![true, false])).collect())
            })),
            c_stability: broadcast(&left_property.c_stability, &num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, &num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            aggregator: None,
            data_type: left_property.data_type,
            dataset_id: left_property.dataset_id,
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
    pub f64: Option<Box<dyn Fn(&f64, &f64) -> Result<f64>>>,
    pub i64: Option<Box<dyn Fn(&i64, &i64) -> Result<i64>>>,
    pub str: Option<Box<dyn Fn(&String, &String) -> Result<String>>>,
    pub bool: Option<Box<dyn Fn(&bool, &bool) -> Result<bool>>>,
}

pub struct Bounds<'a, T> {
    pub left_min: &'a Option<T>,
    pub left_max: &'a Option<T>,
    pub right_min: &'a Option<T>,
    pub right_max: &'a Option<T>,
}
pub struct OptimizeOperators {
    pub f64: Option<Box<dyn Fn(Bounds<f64>) -> Result<(Option<f64>, Option<f64>)>>>,
    pub i64: Option<Box<dyn Fn(Bounds<i64>) -> Result<(Option<i64>, Option<i64>)>>>,
}

pub fn propagate_binary_shape(left_property: &ArrayProperties, right_property: &ArrayProperties) -> Result<(i64, Option<i64>)> {
    let left_num_columns = left_property.num_columns()?;
    let right_num_columns = right_property.num_columns()?;

    let left_is_column_broadcastable = left_property.releasable && left_num_columns == 1;
    let right_is_column_broadcastable = right_property.releasable && right_num_columns == 1;

    if !(left_is_column_broadcastable || right_is_column_broadcastable) && left_num_columns != right_num_columns {
        return Err("number of columns must be the same for left and right arguments".into());
    }

    let output_num_columns = left_num_columns.max(right_num_columns);

    // n must be known to prevent conformability attacks
    let left_num_records = left_property.num_records()?;
    let right_num_records = right_property.num_records()?;

    let left_is_row_broadcastable = left_property.releasable && left_num_records == 1;
    let right_is_row_broadcastable = right_property.releasable && right_num_records == 1;

    if !(left_is_row_broadcastable || right_is_row_broadcastable || (left_num_records == right_num_records)) {
        if left_property.dataset_id == right_property.dataset_id {
            return Ok((output_num_columns, None));
        }
        return Err("number of rows must be the same for left and right arguments".into());
    }

    // either left, right or both are broadcastable, so take the largest
    let output_num_records = left_num_records.max(right_num_records);

    Ok((output_num_columns, Some(output_num_records)))
}

pub fn propagate_binary_nature(
    left_property: &ArrayProperties, right_property: &ArrayProperties,
    operator: &Operators,
    optimization_operator: &OptimizeOperators,
    &output_num_columns: &i64
) -> Result<Option<Nature>> {
    Ok(match (left_property.nature.clone(), right_property.nature.clone()) {
        (Some(left_nature), Some(right_nature)) => match (left_nature, right_nature) {
            (Nature::Continuous(left_nature), Nature::Continuous(right_nature)) => {
                match (left_nature.min, left_nature.max, right_nature.min, right_nature.max) {
                    (Vector1DNull::F64(lmin), Vector1DNull::F64(lmax), Vector1DNull::F64(rmin), Vector1DNull::F64(rmax)) => {
                        let lmin = broadcast(&lmin, &output_num_columns)?;
                        let lmax = broadcast(&lmax, &output_num_columns)?;
                        let rmin = broadcast(&rmin, &output_num_columns)?;
                        let rmax = broadcast(&rmax, &output_num_columns)?;

                        let mut min = Vec::new();
                        let mut max = Vec::new();
                        lmin.iter().zip(lmax.iter()).zip(rmin.iter().zip(rmax.iter()))
                            .map(|((left_min, left_max), (right_min, right_max))| {
                                match &optimization_operator.f64 {
                                    Some(operator) => {
                                        let (col_min, col_max) = operator(Bounds {left_min, left_max, right_min, right_max })?;
                                        min.push(col_min);
                                        max.push(col_max);
                                    },
                                    None => {
                                        min.push(None);
                                        max.push(None);
                                    }
                                }
                                Ok(())
                            })
                            .collect::<Result<()>>()?;
                        Some(Nature::Continuous(NatureContinuous {min: Vector1DNull::F64(min), max: Vector1DNull::F64(max)}))
                    },
                    (Vector1DNull::I64(lmin), Vector1DNull::I64(lmax), Vector1DNull::I64(rmin), Vector1DNull::I64(rmax)) => {
                        let lmin = broadcast(&lmin, &output_num_columns)?;
                        let lmax = broadcast(&lmax, &output_num_columns)?;
                        let rmin = broadcast(&rmin, &output_num_columns)?;
                        let rmax = broadcast(&rmax, &output_num_columns)?;

                        let mut min = Vec::new();
                        let mut max = Vec::new();
                        lmin.iter().zip(lmax.iter()).zip(rmin.iter().zip(rmax.iter()))
                            .map(|((left_min, left_max), (right_min, right_max))| {
                                match &optimization_operator.i64 {
                                    Some(operator) => {
                                        let (col_min, col_max) = operator(Bounds {left_min, left_max, right_min, right_max })?;
                                        min.push(col_min);
                                        max.push(col_max);
                                    },
                                    None => {
                                        min.push(None);
                                        max.push(None);
                                    }
                                }
                                Ok(())
                            })
                            .collect::<Result<()>>()?;
                        Some(Nature::Continuous(NatureContinuous {min: Vector1DNull::I64(min), max: Vector1DNull::I64(max)}))
                    },
                    _ => return Err("continuous bounds must be numeric and homogeneously typed".into())
                }
            }

            (Nature::Categorical(left_nature), Nature::Categorical(right_nature)) => Some(Nature::Categorical(NatureCategorical {
                categories: match (left_nature.categories.standardize(&output_num_columns)?, right_nature.categories.standardize(&output_num_columns)?) {
                    (Jagged::F64(left), Jagged::F64(right)) =>
                        Jagged::F64(left.iter().zip(right.iter()).map(|(left, right)|
                            match (left, right, &operator.f64) {
                                (Some(left), Some(right), Some(operator)) => Ok(Some(left.iter()
                                    .map(|left| right.iter()
                                        .map(|right| operator(left, right))
                                        .collect::<Result<Vec<_>>>())
                                    .collect::<Result<Vec<Vec<_>>>>()?
                                    .into_iter().flatten().collect::<Vec<_>>())),
                                (Some(_), Some(_), None) => Err("categories cannot be propagated for floats".into()),
                                _ => Ok(None)
                            }).collect::<Result<Vec<Option<Vec<_>>>>>()?),
                    (Jagged::I64(left), Jagged::I64(right)) =>
                        Jagged::I64(left.iter().zip(right.iter()).map(|(left, right)|
                            match (left, right, &operator.i64) {
                                (Some(left), Some(right), Some(operator)) => Ok(Some(left.iter()
                                    .map(|left| right.iter()
                                        .map(|right| operator(left, right))
                                        .collect::<Result<Vec<_>>>())
                                    .collect::<Result<Vec<Vec<_>>>>()?
                                    .into_iter().flatten().collect::<Vec<_>>())),
                                (Some(_), Some(_), None) => Err("categories cannot be propagated for integers".into()),
                                _ => Ok(None)
                            }).collect::<Result<Vec<Option<Vec<_>>>>>()?),
                    (Jagged::Bool(left), Jagged::Bool(right)) =>
                        Jagged::Bool(left.iter().zip(right.iter()).map(|(left, right)|
                            match (left, right, &operator.bool) {
                                (Some(left), Some(right), Some(operator)) => Ok(Some(left.iter()
                                    .map(|left| right.iter()
                                        .map(|right| operator(left, right))
                                        .collect::<Result<Vec<_>>>())
                                    .collect::<Result<Vec<Vec<_>>>>()?
                                    .into_iter().flatten().collect::<Vec<_>>())),
                                (Some(_), Some(_), None) => Err("categories cannot be propagated for booleans".into()),
                                _ => Ok(None)
                            }).collect::<Result<Vec<Option<Vec<_>>>>>()?),
                    (Jagged::Str(left), Jagged::Str(right)) =>
                        Jagged::Str(left.iter().zip(right.iter()).map(|(left, right)|
                            match (left, right, &operator.str) {
                                (Some(left), Some(right), Some(operator)) => Ok(Some(left.iter()
                                    .map(|left| right.iter()
                                        .map(|right| operator(left, right))
                                        .collect::<Result<Vec<_>>>())
                                    .collect::<Result<Vec<Vec<_>>>>()?
                                    .into_iter().flatten().collect::<Vec<_>>())),
                                (Some(_), Some(_), None) => Err("categories cannot be propagated for strings".into()),
                                _ => Ok(None)
                            }).collect::<Result<Vec<Option<Vec<_>>>>>()?),
                    _ => return Err("natures must be homogeneously typed".into())
                }.deduplicate()?
            })),
            _ => None
        },
        _ => None
    })
}

fn broadcast<T: Clone>(data: &[T], length: &i64) -> Result<Vec<T>> {
    if data.len() as i64 == *length {
        return Ok(data.to_owned());
    }

    if data.len() != 1 {
        return Err("could not broadcast vector".into());
    }

    Ok((0..*length).map(|_| data[0].clone()).collect())
}
