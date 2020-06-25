use crate::errors::*;

use crate::base::{Nature, NatureCategorical, Vector1DNull, Jagged, ArrayProperties, ValueProperties, DataType};

use crate::{proto, base, Warnable, Integer, Float};

use crate::utilities::{prepend};

use crate::components::{Component};

use crate::base::{IndexKey, Value, NatureContinuous};
use num::{CheckedAdd, CheckedSub, Zero};
use indexmap::map::IndexMap;
use std::ops::{Mul, Div};


impl Component for proto::Abs {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let mut data_property = properties.get(&IndexKey::from("data"))
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();
        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        data_property.nature = propagate_unary_nature(
            &data_property,
            &UnaryOperators {
                float: Some(Box::new(|v| Ok(v.abs()))),
                int: Some(Box::new(|v| Ok(v.abs()))),
                bool: None,
                str: None,
            },
            &OptimizeUnaryOperators {
                float: Some(Box::new(|bounds| match (bounds.lower, bounds.upper) {
                    (Some(lower), Some(upper)) => Ok((
                        Some(if lower > &0. { *lower } else { -*upper }),
                        Some(if lower + upper > 0. { *upper } else { -*lower }))),
                    _ => Ok((None, None))
                })),
                int: Some(Box::new(|bounds| match (bounds.lower, bounds.upper) {
                    (Some(lower), Some(upper)) => Ok((
                        Some(if lower > &0 { *lower } else { -*upper }),
                        Some(if lower + upper > 0 { *upper } else { -*lower }))),
                    _ => Ok((None, None))
                })),
            }, data_property.num_columns()?)?;

        Ok(ValueProperties::Array(data_property).into())
    }
}

impl Component for proto::Add {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let left_property = properties.get(&IndexKey::from("left"))
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get::<IndexKey>(&"right".into())
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        // // Add is 1-Lipschitz in L1 space
        // if let Some(mut aggregator) = data_property.aggregator {
        //     aggregator.lipschitz_constants = aggregator.lipschitz_constants
        //         .into_iter().map(|v| v * 1.).collect();
        //     data_property.aggregator = Some(aggregator);
        // }

        if !left_property.releasable {
            left_property.assert_is_not_aggregated()?;
        }
        if !right_property.releasable {
            right_property.assert_is_not_aggregated()?;
        }

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;
        if left_property.data_type != right_property.data_type {
            return Err("left and right arguments must share the same data types".into())
        }

        Ok(ValueProperties::Array(ArrayProperties {
            nullity: left_property.nullity || right_property.nullity,
            releasable: left_property.releasable && right_property.releasable,
            nature: propagate_binary_nature(&left_property, &right_property, &BinaryOperators {
                float: Some(Box::new(|l: &Float, r: &Float|
                    Ok(l + r))),
                int: Some(Box::new(|l: &Integer, r: &Integer|
                    l.checked_add(r).ok_or_else(|| Error::from("addition may result in underflow or overflow")))),
                str: Some(Box::new(|l: &String, r: &String| Ok(format!("{}{}", l, r)))),
                bool: None,
            }, &OptimizeBinaryOperators {
                float: Some(&|bounds| Ok((
                    bounds.left_lower.and_then(|lmin| bounds.right_lower.and_then(|rmin|
                        Some(lmin + rmin))),
                    bounds.left_upper.and_then(|lmax| bounds.right_upper.and_then(|rmax|
                        Some(lmax + rmax))),
                ))),
                int: Some(&|bounds| Ok((
                    match (bounds.left_lower, bounds.right_lower) {
                        (Some(lmin), Some(rmin)) => Some(lmin.checked_add(rmin)
                            .ok_or_else(|| Error::from("addition may result in underflow or overflow"))?),
                        _ => None
                    },
                    match (bounds.left_upper, bounds.right_upper) {
                        (Some(lmax), Some(rmax)) => Some(lmax.checked_add(rmax)
                            .ok_or_else(|| Error::from("addition may result in underflow or overflow"))?),
                        _ => None
                    })))
            }, num_columns)?,
            c_stability: broadcast(&left_property.c_stability, num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            aggregator: None,
            data_type: left_property.data_type,
            dataset_id: left_property.dataset_id,
            is_not_empty: left_property.is_not_empty && right_property.is_not_empty,
            dimensionality: left_property.dimensionality
                .max(right_property.dimensionality),
            group_id: left_property.group_id
        }).into())
    }
}

impl Component for proto::And {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let mut left_property = properties.get(&IndexKey::from("left"))
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get::<IndexKey>(&"right".into())
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        if !left_property.releasable {
            left_property.assert_is_not_aggregated()?;
        }
        if !right_property.releasable {
            right_property.assert_is_not_aggregated()?;
        }

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;
        if left_property.data_type != right_property.data_type {
            return Err("left and right arguments must share the same data types".into())
        }

        left_property.releasable = left_property.releasable && right_property.releasable;
        left_property.nature = propagate_binary_nature(
            &left_property, &right_property,
            &BinaryOperators {
                float: None,
                int: None,
                str: None,
                bool: Some(Box::new(|l: &bool, r: &bool| Ok(*l && *r))),
            }, &OptimizeBinaryOperators { float: None, int: None },
            num_columns)?;
        left_property.c_stability = broadcast(&left_property.c_stability, num_columns)?.iter()
            .zip(broadcast(&right_property.c_stability, num_columns)?)
            .map(|(l, r)| l.max(r)).collect();
        left_property.num_columns = Some(num_columns);
        left_property.num_records = num_records;

        left_property.is_not_empty = left_property.is_not_empty && right_property.is_not_empty;
        left_property.dimensionality = left_property.dimensionality
            .max(right_property.dimensionality);

        Ok(ValueProperties::Array(left_property).into())
    }
}

impl Component for proto::Divide {
    #[allow(clippy::float_cmp)]
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let left_property = properties.get(&IndexKey::from("left"))
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get::<IndexKey>(&"right".into())
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        if !left_property.releasable {
            left_property.assert_is_not_aggregated()?;
        }
        if !right_property.releasable {
            right_property.assert_is_not_aggregated()?;
        }

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;
        if left_property.data_type != right_property.data_type {
            return Err("left and right arguments must share the same data types".into())
        }

        let float_denominator_may_span_zero = match right_property.clone().nature {
            Some(nature) => match nature {
                Nature::Continuous(nature) => nature.lower.float()
                    .map(|min| nature.upper.float()
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
                Nature::Categorical(nature) => nature.categories.float()
                    .map(|categories| categories.iter()
                        .any(|column| column.iter()
                            .any(|category| category.is_nan() || category == &0.)))
                    // if categories are not known, a category could be zero or NAN
                    .unwrap_or(false)
            },
            // if nature is not known, data could span zero
            _ => true
        };

        // minimize and maximize b / e when a <= b <= c and d <= e <= f
        fn optimize<T: PartialOrd + Div<Output=T> + Zero + Copy>(
            a: T, c: T, d: T, f: T
        ) -> Result<(Option<T>, Option<T>)> {

            let zero = T::zero();
            // maximize {b * d | a <= b <= c && d <= e <= f}
            let max = match (a, c, d, f) {

                // if either interval is a point
                (a, c, d, f) if a == c || d == f =>
                    Some(c / f),

                // if both intervals are not points
                (a, c, d, f) if a > zero && a < c && ((f == zero && d < zero) && (d < f && f < zero)) =>
                    Some(a / d),
                (a, c, d, f) if d > zero && d < f && c > zero && a < c =>
                    Some(c / d),
                (a, c, d, f) if (a < c || c > zero) && d < f && f < zero && (a <= zero || c <= zero) =>
                    Some(a / f),
                (a, c, d, f) if f > zero && a < c && c <= zero && (d == zero || (d >= zero && d < f)) =>
                    Some(c / f),

                _ => return Err("potential division by zero".into())
            };

            // minimize {b * d | a <= b <= c && d <= e <= f}
            let min = match (a, c, d, f) {
                // if either interval is a point
                (a, c, d, f) if a == c || d == f =>
                    Some(a / d),

                // if both intervals are not points
                (a, c, d, f) if zero < d && d < f && (a < zero || c <= zero) && (a < c && c > zero) =>
                    Some(a / d),
                (a, c, d, f) if a < c && c <= zero && ((f == zero && d < zero) || (d < f && f < zero)) =>
                    Some(c / d),
                (a, c, d, f) if (d == zero || (zero < d && d < f)) && zero < a && a < c && f > zero =>
                    Some(a / f),
                (a, c, d, f) if f < zero && d < f && c > zero && a < c =>
                    Some(c / f),

                _ => return Err("potential division by zero".into())
            };
            Ok((min, max))
        }

        fn optimize_wrapper<T: PartialOrd + Div<Output=T> + Zero + Copy>(
            bounds: BinaryBounds<T>
        ) -> Result<(Option<T>, Option<T>)> {

            let a = match bounds.left_lower {
                Some(v) => *v,
                None => return Ok((None, None))
            };
            let c = match bounds.left_upper {
                Some(v) => *v,
                None => return Ok((None, None))
            };
            let d = match bounds.right_lower {
                Some(v) => *v,
                None => {
                    if bounds.right_upper.map(|v| v >= T::zero()).unwrap_or(true) {
                        return Err("potential division by zero".into())
                    }
                    return Ok((None, None))
                }
            };
            let f = match bounds.right_upper {
                Some(v) => *v,
                None => {
                    if bounds.right_lower.map(|v| v <= T::zero()).unwrap_or(true) {
                        return Err("potential division by zero".into())
                    }
                    return Ok((None, None))
                }
            };
            optimize(a, c, d, f)
        }

        Ok(ValueProperties::Array(ArrayProperties {
            nullity: left_property.nullity || right_property.nullity || float_denominator_may_span_zero,
            releasable: left_property.releasable && right_property.releasable,
            nature: propagate_binary_nature(&left_property, &right_property, &BinaryOperators {
                float: Some(Box::new(|l: &Float, r: &Float| {
                    let category = l / r;
                    if !category.is_finite() {
                        return Err("either division by zero, underflow or overflow".into())
                    }
                    Ok(category)
                })),
                int: Some(Box::new(|l: &Integer, r: &Integer|
                    l.checked_div(*r).ok_or_else(|| Error::from("either division by zero, or underflow or overflow")))),
                str: None,
                bool: None,
            }, &OptimizeBinaryOperators {
                float: Some(&optimize_wrapper),
                int: Some(&optimize_wrapper)
            }, num_columns)?,
            c_stability: broadcast(&left_property.c_stability, num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            aggregator: None,
            data_type: left_property.data_type,
            dataset_id: left_property.dataset_id,
            is_not_empty: left_property.is_not_empty && right_property.is_not_empty,
            dimensionality: left_property.dimensionality
                .max(right_property.dimensionality),
            group_id: left_property.group_id
        }).into())
    }
}

impl Component for proto::Equal {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let left_property = properties.get(&IndexKey::from("left"))
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get::<IndexKey>(&"right".into())
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        if !left_property.releasable {
            left_property.assert_is_not_aggregated()?;
        }
        if !right_property.releasable {
            right_property.assert_is_not_aggregated()?;
        }

        if left_property.data_type != right_property.data_type {
            return Err("left and right must be homogeneously typed".into())
        }

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ValueProperties::Array(ArrayProperties {
            nullity: false,
            releasable: left_property.releasable && right_property.releasable,
            nature: Some(Nature::Categorical(NatureCategorical {
                categories: Jagged::Bool((0..num_columns).map(|_| vec![true, false]).collect())
            })),
            c_stability: broadcast(&left_property.c_stability, num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            aggregator: None,
            data_type: DataType::Bool,
            dataset_id: left_property.dataset_id,
            is_not_empty: left_property.is_not_empty && right_property.is_not_empty,
            dimensionality: left_property.dimensionality.max(right_property.dimensionality),
            group_id: left_property.group_id
        }).into())
    }
}


impl Component for proto::GreaterThan {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let left_property = properties.get(&IndexKey::from("left"))
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get::<IndexKey>(&"right".into())
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        if !left_property.releasable {
            left_property.assert_is_not_aggregated()?;
        }
        if !right_property.releasable {
            right_property.assert_is_not_aggregated()?;
        }

        if left_property.data_type != right_property.data_type {
            return Err("left and right must be homogeneously typed".into())
        }
        if left_property.data_type != DataType::Int && left_property.data_type != DataType::Float {
            return Err("left must be numeric".into())
        }
        if right_property.data_type != DataType::Int && right_property.data_type != DataType::Float {
            return Err("right must be numeric".into())
        }

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ValueProperties::Array(ArrayProperties {
            nullity: false,
            releasable: left_property.releasable && right_property.releasable,
            nature: Some(Nature::Categorical(NatureCategorical {
                categories: Jagged::Bool((0..num_columns).map(|_| vec![true, false]).collect())
            })),
            c_stability: broadcast(&left_property.c_stability, num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            aggregator: None,
            data_type: DataType::Bool,
            dataset_id: left_property.dataset_id,
            is_not_empty: left_property.is_not_empty && right_property.is_not_empty,
            dimensionality: left_property.dimensionality
                .max(right_property.dimensionality),
            group_id: left_property.group_id
        }).into())
    }
}


impl Component for proto::LessThan {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let left_property = properties.get(&IndexKey::from("left"))
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get::<IndexKey>(&"right".into())
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        if !left_property.releasable {
            left_property.assert_is_not_aggregated()?;
        }
        if !right_property.releasable {
            right_property.assert_is_not_aggregated()?;
        }

        if left_property.data_type != right_property.data_type {
            return Err("left and right must be homogeneously typed".into())
        }
        if left_property.data_type != DataType::Int && left_property.data_type != DataType::Float {
            return Err("left must be numeric".into())
        }
        if right_property.data_type != DataType::Int && right_property.data_type != DataType::Float {
            return Err("right must be numeric".into())
        }

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;

        Ok(ValueProperties::Array(ArrayProperties {
            nullity: false,
            releasable: left_property.releasable && right_property.releasable,
            nature: Some(Nature::Categorical(NatureCategorical {
                categories: Jagged::Bool((0..num_columns).map(|_| vec![true, false]).collect())
            })),
            c_stability: broadcast(&left_property.c_stability, num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            aggregator: None,
            data_type: DataType::Bool,
            dataset_id: left_property.dataset_id,
            is_not_empty: left_property.is_not_empty && right_property.is_not_empty,
            dimensionality: left_property.dimensionality
                .max(right_property.dimensionality),
            group_id: left_property.group_id
        }).into())
    }
}


impl Component for proto::Log {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let mut data_property = properties.get(&IndexKey::from("data"))
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let base_property = properties.get::<IndexKey>(&"base".into())
            .ok_or("base: missing")?.array()
            .map_err(prepend("base:"))?.clone();

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }
        if !base_property.releasable {
            base_property.assert_is_not_aggregated()?;
        }

        if data_property.data_type != DataType::Float {
            return Err("arguments for log must be float and homogeneously typed".into());
        }

        if !base_property.lower_float()?.iter()
            .zip(base_property.upper_float()?.iter())
            .all(|(min, max)| min > &0. && max < &1. || min > &1.) {
            return Err("base must be in [0, 1) U (1, inf) and not span zero".into())
        }

        if !data_property.lower_float()?.iter()
            .all(|min| min > &0.) {
            return Err("data may potentially be less than zero".into())
        }

        data_property.nature = propagate_binary_nature(
            &data_property, &base_property,
            &BinaryOperators {
                float: Some(Box::new(|v, base| Ok(v.log(*base)))),
                int: None,
                bool: None,
                str: None,
            },
            &OptimizeBinaryOperators {
                float: Some(&|_bounds| {
                    // TODO: derive data bounds for log transform
                    Ok((None, None))
                }),
                int: None
            }, data_property.num_columns()?)?;

        data_property.is_not_empty = data_property.is_not_empty && base_property.is_not_empty;
        data_property.dimensionality = data_property.dimensionality
            .max(base_property.dimensionality);
        Ok(ValueProperties::Array(data_property).into())
    }
}


impl Component for proto::Modulo {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let mut left_property = properties.get(&IndexKey::from("left"))
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get::<IndexKey>(&"right".into())
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        if !left_property.releasable {
            left_property.assert_is_not_aggregated()?;
        }
        if !right_property.releasable {
            right_property.assert_is_not_aggregated()?;
        }

        match (left_property.data_type.clone(), right_property.data_type.clone()) {
            (DataType::Float, DataType::Float) => {

                if !right_property.lower_float()?.iter().all(|v| v > &0.) {
                    return Err("divisor must be greater than zero".into())
                }

                left_property.nature = propagate_binary_nature(
                    &left_property, &right_property,
                    &BinaryOperators {
                        float: Some(Box::new(|l, r| Ok(l.rem_euclid(*r)))),
                        int: None,
                        bool: None,
                        str: None,
                    },
                    &OptimizeBinaryOperators {
                        // TODO: this could be tighter
                        float: Some(&|bounds| Ok((Some(0.), *bounds.right_upper))),
                        int: None
                    }, left_property.num_columns()?)?;
            },
            (DataType::Int, DataType::Int) => {
                if !right_property.lower_int()?.iter().all(|v| v > &0) {
                    return Err("divisor must be greater than zero".into())
                }
                left_property.nature = propagate_binary_nature(
                    &left_property, &right_property,
                    &BinaryOperators {
                        float: None,
                        int: Some(Box::new(|l, r| Ok(l.rem_euclid(*r)))),
                        bool: None,
                        str: None,
                    },
                    &OptimizeBinaryOperators {
                        float: None,
                        int: Some(&|bounds| Ok((Some(0), bounds.right_upper.map(|v| v - 1)))),
                    }, left_property.num_columns()?)?;
            },
            _ => return Err("arguments for power must be numeric and homogeneously typed".into())
        };

        left_property.is_not_empty = left_property.is_not_empty && right_property.is_not_empty;
        left_property.dimensionality = left_property.dimensionality
            .max(right_property.dimensionality);
        Ok(ValueProperties::Array(left_property).into())
    }
}


impl Component for proto::Multiply {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let left_property = properties.get(&IndexKey::from("left"))
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get::<IndexKey>(&"right".into())
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        if !left_property.releasable {
            left_property.assert_is_not_aggregated()?;
        }
        if !right_property.releasable {
            right_property.assert_is_not_aggregated()?;
        }

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;
        if left_property.data_type != right_property.data_type {
            return Err("left and right arguments must share the same data types".into())
        }

        /// compute minimum and maximum of b * e when a <= b <= c and d <= e <= f
        fn optimize<T: PartialOrd + Mul<Output=T> + Zero + Copy>(
            a: T, c: T, d: T, f: T
        ) -> Result<(Option<T>, Option<T>)> {

            let zero = T::zero();
            // maximize {b * d | a <= b <= c && d <= e <= f}
            let max = match (a, c, d, f) {

                // if either interval is a point
                (a, c, d, f) if a == c || d == f =>
                    Some(c * f),

                // if both intervals are not points
                (a, c, d, f) if (d < zero && ((c > zero && ((f == zero && a < zero) || (a * d > c * f && f > zero && d + f >= zero))) || (a < c && f >= zero && c <= zero)))
                    || (a < c && c <= zero && ((d < f && f < zero) || (f > zero && d + f < zero)))
                    || (c > zero && ((d < f && f < zero && a <= zero) || (f > zero && d + f < zero && a * d <= c * f))) =>
                    Some(a * d),
                (a, c, d, f) if zero <= d && d < f && c <= zero && a < c =>
                    Some(c * d),
                (a, c, d, f) if f < zero && d < f && zero < a && a < c =>
                    Some(a * f),
                (a, c, d, f) if c > zero && f > zero && a < c
                    && ((a * d >= c * f && d + f >= zero && d < zero) || (d < f && d >= zero) || (c * f < a * d && d + f < zero)) =>
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
                (a, c, d, f) if d > zero && d < f && a > zero && a < c =>
                    Some(a * d),
                (a, c, d, f) if c > zero && a < c && ((f > zero && a * f > c * d && d < zero)
                    || (d < f && f <= zero)) =>
                    Some(c * d),
                (a, c, d, f) if f > zero && ((c > zero && ((a < zero && (d == zero || (d >= zero && d < f)
                    || (d <= zero && a * f <= c * d))) || (d < zero && a * f <= c * d)))
                    || (a < c && c <= zero && (d < f || d <= zero))) =>
                    Some(a * f),
                (a, c, d, f) if f <= zero && d < f && c <= zero && a < c =>
                    Some(c * f),

                // Prior cases should cover all
                _ => None
            };
            Ok((min, max))
        }

        fn optimize_wrapper<T: PartialOrd + Mul<Output=T> + Zero + Copy>(
            bounds: BinaryBounds<T>
        ) -> Result<(Option<T>, Option<T>)> {
            let a = match bounds.left_lower {
                Some(v) => *v,
                None => return Ok((None, None))
            };
            let c = match bounds.left_upper {
                Some(v) => *v,
                None => return Ok((None, None))
            };
            let d = match bounds.right_lower {
                Some(v) => *v,
                None => return Ok((None, None))
            };
            let f = match bounds.right_upper {
                Some(v) => *v,
                None => return Ok((None, None))
            };
            optimize(a, c, d, f)
        }

        Ok(ValueProperties::Array(ArrayProperties {
            nullity: left_property.nullity || right_property.nullity,
            releasable: left_property.releasable && right_property.releasable,
            nature: propagate_binary_nature(&left_property, &right_property, &BinaryOperators {
                float: Some(Box::new(|l: &Float, r: &Float| {
                    let category = l * r;
                    if !category.is_finite() {
                        return Err("multiplication may result in underflow or overflow".into())
                    }
                    Ok(category)
                })),
                int: Some(Box::new(|l: &Integer, r: &Integer|
                    l.checked_mul(*r).ok_or_else(|| Error::from("multiplication may result in underflow or overflow")))),
                str: None,
                bool: None,
            }, &OptimizeBinaryOperators {
                float: Some(&optimize_wrapper),
                int: Some(&optimize_wrapper),
            }, num_columns)?,
            c_stability: broadcast(&left_property.c_stability, num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            data_type: left_property.data_type,
            num_records,
            aggregator: None,
            dataset_id: left_property.dataset_id,
            is_not_empty: left_property.is_not_empty && right_property.is_not_empty,
            dimensionality: left_property.dimensionality
                .max(right_property.dimensionality),
            group_id: left_property.group_id
        }).into())
    }
}


impl Component for proto::Negate {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let mut data_property = properties.get(&IndexKey::from("data"))
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        data_property.nature = propagate_unary_nature(
            &data_property,
            &UnaryOperators {
                float: None,
                int: None,
                str: None,
                bool: Some(Box::new(|v| Ok(!*v))),
            }, &OptimizeUnaryOperators { float: None, int: None },
            data_property.num_columns()?)?;

        Ok(ValueProperties::Array(data_property).into())
    }
}

impl Component for proto::Negative {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let mut data_property = properties.get(&IndexKey::from("data"))
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        data_property.nature = propagate_unary_nature(
            &data_property,
            &UnaryOperators {
                float: Some(Box::new(|v| Ok(-*v))),
                int: Some(Box::new(|v| Ok(-*v))),
                bool: None,
                str: None,
            },
            &OptimizeUnaryOperators {
                float: Some(Box::new(|bounds|
                    Ok((bounds.upper.map(|v| -v), bounds.lower.map(|v| -v))))),
                int: Some(Box::new(|bounds|
                    Ok((bounds.upper.map(|v| -v), bounds.lower.map(|v| -v))))),
            }, data_property.num_columns()?)?;

        Ok(ValueProperties::Array(data_property).into())
    }
}


impl Component for proto::Or {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let mut left_property = properties.get(&IndexKey::from("left"))
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get::<IndexKey>(&"right".into())
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        if !left_property.releasable {
            left_property.assert_is_not_aggregated()?;
        }
        if !right_property.releasable {
            right_property.assert_is_not_aggregated()?;
        }

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;
        if left_property.data_type != right_property.data_type {
            return Err("left and right arguments must share the same data types".into())
        }

        left_property.releasable = left_property.releasable && right_property.releasable;
        left_property.nature = propagate_binary_nature(
            &left_property, &right_property,
            &BinaryOperators {
                float: None,
                int: None,
                str: None,
                bool: Some(Box::new(|l: &bool, r: &bool| Ok(*l || *r))),
            }, &OptimizeBinaryOperators { float: None, int: None },
            num_columns)?;
        left_property.c_stability = broadcast(&left_property.c_stability, num_columns)?.iter()
            .zip(broadcast(&right_property.c_stability, num_columns)?)
            .map(|(l, r)| l.max(r)).collect();
        left_property.num_columns = Some(num_columns);
        left_property.num_records = num_records;

        left_property.is_not_empty = left_property.is_not_empty && right_property.is_not_empty;
        left_property.dimensionality = left_property.dimensionality
            .max(right_property.dimensionality);

        Ok(ValueProperties::Array(left_property).into())
    }
}


impl Component for proto::Power {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let mut data_property = properties.get(&IndexKey::from("data"))
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();
        let radical_property = properties.get::<IndexKey>(&"radical".into())
            .ok_or("radical: missing")?.array()
            .map_err(prepend("radical:"))?.clone();

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }
        if !radical_property.releasable {
            radical_property.assert_is_not_aggregated()?;
        }

        match (data_property.data_type.clone(), radical_property.data_type.clone()) {
            (DataType::Float, DataType::Float) => {

                data_property.nature = propagate_binary_nature(
                    &data_property, &radical_property,
                    &BinaryOperators {
                        float: Some(Box::new(|l, r| Ok(l.powf(*r)))),
                        int: None,
                        bool: None,
                        str: None,
                    },
                    // TODO: derive bounds
                    &OptimizeBinaryOperators {
                        float: Some(&|_bounds| Ok((None, None))),
                        int: None
                    }, data_property.num_columns()?)?;
            },
            (DataType::Int, DataType::Int) => {
                if !radical_property.lower_int()?.iter().all(|min| min >= &0) {
                    return Err("integer power must not be negative".into())
                }

                data_property.nature = propagate_binary_nature(
                    &data_property, &radical_property,
                    &BinaryOperators {
                        float: None,
                        int: Some(Box::new(|l, r| l.checked_pow(*r as u32)
                            .ok_or_else(|| Error::from("power may result in overflow")))),
                        bool: None,
                        str: None,
                    },
                    // TODO: derive bounds and throw error if potential overflow
                    &OptimizeBinaryOperators {
                        float: None,
                        int: Some(&|_bounds| Ok((None, None))),
                    }, data_property.num_columns()?)?;
            },
            _ => return Err("arguments for power must be numeric and homogeneously typed".into())
        }

        data_property.is_not_empty = data_property.is_not_empty && radical_property.is_not_empty;
        data_property.dimensionality = data_property.dimensionality
            .max(radical_property.dimensionality);
        Ok(ValueProperties::Array(data_property).into())
    }
}


impl Component for proto::RowMax {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let left_property = properties.get(&IndexKey::from("left"))
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get::<IndexKey>(&"right".into())
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        if !left_property.releasable {
            left_property.assert_is_not_aggregated()?;
        }
        if !right_property.releasable {
            right_property.assert_is_not_aggregated()?;
        }

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;
        if left_property.data_type != right_property.data_type {
            return Err("left and right arguments must share the same data types".into())
        }

        Ok(ValueProperties::Array(ArrayProperties {
            nullity: left_property.nullity || right_property.nullity,
            releasable: left_property.releasable && right_property.releasable,
            nature: propagate_binary_nature(&left_property, &right_property, &BinaryOperators {
                float: Some(Box::new(|l: &Float, r: &Float|
                    Ok(l.max(*r)))),
                int: Some(Box::new(|l: &Integer, r: &Integer|
                    Ok(*l.max(r)))),
                str: Some(Box::new(|l: &String, r: &String| Ok(format!("{}{}", l, r)))),
                bool: None,
            }, &OptimizeBinaryOperators {
                float: Some(&|bounds| Ok((
                    // min
                    match (bounds.left_lower, bounds.right_lower) {
                        (Some(left_lower), Some(right_lower)) => Some(left_lower.max(*right_lower)),
                        _ => None
                    },
                    // max
                    match (bounds.left_upper, bounds.right_upper) {
                        (Some(left_upper), Some(right_upper)) => Some(left_upper.max(*right_upper)),
                        _ => None
                    }
                ))),
                int: Some(&|bounds| Ok((
                    // min
                    match (bounds.left_lower, bounds.right_lower) {
                        (Some(left_lower), Some(right_lower)) => Some(*left_lower.max(right_lower)),
                        _ => None
                    },
                    // max
                    match (bounds.left_upper, bounds.right_upper) {
                        (Some(left_upper), Some(right_upper)) => Some(*left_upper.max(right_upper)),
                        _ => None
                    }
                )))
            }, num_columns)?,
            c_stability: broadcast(&left_property.c_stability, num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            aggregator: None,
            data_type: left_property.data_type,
            dataset_id: left_property.dataset_id,
            is_not_empty: left_property.is_not_empty && right_property.is_not_empty,
            dimensionality: left_property.dimensionality
                .max(right_property.dimensionality),
            group_id: left_property.group_id
        }).into())
    }
}

impl Component for proto::RowMin {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let left_property = properties.get(&IndexKey::from("left"))
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get::<IndexKey>(&"right".into())
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        if !left_property.releasable {
            left_property.assert_is_not_aggregated()?;
        }
        if !right_property.releasable {
            right_property.assert_is_not_aggregated()?;
        }

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;
        if left_property.data_type != right_property.data_type {
            return Err("left and right arguments must share the same data types".into())
        }

        Ok(ValueProperties::Array(ArrayProperties {
            nullity: left_property.nullity || right_property.nullity,
            releasable: left_property.releasable && right_property.releasable,
            nature: propagate_binary_nature(&left_property, &right_property, &BinaryOperators {
                float: Some(Box::new(|l: &Float, r: &Float|
                    Ok(l.min(*r)))),
                int: Some(Box::new(|l: &Integer, r: &Integer|
                    Ok(*l.min(r)))),
                str: Some(Box::new(|l: &String, r: &String| Ok(format!("{}{}", l, r)))),
                bool: None,
            }, &OptimizeBinaryOperators {
                float: Some(&|bounds| Ok((
                    // min
                    match (bounds.left_lower, bounds.right_lower) {
                        (Some(left_lower), Some(right_lower)) => Some(left_lower.min(*right_lower)),
                        _ => None
                    },
                    // max
                    match (bounds.left_upper, bounds.right_upper) {
                        (Some(left_upper), Some(right_upper)) => Some(left_upper.min(*right_upper)),
                        _ => None
                    }
                ))),
                int: Some(&|bounds| Ok((
                    // min
                    match (bounds.left_lower, bounds.right_lower) {
                        (Some(left_lower), Some(right_lower)) => Some(*left_lower.min(right_lower)),
                        _ => None
                    },
                    // max
                    match (bounds.left_upper, bounds.right_upper) {
                        (Some(left_upper), Some(right_upper)) => Some(*left_upper.min(right_upper)),
                        _ => None
                    }
                )))
            }, num_columns)?,
            c_stability: broadcast(&left_property.c_stability, num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            aggregator: None,
            data_type: left_property.data_type,
            dataset_id: left_property.dataset_id,
            is_not_empty: left_property.is_not_empty && right_property.is_not_empty,
            dimensionality: left_property.dimensionality
                .max(right_property.dimensionality),
            group_id: left_property.group_id
        }).into())
    }
}

impl Component for proto::Subtract {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let left_property = properties.get(&IndexKey::from("left"))
            .ok_or("left: missing")?.array()
            .map_err(prepend("left:"))?.clone();
        let right_property = properties.get::<IndexKey>(&"right".into())
            .ok_or("right: missing")?.array()
            .map_err(prepend("right:"))?.clone();

        if !left_property.releasable {
            left_property.assert_is_not_aggregated()?;
        }
        if !right_property.releasable {
            right_property.assert_is_not_aggregated()?;
        }

        let (num_columns, num_records) = propagate_binary_shape(&left_property, &right_property)?;
        if left_property.data_type != right_property.data_type {
            return Err("left and right arguments must share the same data types".into())
        }

        Ok(ValueProperties::Array(ArrayProperties {
            nullity: left_property.nullity || right_property.nullity,
            releasable: left_property.releasable && right_property.releasable,
            nature: propagate_binary_nature(&left_property, &right_property, &BinaryOperators {
                float: Some(Box::new(|l: &Float, r: &Float|
                    Ok(l - r))),
                int: Some(Box::new(|l: &Integer, r: &Integer|
                    l.checked_sub(r).ok_or_else(|| Error::from("subtraction may result in underflow or overflow")))),
                str: None,
                bool: None,
            }, &OptimizeBinaryOperators {
                float: Some(&|bounds| Ok((
                    bounds.left_lower.and_then(|lmin| bounds.right_lower.and_then(|rmin|
                        Some(lmin - rmin))),
                    bounds.left_upper.and_then(|lmax| bounds.right_upper.and_then(|rmax|
                        Some(lmax - rmax))),
                ))),
                int: Some(&|bounds| Ok((
                    match (bounds.left_lower, bounds.right_lower) {
                        (Some(lmin), Some(rmin)) => Some(lmin.checked_sub(rmin)
                            .ok_or_else(|| Error::from("subtraction may result in underflow or overflow"))?),
                        _ => None
                    },
                    match (bounds.left_upper, bounds.right_upper) {
                        (Some(lmax), Some(rmax)) => Some(lmax.checked_sub(rmax)
                            .ok_or_else(|| Error::from("subtraction may result in underflow or overflow"))?),
                        _ => None
                    })))
            }, num_columns)?,
            c_stability: broadcast(&left_property.c_stability, num_columns)?.iter()
                .zip(broadcast(&right_property.c_stability, num_columns)?)
                .map(|(l, r)| l.max(r)).collect(),
            num_columns: Some(num_columns),
            num_records,
            aggregator: None,
            data_type: left_property.data_type,
            dataset_id: left_property.dataset_id,
            is_not_empty: left_property.is_not_empty && right_property.is_not_empty,
            dimensionality: left_property.dimensionality
                .max(right_property.dimensionality),
            group_id: left_property.group_id
        }).into())
    }
}

type UnaryOperator<T> = Option<Box<dyn Fn(&T) -> Result<T>>>;
pub struct UnaryOperators {
    pub float: UnaryOperator<Float>,
    pub int: UnaryOperator<Integer>,
    pub str: UnaryOperator<String>,
    pub bool: UnaryOperator<bool>,
}
pub struct UnaryBounds<'a, T> {
    pub lower: &'a Option<T>,
    pub upper: &'a Option<T>,
}

type UnaryOptimizer<T> = Option<Box<dyn Fn(UnaryBounds<T>) -> Result<(Option<T>, Option<T>)>>>;
pub struct OptimizeUnaryOperators {
    pub float: UnaryOptimizer<Float>,
    pub int: UnaryOptimizer<Integer>,
}

type BinaryOperator<T> = Option<Box<dyn Fn(&T, &T) -> Result<T>>>;
pub struct BinaryOperators {
    pub float: BinaryOperator<Float>,
    pub int: BinaryOperator<Integer>,
    pub str: BinaryOperator<String>,
    pub bool: BinaryOperator<bool>,
}
pub struct BinaryBounds<'a, T> {
    pub left_lower: &'a Option<T>,
    pub left_upper: &'a Option<T>,
    pub right_lower: &'a Option<T>,
    pub right_upper: &'a Option<T>,
}

type BinaryOptimizer<'a, T> = Option<&'a dyn Fn(BinaryBounds<T>) -> Result<(Option<T>, Option<T>)>>;
pub struct OptimizeBinaryOperators<'a> {
    pub float: BinaryOptimizer<'a, Float>,
    pub int: BinaryOptimizer<'a, Integer>,
}

pub fn propagate_binary_shape(left_property: &ArrayProperties, right_property: &ArrayProperties) -> Result<(i64, Option<i64>)> {
    if !left_property.releasable && !right_property.releasable && left_property.group_id != right_property.group_id {
        return Err("data from separate partitions may not be mixed".into())
    }

    let left_num_columns = left_property.num_columns()?;
    let right_num_columns = right_property.num_columns()?;

    let left_is_column_broadcastable = left_num_columns == 1;
    let right_is_column_broadcastable = right_num_columns == 1;

    if !(left_is_column_broadcastable || right_is_column_broadcastable) && left_num_columns != right_num_columns {
        return Err("number of columns must be the same for left and right arguments, or one column must be broadcastable".into());
    }

    let output_num_columns = left_num_columns.max(right_num_columns);

    let l = left_property.num_records;
    let r = right_property.num_records;

    let left_is_row_broadcastable = left_property.releasable && l == Some(1);
    let right_is_row_broadcastable = right_property.releasable && r == Some(1);

    if !(left_is_row_broadcastable || right_is_row_broadcastable || (l == r && l.is_some())) {
        if left_property.dataset_id == right_property.dataset_id {
            return Ok((output_num_columns, None));
        }
        return Err("number of rows must be the same for left and right arguments".into());
    }

    // either left, right or both are broadcastable, so take the largest
    let output_num_records = vec![l, r].iter().filter_map(|v| *v).max().unwrap();

    Ok((output_num_columns, Some(output_num_records)))
}

pub fn propagate_unary_nature(
    data_property: &ArrayProperties,
    operator: &UnaryOperators,
    optimization_operator: &OptimizeUnaryOperators,
    output_num_columns: i64
) -> Result<Option<Nature>> {
    Ok(match data_property.nature.clone() {
        Some(nature) => match nature {
            Nature::Continuous(nature) => match (nature.lower, nature.upper) {
                (Vector1DNull::Float(min), Vector1DNull::Float(max)) => {
                    let mut output_min = Vec::new();
                    let mut output_max = Vec::new();
                    broadcast(&min, output_num_columns)?.iter()
                        .zip(broadcast(&max, output_num_columns)?.iter())
                        .try_for_each(|(min, max)| {
                            match &optimization_operator.float {
                                Some(operator) => {
                                    let (min, max) = operator(UnaryBounds{ lower: min, upper: max })?;
                                    output_min.push(min);
                                    output_max.push(max);
                                },
                                None => {
                                    output_min.push(None);
                                    output_max.push(None);
                                }
                            };
                            Ok::<_, Error>(())
                        })?;
                    Some(Nature::Continuous(NatureContinuous { lower: Vector1DNull::Float(output_min), upper: Vector1DNull::Float(output_max)}))
                }
                (Vector1DNull::Int(min), Vector1DNull::Int(max)) => {
                    let mut output_min = Vec::new();
                    let mut output_max = Vec::new();
                    broadcast(&min, output_num_columns)?.iter()
                        .zip(broadcast(&max, output_num_columns)?.iter())
                        .try_for_each(|(min, max)| {
                            match &optimization_operator.int {
                                Some(operator) => {
                                    let (min, max) = operator(UnaryBounds{ lower: min, upper: max })?;
                                    output_min.push(min);
                                    output_max.push(max);
                                },
                                None => {
                                    output_min.push(None);
                                    output_max.push(None);
                                }
                            };
                            Ok::<_, Error>(())
                        })?;
                    Some(Nature::Continuous(NatureContinuous { lower: Vector1DNull::Int(output_min), upper: Vector1DNull::Int(output_max)}))
                },
                _ => return Err("continuous bounds must be numeric and homogeneously typed".into())
            }
            Nature::Categorical(nature) => Some(Nature::Categorical(NatureCategorical { categories: match nature.categories.standardize(output_num_columns)? {
                Jagged::Float(categories) => Jagged::Float(categories.iter().map(|cats|
                    match &operator.float {
                        Some(operator) =>
                            Ok(cats.iter().map(operator).collect::<Result<Vec<_>>>()?),
                        None => Err("categories cannot be propagated for floats".into()),
                    }).collect::<Result<Vec<Vec<_>>>>()?),
                Jagged::Int(categories) => Jagged::Int(categories.iter().map(|cats|
                    match &operator.int {
                        Some(operator) =>
                            Ok(cats.iter().map(operator).collect::<Result<Vec<_>>>()?),
                        None => Err("categories cannot be propagated for integers".into()),
                    }).collect::<Result<Vec<Vec<_>>>>()?),
                Jagged::Bool(categories) => Jagged::Bool(categories.iter().map(|cats|
                    match &operator.bool {
                        Some(operator) =>
                            Ok(cats.iter().map(operator).collect::<Result<Vec<_>>>()?),
                        None => Err("categories cannot be propagated for booleans".into()),
                    }).collect::<Result<Vec<Vec<_>>>>()?),
                Jagged::Str(categories) => Jagged::Str(categories.iter().map(|cats|
                    match &operator.str {
                        Some(operator) =>
                            Ok(cats.iter().map(operator).collect::<Result<Vec<_>>>()?),
                        None => Err("categories cannot be propagated for strings".into()),
                    }).collect::<Result<Vec<Vec<_>>>>()?),
            }}))
        },
        None => None
    })
}

/// Given properties from two arguments,
/// and functions to maximize intervals and perform cartesian products,
/// infer the nature (continuous bounds, category sets) of the output data
pub fn propagate_binary_nature(
    left_property: &ArrayProperties, right_property: &ArrayProperties,
    operator: &BinaryOperators,
    optimization_operator: &OptimizeBinaryOperators,
    output_num_columns: i64
) -> Result<Option<Nature>> {

    let (left_nature, right_nature) = match (&left_property.nature, &right_property.nature) {
        (Some(l), Some(r)) => (l, r),
        _ => return Ok(None)
    };

    match (&left_nature, &right_nature) {
        (Nature::Continuous(left_nature), Nature::Continuous(right_nature)) =>
            propagate_binary_continuous_nature(left_nature, right_nature, optimization_operator, output_num_columns),

        (Nature::Categorical(left_nature), Nature::Categorical(right_nature)) =>
            propagate_binary_categorical_nature(left_nature, right_nature, operator, output_num_columns),
        _ => Ok(None)
    }
}

fn propagate_binary_continuous_nature(
    left_nature: &NatureContinuous, right_nature: &NatureContinuous,
    optimization_operator: &OptimizeBinaryOperators,
    output_num_columns: i64
) -> Result<Option<Nature>> {
    let NatureContinuous {
        lower: left_lower, upper: left_upper
    } = left_nature;

    let NatureContinuous {
        lower: right_lower, upper: right_upper
    } = right_nature;

    Ok(match (left_lower, left_upper, right_lower, right_upper) {
        (Vector1DNull::Float(lmin), Vector1DNull::Float(lmax), Vector1DNull::Float(rmin), Vector1DNull::Float(rmax)) => {
            let lmin = broadcast(&lmin, output_num_columns)?;
            let lmax = broadcast(&lmax, output_num_columns)?;
            let rmin = broadcast(&rmin, output_num_columns)?;
            let rmax = broadcast(&rmax, output_num_columns)?;

            let mut min = Vec::new();
            let mut max = Vec::new();
            lmin.iter().zip(lmax.iter()).zip(rmin.iter().zip(rmax.iter()))
                .try_for_each(|((left_min, left_max), (right_min, right_max))| {
                    match &optimization_operator.float {
                        Some(operator) => {
                            let (col_min, col_max) = operator(BinaryBounds { left_lower: left_min, left_upper: left_max, right_lower: right_min, right_upper: right_max })?;
                            min.push(col_min);
                            max.push(col_max);
                        },
                        None => {
                            min.push(None);
                            max.push(None);
                        }
                    }
                    Ok::<_, Error>(())
                })?;
            Some(Nature::Continuous(NatureContinuous { lower: Vector1DNull::Float(min), upper: Vector1DNull::Float(max)}))
        },
        (Vector1DNull::Int(lmin), Vector1DNull::Int(lmax), Vector1DNull::Int(rmin), Vector1DNull::Int(rmax)) => {
            let lmin = broadcast(&lmin, output_num_columns)?;
            let lmax = broadcast(&lmax, output_num_columns)?;
            let rmin = broadcast(&rmin, output_num_columns)?;
            let rmax = broadcast(&rmax, output_num_columns)?;

            let mut min = Vec::new();
            let mut max = Vec::new();
            lmin.iter().zip(lmax.iter()).zip(rmin.iter().zip(rmax.iter()))
                .try_for_each(|((left_min, left_max), (right_min, right_max))| {
                    match &optimization_operator.int {
                        Some(operator) => {
                            let (col_min, col_max) = operator(BinaryBounds { left_lower: left_min, left_upper: left_max, right_lower: right_min, right_upper: right_max })?;
                            min.push(col_min);
                            max.push(col_max);
                        },
                        None => {
                            min.push(None);
                            max.push(None);
                        }
                    }
                    Ok::<_, Error>(())
                })?;
            Some(Nature::Continuous(NatureContinuous { lower: Vector1DNull::Int(min), upper: Vector1DNull::Int(max)}))
        },
        _ => return Err("continuous bounds must be numeric and homogeneously typed".into())
    })
}


fn propagate_binary_categorical_nature(
    left_nature: &NatureCategorical, right_nature: &NatureCategorical,
    operator: &BinaryOperators,
    output_num_columns: i64
) -> Result<Option<Nature>> {
    Ok(Some(Nature::Categorical(NatureCategorical {
        categories: match (left_nature.categories.clone().standardize(output_num_columns)?, right_nature.categories.clone().standardize(output_num_columns)?) {
            (Jagged::Float(left), Jagged::Float(right)) =>
                Jagged::Float(left.iter().zip(right.iter()).map(|(left, right)|
                    match &operator.float {
                        Some(operator) => Ok(left.iter()
                            .map(|left| right.iter()
                                .map(|right| operator(left, right))
                                .collect::<Result<Vec<_>>>())
                            .collect::<Result<Vec<Vec<_>>>>()?
                            .into_iter().flatten().collect::<Vec<_>>()),
                        None => Err("categories cannot be propagated for floats".into()),
                    }).collect::<Result<Vec<Vec<_>>>>()?),
            (Jagged::Int(left), Jagged::Int(right)) =>
                Jagged::Int(left.iter().zip(right.iter()).map(|(left, right)|
                    match &operator.int {
                        Some(operator) => Ok(left.iter()
                            .map(|left| right.iter()
                                .map(|right| operator(left, right))
                                .collect::<Result<Vec<_>>>())
                            .collect::<Result<Vec<Vec<_>>>>()?
                            .into_iter().flatten().collect::<Vec<_>>()),
                        None => Err("categories cannot be propagated for integers".into()),
                    }).collect::<Result<Vec<Vec<_>>>>()?),
            (Jagged::Bool(left), Jagged::Bool(right)) =>
                Jagged::Bool(left.iter().zip(right.iter()).map(|(left, right)|
                    match &operator.bool {
                        Some(operator) => Ok(left.iter()
                            .map(|left| right.iter()
                                .map(|right| operator(left, right))
                                .collect::<Result<Vec<_>>>())
                            .collect::<Result<Vec<Vec<_>>>>()?
                            .into_iter().flatten().collect::<Vec<_>>()),
                        None => Err("categories cannot be propagated for booleans".into()),
                    }).collect::<Result<Vec<Vec<_>>>>()?),
            (Jagged::Str(left), Jagged::Str(right)) =>
                Jagged::Str(left.iter().zip(right.iter()).map(|(left, right)|
                    match &operator.str {
                        Some(operator) => Ok(left.iter()
                            .map(|left| right.iter()
                                .map(|right| operator(left, right))
                                .collect::<Result<Vec<_>>>())
                            .collect::<Result<Vec<Vec<_>>>>()?
                            .into_iter().flatten().collect::<Vec<_>>()),
                        None => Err("categories cannot be propagated for strings".into()),
                    }).collect::<Result<Vec<Vec<_>>>>()?),
            _ => return Err("natures must be homogeneously typed".into())
        }.deduplicate()?
    })))
}

fn broadcast<T: Clone>(data: &[T], length: i64) -> Result<Vec<T>> {
    if data.len() as i64 == length {
        return Ok(data.to_owned());
    }

    if data.len() != 1 {
        return Err("could not broadcast vector".into());
    }

    Ok((0..length).map(|_| data[0].clone()).collect())
}
