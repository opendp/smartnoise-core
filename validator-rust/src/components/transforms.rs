use crate::errors::*;

use std::collections::HashMap;
use crate::base::{Nature, Vector1DNull, NodeProperties, ArrayND, NatureCategorical, standardize_categorical_argument, Vector2DJagged, standardize_numeric_argument, Vector1D, ArrayNDProperties, ValueProperties, prepend};

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
            nullity: left_property.nullity && right_property.nullity,
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
            nullity: left_property.nullity && right_property.nullity,
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


// TODO: swap max/min if negative
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

        Ok(ArrayNDProperties {
            nullity: left_property.nullity && right_property.nullity,
            releasable: left_property.releasable && right_property.releasable,
            nature: propagate_binary_nature(&left_property, &right_property, &Operators {
                f64: Some(Box::new(|l: &f64, r: &f64| l / r)),
                i64: Some(Box::new(|l: &i64, r: &i64| l / r)),
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


// TODO: swap max/min if negative
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
            nullity: left_property.nullity && right_property.nullity,
            releasable: left_property.releasable && right_property.releasable,
            nature: propagate_binary_nature(&left_property, &right_property, &Operators {
                f64: Some(Box::new(|l: &f64, r: &f64| l * r)),
                i64: Some(Box::new(|l: &i64, r: &i64| l * r)),
                str: None, bool: None
            }, &num_columns)?,
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

// TODO: swap max/min if negative
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
            nullity: left_property.nullity && right_property.nullity,
            releasable: left_property.releasable && right_property.releasable,
            nature: propagate_binary_nature(&left_property, &right_property, &Operators {
                f64: Some(Box::new(|l: &f64, r: &f64| l.powf(*r))),
                i64: Some(Box::new(|l: &i64, r: &i64| l.pow(*r as u32))),
                str: None, bool: None
            }, &num_columns)?,
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
            nullity: left_property.nullity && right_property.nullity,
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

        Ok(ArrayNDProperties {
            nullity: true,
            releasable: left_property.releasable && right_property.releasable,
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
        _properties: &base::NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}

impl Component for proto::Remainder {
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
            nullity: true,
            releasable: left_property.releasable && right_property.releasable,
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
        _properties: &base::NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
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
            nullity: left_property.nullity && right_property.nullity,
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
            nullity: left_property.nullity && right_property.nullity,
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
            nullity: left_property.nullity && right_property.nullity,
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
            nullity: left_property.nullity && right_property.nullity,
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
            nullity: left_property.nullity && right_property.nullity,
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
            nullity: left_property.nullity && right_property.nullity,
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
                                    (Some(l), Some(r)) => Some(operator(l, &r)),
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