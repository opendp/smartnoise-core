//! Core data structures

use crate::errors::*;

use crate::{proto, base, Integer, Float};

use ndarray::prelude::Ix1;

use std::collections::HashMap;
use ndarray::{ArrayD, arr0, Dimension, arr1};

use crate::utilities::{standardize_categorical_argument, deduplicate, get_common_value};
use indexmap::IndexMap;
use crate::utilities::serial::{parse_argument_node_ids, serialize_index_key};
use std::ops::{Add, Div, Mul};
use itertools::Itertools;

/// The universal data representation.
///
/// Arguments to components are hash-maps of Value and the result of a component is a Value.
/// The Value is also used in the validator for public arguments.
///
/// The Value has a one-to-one mapping to a protobuf Value.
///
/// Components unwrap arguments into more granular types, like ndarray::Array1<f64>,
/// run a computation, and then repackage the result back into a Value.
#[derive(Clone, Debug)]
pub enum Value {
    /// An arbitrary-dimensional homogeneously typed array
    Array(Array),
    /// An index-map, where the keys are enum-typed and the values are of type Value
    Dataframe(IndexMap<IndexKey, Value>),
    /// An index-map, where the keys are enum-typed and the values are of type Value
    Partitions(IndexMap<IndexKey, Value>),
    /// A 2D homogeneously typed matrix, where the columns may be unknown and the column lengths may be inconsistent
    Jagged(Jagged),
    /// An arbitrary function expressed in the graph language
    Function(proto::Function),
}

impl Value {
    /// Retrieve an Array from a Value, assuming the Value contains an Array
    pub fn array(self) -> Result<Array> {
        match self {
            Value::Array(array) => Ok(array),
            _ => Err("value must be an array".into())
        }
    }
    pub fn ref_array(&self) -> Result<&Array> {
        match self {
            Value::Array(array) => Ok(array),
            _ => Err("value must be an array".into())
        }
    }
    /// Retrieve Jagged from a Value, assuming the Value contains Jagged
    pub fn jagged(self) -> Result<Jagged> {
        match self {
            Value::Jagged(jagged) => Ok(jagged),
            _ => Err("value must be a jagged array".into())
        }
    }
    pub fn ref_jagged(&self) -> Result<&Jagged> {
        match self {
            Value::Jagged(array) => Ok(array),
            _ => Err("value must be a jagged array".into())
        }
    }

    pub fn dataframe(self) -> Result<IndexMap<IndexKey, Value>> {
        match self {
            Value::Dataframe(dataframe) => Ok(dataframe),
            _ => Err("value must be a dataframe".into())
        }
    }
    pub fn partitions(self) -> Result<IndexMap<IndexKey, Value>> {
        match self {
            Value::Partitions(partitions) => Ok(partitions),
            _ => Err("value must be partitions".into())
        }
    }
    pub fn ref_partitions(&self) -> Result<&IndexMap<IndexKey, Value>> {
        match self {
            Value::Partitions(array) => Ok(array),
            _ => Err("value must be partitions".into())
        }
    }

    pub fn function(self) -> Result<proto::Function> {
        match self {
            Value::Function(function) => Ok(function),
            _ => Err("value must be a function".into())
        }
    }

    pub fn from_index_key(key: IndexKey) -> Result<Self> {
        Ok(match key {
            IndexKey::Int(key) => key.into(),
            IndexKey::Str(key) => key.into(),
            IndexKey::Bool(key) => key.into(),
            IndexKey::Tuple(key) => match get_common_value(&key.iter().map(|v| Ok(match v {
                IndexKey::Int(_) => DataType::Int,
                IndexKey::Str(_) => DataType::Str,
                IndexKey::Bool(_) => DataType::Bool,
                _ => return Err("index keys may not be nested".into())
            })).collect::<Result<Vec<DataType>>>()?) {
                Some(DataType::Int) => arr1(&key.into_iter().map(|v| match v {
                    IndexKey::Int(v) => v,
                    _ => unreachable!()
                }).collect::<Vec<_>>()).into_dyn().into(),
                Some(DataType::Bool) => arr1(&key.into_iter().map(|v| match v {
                    IndexKey::Bool(v) => v,
                    _ => unreachable!()
                }).collect::<Vec<_>>()).into_dyn().into(),
                Some(DataType::Str) => arr1(&key.into_iter().map(|v| match v {
                    IndexKey::Str(v) => v,
                    _ => unreachable!()
                }).collect::<Vec<_>>()).into_dyn().into(),
                _ => return Err("index key tuples may not currently have mixed types".into())
            }
        })
    }
}

impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Array(lhs), Value::Array(rhs)) => lhs == rhs,
            _ => false
        }
    }
}

impl PartialEq for Array {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Array::Bool(lhs), Array::Bool(rhs)) => lhs == rhs,
            (Array::Float(lhs), Array::Float(rhs)) => lhs == rhs,
            (Array::Int(lhs), Array::Int(rhs)) => lhs == rhs,
            _ => false
        }
    }
}

// build Value from other types with .into()
impl From<bool> for Value {
    fn from(value: bool) -> Self {
        Value::Array(Array::Bool(arr0(value).into_dyn()))
    }
}

impl From<Float> for Value {
    fn from(value: Float) -> Self {
        Value::Array(Array::Float(arr0(value).into_dyn()))
    }
}

impl From<Integer> for Value {
    fn from(value: Integer) -> Self {
        Value::Array(Array::Int(arr0(value).into_dyn()))
    }
}

impl From<String> for Value {
    fn from(value: String) -> Self {
        Value::Array(Array::Str(arr0(value).into_dyn()))
    }
}

impl<T> From<ndarray::Array<bool, ndarray::Dim<T>>> for Value
    where ndarray::Dim<T>: Dimension {
    fn from(value: ndarray::Array<bool, ndarray::Dim<T>>) -> Self {
        Value::Array(Array::Bool(value.into_dyn()))
    }
}

impl<T> From<ndarray::Array<Integer, ndarray::Dim<T>>> for Value
    where ndarray::Dim<T>: Dimension {
    fn from(value: ndarray::Array<Integer, ndarray::Dim<T>>) -> Self {
        Value::Array(Array::Int(value.into_dyn()))
    }
}

impl<T> From<ndarray::Array<Float, ndarray::Dim<T>>> for Value
    where ndarray::Dim<T>: Dimension {
    fn from(value: ndarray::Array<Float, ndarray::Dim<T>>) -> Self {
        Value::Array(Array::Float(value.into_dyn()))
    }
}

impl<T> From<ndarray::Array<String, ndarray::Dim<T>>> for Value
    where ndarray::Dim<T>: Dimension {
    fn from(value: ndarray::Array<String, ndarray::Dim<T>>) -> Self {
        Value::Array(Array::Str(value.into_dyn()))
    }
}

impl From<std::num::TryFromIntError> for Error {
    fn from(value: std::num::TryFromIntError) -> Self {
        format!("{}", value).into()
    }
}

impl From<std::num::ParseIntError> for Error {
    fn from(value: std::num::ParseIntError) -> Self {
        format!("{}", value).into()
    }
}

impl From<ndarray_stats::errors::MinMaxError> for Error {
    fn from(value: ndarray_stats::errors::MinMaxError) -> Self {
        format!("min-max error: {}", value).into()
    }
}

impl From<ndarray::ShapeError> for Error {
    fn from(value: ndarray::ShapeError) -> Self {
        format!("shape error: {:?}", value).into()
    }
}


/// The universal n-dimensional array representation.
///
/// ndarray ArrayD's are artificially allowed to be 0, 1 or 2-dimensional.
/// The first axis denotes the number rows/observations. The second axis the number of columns.
///
/// The Array has a one-to-one mapping to a protobuf ArrayND.
#[derive(Clone, Debug)]
pub enum Array {
    Bool(ArrayD<bool>),
    Int(ArrayD<Integer>),
    Float(ArrayD<Float>),
    Str(ArrayD<String>),
}

impl Array {
    /// Retrieve the float ndarray, assuming the data type of the ArrayND is float
    pub fn float(self) -> Result<ArrayD<Float>> {
        match self {
            Array::Float(x) => Ok(x),
            Array::Int(_) => Err("atomic type: expected float, got integer".into()),
            Array::Bool(_) => Err("atomic type: expected float, got bool".into()),
            Array::Str(_) => Err("atomic type: expected float, got string".into()),
        }
    }
    pub fn ref_float(&self) -> Result<&ArrayD<Float>> {
        match self {
            Array::Float(x) => Ok(x),
            Array::Int(_) => Err("atomic type: expected float, got integer".into()),
            Array::Bool(_) => Err("atomic type: expected float, got bool".into()),
            Array::Str(_) => Err("atomic type: expected float, got string".into()),
        }
    }
    pub fn first_float(&self) -> Result<Float> {
        match self {
            Array::Float(x) => {
                if x.len() != 1 {
                    return Err("non-singleton array passed for an argument that must be scalar".into());
                }
                Ok(x.first().unwrap().to_owned())
            }
            _ => Err("value must be float".into())
        }
    }
    pub fn vec_float(self, optional_length: Option<i64>) -> Result<Vec<Float>> {
        let data = self.float()?;
        let err_msg = "failed attempt to cast float ArrayD to vector".into();
        match data.ndim() {
            0 => match (optional_length, data.first()) {
                (Some(length), Some(v)) => Ok((0..length).map(|_| *v).collect()),
                _ => Err(err_msg)
            },
            1 => Ok(data.into_dimensionality::<Ix1>()?.to_vec()),
            _ => Err(err_msg)
        }
    }
    /// Retrieve the i64 ndarray, assuming the data type of the ArrayND is i64
    pub fn int(self) -> Result<ArrayD<Integer>> {
        match self {
            Array::Int(x) => Ok(x),
            Array::Float(_) => Err("atomic type: expected integer, got float".into()),
            Array::Bool(_) => Err("atomic type: expected integer, got bool".into()),
            Array::Str(_) => Err("atomic type: expected integer, got string".into()),
        }
    }
    /// Retrieve the i64 ndarray, assuming the data type of the ArrayND is i64
    pub fn ref_int(&self) -> Result<&ArrayD<Integer>> {
        match self {
            Array::Int(x) => Ok(x),
            Array::Float(_) => Err("atomic type: expected integer, got float".into()),
            Array::Bool(_) => Err("atomic type: expected integer, got bool".into()),
            Array::Str(_) => Err("atomic type: expected integer, got string".into()),
        }
    }
    pub fn first_int(&self) -> Result<Integer> {
        match self {
            Array::Int(x) => {
                if x.len() != 1 {
                    return Err("non-singleton array passed for an argument that must be scalar".into());
                }
                Ok(x.first().unwrap().to_owned())
            }
            _ => Err("value must be an integer".into())
        }
    }
    pub fn vec_int(self, optional_length: Option<i64>) -> Result<Vec<Integer>> {
        let data = self.int()?;
        let err_msg = "failed attempt to cast i64 ArrayD to vector".into();
        match data.ndim() {
            0 => match (optional_length, data.first()) {
                (Some(length), Some(v)) => Ok((0..length).map(|_| *v).collect()),
                _ => Err(err_msg)
            },
            1 => Ok(data.into_dimensionality::<Ix1>()?.to_vec()),
            _ => Err(err_msg)
        }
    }
    /// Retrieve the String ndarray, assuming the data type of the ArrayND is String
    pub fn string(self) -> Result<ArrayD<String>> {
        match self {
            Array::Str(x) => Ok(x),
            Array::Int(_) => Err("atomic type: expected string, got integer".into()),
            Array::Bool(_) => Err("atomic type: expected string, got bool".into()),
            Array::Float(_) => Err("atomic type: expected string, got float".into()),
        }
    }
    pub fn ref_string(&self) -> Result<&ArrayD<String>> {
        match self {
            Array::Str(x) => Ok(x),
            Array::Int(_) => Err("atomic type: expected string, got integer".into()),
            Array::Bool(_) => Err("atomic type: expected string, got bool".into()),
            Array::Float(_) => Err("atomic type: expected string, got float".into()),
        }
    }
    pub fn first_string(&self) -> Result<String> {
        match self {
            Array::Str(x) => {
                if x.len() != 1 {
                    return Err("non-singleton array passed for an argument that must be scalar".into());
                }
                Ok(x.first().unwrap().to_owned())
            }
            _ => Err("value must be a string".into())
        }
    }
    /// Retrieve the bool ndarray, assuming the data type of the ArrayND is bool
    pub fn bool(self) -> Result<ArrayD<bool>> {
        match self {
            Array::Bool(x) => Ok(x),
            Array::Int(_) => Err("atomic type: expected bool, got integer".into()),
            Array::Str(_) => Err("atomic type: expected bool, got string".into()),
            Array::Float(_) => Err("atomic type: expected bool, got float".into()),
        }
    }
    pub fn ref_bool(&self) -> Result<&ArrayD<bool>> {
        match self {
            Array::Bool(x) => Ok(x),
            Array::Int(_) => Err("atomic type: expected bool, got integer".into()),
            Array::Str(_) => Err("atomic type: expected bool, got string".into()),
            Array::Float(_) => Err("atomic type: expected bool, got float".into()),
        }
    }
    pub fn first_bool(&self) -> Result<bool> {
        match self {
            Array::Bool(x) => {
                if x.len() != 1 {
                    return Err("non-singleton array passed for an argument that must be scalar".into());
                }
                Ok(x.first().unwrap().to_owned())
            }
            _ => Err("value must be a bool".into())
        }
    }

    pub fn shape(&self) -> Vec<usize> {
        match self {
            Array::Bool(array) => array.shape().to_owned(),
            Array::Float(array) => array.shape().to_owned(),
            Array::Int(array) => array.shape().to_owned(),
            Array::Str(array) => array.shape().to_owned()
        }
    }
    pub fn num_records(&self) -> Result<usize> {
        let shape = self.shape();
        match shape.len() {
            0 => Ok(1),
            1 | 2 => Ok(shape[0]),
            _ => Err("arrays may have max dimensionality of 2".into())
        }
    }
    pub fn num_columns(&self) -> Result<usize> {
        let shape = self.shape();
        match shape.len() {
            0 => Ok(1),
            1 => Ok(1),
            2 => Ok(shape[1]),
            _ => Err("arrays may have max dimensionality of 2".into())
        }
    }
}

/// The universal jagged array representation.
///
/// Typically used to store categorically clamped values.
/// In practice, use is limited to public categories over multiple columns, and the upper triangular covariance matrix
///
/// Jagged has a one-to-one mapping to a protobuf Vector2DJagged.
#[derive(Clone, Debug)]
pub enum Jagged {
    Bool(Vec<Vec<bool>>),
    Int(Vec<Vec<Integer>>),
    Float(Vec<Vec<Float>>),
    Str(Vec<Vec<String>>),
}

impl Jagged {
    /// Retrieve the f64 jagged matrix, assuming the data type of the jagged matrix is f64, and assuming all columns are defined
    pub fn float(&self) -> Result<Vec<Vec<Float>>> {
        match self {
            Jagged::Float(data) => Ok(data.clone()),
            _ => Err("expected float type on a non-float Jagged matrix".into())
        }
    }
    /// Retrieve the i64 jagged matrix, assuming the data type of the jagged matrix is i64
    pub fn int(&self) -> Result<Vec<Vec<Integer>>> {
        match self {
            Jagged::Int(data) => Ok(data.clone()),
            _ => Err("expected int type on a non-int Jagged matrix".into())
        }
    }
    /// Retrieve the String jagged matrix, assuming the data type of the jagged matrix is String
    pub fn string(&self) -> Result<Vec<Vec<String>>> {
        match self {
            Jagged::Str(data) => Ok(data.clone()),
            _ => Err("expected string type on a non-string Jagged matrix".into())
        }
    }
    /// Retrieve the bool jagged matrix, assuming the data type of the jagged matrix is bool
    pub fn bool(&self) -> Result<Vec<Vec<bool>>> {
        match self {
            Jagged::Bool(data) => Ok(data.clone()),
            _ => Err("expected bool type on a non-bool Jagged matrix".into())
        }
    }
    pub fn num_columns(&self) -> i64 {
        match self {
            Jagged::Bool(vector) => vector.len() as i64,
            Jagged::Float(vector) => vector.len() as i64,
            Jagged::Int(vector) => vector.len() as i64,
            Jagged::Str(vector) => vector.len() as i64,
        }
    }
    pub fn num_records(&self) -> Vec<i64> {
        match self {
            Jagged::Bool(value) => value.iter()
                .map(|column| column.len() as i64).collect(),
            Jagged::Float(value) => value.iter()
                .map(|column| column.len() as i64).collect(),
            Jagged::Int(value) => value.iter()
                .map(|column| column.len() as i64).collect(),
            Jagged::Str(value) => value.iter()
                .map(|column| column.len() as i64).collect(),
        }
    }

    pub fn deduplicate(&self) -> Result<Jagged> {
        match self.to_owned() {
            Jagged::Float(_) =>
                Err("float data may not be categorical".into()),
            Jagged::Int(categories) => Ok(categories.into_iter()
                .map(|v| v.into_iter().unique().collect())
                .collect::<Vec<Vec<Integer>>>().into()),
            Jagged::Bool(categories) => Ok(categories.into_iter()
                .map(deduplicate)
                .collect::<Vec<Vec<bool>>>().into()),
            Jagged::Str(categories) => Ok(categories.into_iter()
                .map(deduplicate)
                .collect::<Vec<Vec<String>>>().into()),
        }
    }

    pub fn standardize(self, num_columns: i64) -> Result<Jagged> {
        match self {
            Jagged::Float(_) =>
                Err("float data may not be categorical".into()),
            Jagged::Int(categories) =>
                Ok(standardize_categorical_argument(categories, num_columns)?.into()),
            Jagged::Bool(categories) =>
                Ok(standardize_categorical_argument(categories, num_columns)?.into()),
            Jagged::Str(categories) =>
                Ok(standardize_categorical_argument(categories, num_columns)?.into()),
        }
    }

    pub fn data_type(&self) -> DataType {
        match self {
            Jagged::Int(_) => DataType::Int,
            Jagged::Float(_) => DataType::Float,
            Jagged::Bool(_) => DataType::Bool,
            Jagged::Str(_) => DataType::Str,
        }
    }

    pub fn to_index_keys(&self) -> Result<Vec<Vec<IndexKey>>> {
        Ok(match self {
            Jagged::Bool(categories) =>
                categories.iter()
                    .map(|col| col.iter().cloned()
                        .map(IndexKey::from).collect()).collect::<Vec<Vec<IndexKey>>>(),
            Jagged::Str(categories) =>
                categories.iter()
                    .map(|col| col.iter().cloned()
                        .map(IndexKey::from).collect()).collect(),
            Jagged::Int(categories) =>
                categories.iter()
                    .map(|col| col.iter().cloned()
                        .map(IndexKey::from).collect()).collect(),
            _ => return Err("partitioning based on floats is not supported".into())
        })
    }
}


impl From<Vec<Vec<Float>>> for Jagged {
    fn from(value: Vec<Vec<Float>>) -> Self {
        Jagged::Float(value)
    }
}

impl From<Vec<Vec<Integer>>> for Jagged {
    fn from(value: Vec<Vec<Integer>>) -> Self {
        Jagged::Int(value)
    }
}

impl From<Vec<Vec<bool>>> for Jagged {
    fn from(value: Vec<Vec<bool>>) -> Self {
        Jagged::Bool(value)
    }
}

impl From<Vec<Vec<String>>> for Jagged {
    fn from(value: Vec<Vec<String>>) -> Self {
        Jagged::Str(value)
    }
}


/// Derived properties for the universal value.
///
/// The ValueProperties has a one-to-one mapping to a protobuf ValueProperties.
#[allow(clippy::large_enum_variant)]
#[derive(Clone, Debug)]
pub enum ValueProperties {
    Dataframe(DataframeProperties),
    Partitions(PartitionsProperties),
    Array(ArrayProperties),
    Jagged(JaggedProperties),
    Function(proto::FunctionProperties),
}


impl ValueProperties {
    /// Retrieve properties corresponding to an ArrayND, assuming the corresponding data value is actually the ArrayND variant
    pub fn array(&self) -> Result<&ArrayProperties> {
        match self {
            ValueProperties::Array(array) => Ok(array),
            _ => Err("value must be an array".into())
        }
    }
    /// Retrieve properties corresponding to an Indexmap, assuming the corresponding data value is actually the Indexmap variant
    pub fn dataframe(&self) -> Result<&DataframeProperties> {
        match self {
            ValueProperties::Dataframe(value) => Ok(value),
            _ => Err("value must be a dataframe".into())
        }
    }
    pub fn partitions(&self) -> Result<&PartitionsProperties> {
        match self {
            ValueProperties::Partitions(value) => Ok(value),
            _ => Err("value must be a partition".into())
        }
    }

    /// Retrieve properties corresponding to an Vector2DJagged, assuming the corresponding data value is actually the Vector2DJagged variant
    pub fn jagged(&self) -> Result<&JaggedProperties> {
        match self {
            ValueProperties::Jagged(value) => Ok(value),
            _ => Err("value must be jagged".into())
        }
    }

    pub fn is_public(&self) -> bool {
        match self {
            ValueProperties::Array(v) => v.releasable,
            ValueProperties::Jagged(v) => v.releasable,
            ValueProperties::Dataframe(v) => v.children.values().all(Self::is_public),
            ValueProperties::Partitions(v) => v.children.values().all(Self::is_public),
            ValueProperties::Function(v) => v.releasable,
        }
    }
}


impl From<ArrayProperties> for ValueProperties {
    fn from(value: ArrayProperties) -> Self {
        ValueProperties::Array(value)
    }
}

impl From<DataframeProperties> for ValueProperties {
    fn from(value: DataframeProperties) -> Self {
        ValueProperties::Dataframe(value)
    }
}

impl From<PartitionsProperties> for ValueProperties {
    fn from(value: PartitionsProperties) -> Self {
        ValueProperties::Partitions(value)
    }
}

impl From<JaggedProperties> for ValueProperties {
    fn from(value: JaggedProperties) -> Self {
        ValueProperties::Jagged(value)
    }
}

/// Derived properties for a dataframe.
#[derive(Clone, Debug)]
pub struct DataframeProperties {
    /// properties for each of the columns in the dataframe
    pub children: IndexMap<IndexKey, ValueProperties>,
}

/// Derived properties for a partition.
#[derive(Clone, Debug)]
pub struct PartitionsProperties {
    /// properties for each of the partitions in the indexmap
    pub children: IndexMap<IndexKey, ValueProperties>,
}

impl PartitionsProperties {
    pub fn num_records(&self) -> Result<Option<i64>> {
        Ok(self.children.values()
            .map(|v: &ValueProperties| match v {
                ValueProperties::Partitions(v) => v.num_records(),
                ValueProperties::Dataframe(v) => v.num_records(),
                ValueProperties::Array(v) => Ok(v.num_records),
                _ => Err("invalid Value type for counting records".into())
            })
            .collect::<Result<Vec<Option<i64>>>>()?.into_iter()
            .try_fold(0, |sum, v| v.map(|v| sum + v)))
    }

    pub fn from_values(&self, values: Vec<ValueProperties>) -> IndexMap<IndexKey, ValueProperties> {
        self.children.keys().cloned()
            .zip(values).collect::<IndexMap<base::IndexKey, ValueProperties>>()
    }
}

impl DataframeProperties {
    pub fn num_records(&self) -> Result<Option<i64>> {
        get_common_value(&self.children.values()
            .map(|v| Ok(v.array()?.num_records))
            .collect::<Result<Vec<Option<i64>>>>()?)
            .ok_or_else(|| "dataframe columns must share the same number of rows".into())
    }

    pub fn from_values(&self, values: Vec<ValueProperties>) -> IndexMap<IndexKey, ValueProperties> {
        self.children.keys().cloned()
            .zip(values).collect::<IndexMap<base::IndexKey, ValueProperties>>()
    }
}


/// Derived properties for the universal ArrayND.
///
/// The ArrayNDProperties has a one-to-one mapping to a protobuf ArrayNDProperties.
#[derive(Clone, Debug)]
pub struct ArrayProperties {
    /// Defined if the number of records is known statically (set by the resize component)
    pub num_records: Option<i64>,
    pub num_columns: Option<i64>,
    /// true if the data may contain null values
    pub nullity: bool,
    /// set to true by the mechanisms. Acts as a filter on the values in the release
    pub releasable: bool,
    /// amplification of privacy usage by unstable data transformations, or possibility of duplicated records
    pub c_stability: Vec<Float>,
    /// set when data is aggregated, used to help compute sensitivity from the mechanisms
    pub aggregator: Option<AggregatorProperties>,
    /// either min/max or categories
    pub nature: Option<Nature>,
    /// f64, i64, bool, String
    pub data_type: DataType,
    /// index of last Materialize or Filter node, where dataset was created
    /// used to determine if arrays are conformable even when N is not known
    pub dataset_id: Option<i64>,
    /// true if the number of rows is known to not be length zero
    pub is_not_empty: bool,
    /// number of axes in the array
    pub dimensionality: Option<i64>,
    /// used for tracking subpartitions
    pub group_id: Vec<GroupId>,
    /// used to determine if order of rows has changed
    pub naturally_ordered: bool
}


/// Derived properties for the universal Vector2DJagged.
///
/// JaggedProperties has a one-to-one mapping to the protobuf JaggedProperties message.
#[derive(Clone, Debug)]
pub struct JaggedProperties {
    /// number of records per column
    pub num_records: Option<Vec<i64>>,
    /// true if the data may contain null values
    pub nullity: bool,
    /// set when data is aggregated, used to help compute sensitivity from the mechanisms
    pub aggregator: Option<AggregatorProperties>,
    /// either min/max or categories
    pub nature: Option<Nature>,
    /// type of data
    pub data_type: DataType,
    pub releasable: bool,
}

impl JaggedProperties {
    pub fn num_records(&self) -> Result<Vec<i64>> {
        self.num_records.clone().ok_or_else(|| "number of records is not defined".into())
    }

    pub fn num_columns(&self) -> Result<i64> {
        Ok(self.num_records()?.len() as i64)
    }
}

impl ArrayProperties {
    pub fn lower_float_option(&self) -> Result<Vec<Option<Float>>> {
        match self.nature.to_owned() {
            Some(value) => match value {
                Nature::Continuous(continuous) => match continuous.lower {
                    Vector1DNull::Float(bound) => Ok(bound),
                    Vector1DNull::Int(bound) => Ok(bound.into_iter()
                        .map(|v_opt| v_opt.map(|v| v as Float)).collect()),
                    _ => Err("lower must be numeric".into())
                },
                _ => Err("lower must be an array".into())
            },
            None => Err("continuous nature for lower is not defined".into())
        }
    }
    pub fn lower_float(&self) -> Result<Vec<Float>> {
        let bound = self.lower_float_option()?;
        let value = bound.iter().filter_map(|v| v.to_owned()).collect::<Vec<Float>>();
        if bound.len() == value.len() { Ok(value) } else { Err("not all lower bounds are known".into()) }
    }
    pub fn upper_float_option(&self) -> Result<Vec<Option<Float>>> {
        match self.nature.to_owned() {
            Some(value) => match value {
                Nature::Continuous(continuous) => match continuous.upper {
                    Vector1DNull::Float(bound) => Ok(bound),
                    Vector1DNull::Int(bound) => Ok(bound.into_iter()
                        .map(|v_opt| v_opt.map(|v| v as Float)).collect()),
                    _ => Err("upper must be numeric".into())
                },
                _ => Err("upper must be an array".into())
            },
            None => Err("continuous nature for upper is not defined".into())
        }
    }
    pub fn upper_float(&self) -> Result<Vec<Float>> {
        let bound = self.upper_float_option()?;
        let value = bound.iter().filter_map(|v| v.to_owned()).collect::<Vec<Float>>();
        if bound.len() == value.len() { Ok(value) } else { Err("not all upper bounds are known".into()) }
    }

    pub fn lower_int_option(&self) -> Result<Vec<Option<Integer>>> {
        match self.nature.to_owned() {
            Some(value) => match value {
                Nature::Continuous(continuous) => match continuous.lower {
                    Vector1DNull::Int(bound) => Ok(bound),
                    _ => Err("lower must be composed of integers".into())
                },
                _ => Err("lower must be an array".into())
            },
            None => Err("continuous nature for lower is not defined".into())
        }
    }
    pub fn lower_int(&self) -> Result<Vec<Integer>> {
        let bound = self.lower_int_option()?;
        let value = bound.iter().filter_map(|v| v.to_owned()).collect::<Vec<Integer>>();
        if bound.len() == value.len() { Ok(value) } else { Err("not all lower bounds are known".into()) }
    }
    pub fn upper_int_option(&self) -> Result<Vec<Option<Integer>>> {
        match self.nature.to_owned() {
            Some(value) => match value {
                Nature::Continuous(continuous) => match continuous.upper {
                    Vector1DNull::Int(bound) => Ok(bound),
                    _ => Err("upper must be composed of integers".into())
                },
                _ => Err("upper must be an array".into())
            },
            None => Err("continuous nature for upper is not defined".into())
        }
    }
    pub fn upper_int(&self) -> Result<Vec<Integer>> {
        let bound = self.upper_int_option()?;
        let value = bound.iter().filter_map(|v| v.to_owned()).collect::<Vec<Integer>>();
        if bound.len() == value.len() { Ok(value) } else { Err("not all upper bounds are known".into()) }
    }

    pub fn categories(&self) -> Result<Jagged> {
        match self.nature.to_owned() {
            Some(nature) => match nature {
                Nature::Categorical(nature) => Ok(nature.categories),
                _ => Err("categories is not defined".into())
            },
            None => Err("categorical nature is not defined".into())
        }
    }
    pub fn assert_non_null(&self) -> Result<()> {
        if self.nullity { Err("data may contain nullity when non-nullity is required".into()) } else { Ok(()) }
    }
    pub fn assert_is_not_empty(&self) -> Result<()> {
        if self.is_not_empty { Ok(()) } else { Err("data may be empty when non-emptiness is required".into()) }
    }
    pub fn assert_is_releasable(&self) -> Result<()> {
        if self.releasable { Ok(()) } else { Err("data is not releasable when releasability is required".into()) }
    }
    pub fn num_columns(&self) -> Result<i64> {
        self.num_columns.ok_or_else(|| "number of columns is not defined".into())
    }
    pub fn num_records(&self) -> Result<i64> {
        self.num_records.ok_or_else(|| "number of records is not defined".into())
    }
    pub fn assert_is_not_aggregated(&self) -> Result<()> {
        if self.aggregator.is_some() { Err("aggregated data may not be manipulated".into()) } else { Ok(()) }
    }
}

/// Fundamental data types for ArrayNDs and Vector2DJagged Values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataType {
    Unknown,
    Bool,
    Str,
    Float,
    Int,
}


/// Properties of an aggregation applied to a Value.
///
/// The component variant is passed forward in the graph until a Mechanism needs sensitivity.
/// Since aggregators implement compute_sensitivity,
/// the compute_sensitivity implemented for whatever aggregator was used earlier in the graph is accessible to the mechanism.
///
/// The AggregatorProperties has a one-to-one mapping to a protobuf AggregatorProperties.
#[derive(Clone, Debug)]
pub struct AggregatorProperties {
    pub component: proto::component::Variant,
    pub properties: IndexMap<IndexKey, ValueProperties>,
    pub lipschitz_constants: Value,
}

#[derive(Clone, Debug)]
pub enum Nature {
    Continuous(NatureContinuous),
    Categorical(NatureCategorical),
}

impl Nature {
    pub fn continuous(&self) -> Result<&NatureContinuous> {
        match self {
            Nature::Continuous(continuous) => Ok(continuous),
            _ => Err("nature is categorical when expecting continuous".into())
        }
    }
    pub fn categorical(&self) -> Result<&NatureCategorical> {
        match self {
            Nature::Categorical(categorical) => Ok(categorical),
            _ => Err("nature is continuous when expecting categorical".into())
        }
    }
}

#[derive(Clone, Debug)]
pub struct NatureCategorical {
    pub categories: Jagged
}

#[derive(Clone, Debug)]
pub struct NatureContinuous {
    pub lower: Vector1DNull,
    pub upper: Vector1DNull,
}

#[derive(Clone, Debug)]
pub enum Vector1DNull {
    Bool(Vec<Option<bool>>),
    Int(Vec<Option<Integer>>),
    Float(Vec<Option<Float>>),
    Str(Vec<Option<String>>),
}

impl Vector1DNull {
    /// Retrieve the f64 vec, assuming the data type of the ArrayND is f64
    pub fn float(&self) -> Result<&Vec<Option<Float>>> {
        match self {
            Vector1DNull::Float(x) => Ok(x),
            _ => Err("expected a float on a non-float Vector1DNull".into())
        }
    }
    /// Retrieve the i64 vec, assuming the data type of the ArrayND is i64
    pub fn int(&self) -> Result<&Vec<Option<Integer>>> {
        match self {
            Vector1DNull::Int(x) => Ok(x),
            _ => Err("expected an integer on a non-integer Vector1DNull".into())
        }
    }
}

#[derive(Clone, Debug)]
pub enum Vector1D {
    Bool(Vec<bool>),
    Int(Vec<Integer>),
    Float(Vec<Float>),
    Str(Vec<String>),
}

/// Accepted spaces for sensitivity to be computed within.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SensitivitySpace {
    /// KNorm(1) is L1, KNorm(2) is L2.
    KNorm(u32),
    /// Infinity norm.
    InfNorm,
    Exponential,
}

/// A release consists of Values for each node id.
pub type Release = HashMap<u32, ReleaseNode>;

#[derive(PartialEq, Eq, Clone, Debug, Hash)]
pub struct GroupId {
    pub partition_id: u32,
    pub index: IndexKey
}

#[derive(PartialEq, Eq, Clone, Debug, Hash)]
pub enum IndexKey {
    Str(String),
    Int(Integer),
    Bool(bool),
    Tuple(Vec<IndexKey>)
}

impl ToString for IndexKey {
    fn to_string(&self) -> String {
        match self {
            IndexKey::Str(v) => v.to_string(),
            IndexKey::Int(v) => v.to_string(),
            IndexKey::Bool(v) => v.to_string(),
            IndexKey::Tuple(v) => format!("({:?})", v.iter()
                .map(|v| v.to_string())
                .collect::<Vec<String>>().join(", "))
        }
    }
}

impl IndexKey {
    pub fn new(array: Array) -> Result<IndexKey> {
        match array {
            Array::Int(array) => {
                match array.ndim() {
                    0 => Ok(IndexKey::Int(*array.first().unwrap())),
                    1 => Ok(IndexKey::Tuple(array.into_dimensionality::<ndarray::Ix1>()?
                        .to_vec().into_iter().map(IndexKey::Int).collect())),
                    _ => Err("Indexing keys may not be created from 2+ dimensional arrays.".into())
                }
            }
            Array::Str(array) => {
                match array.ndim() {
                    0 => Ok(IndexKey::Str(array.first().unwrap().to_string())),
                    1 => Ok(IndexKey::Tuple(array.into_dimensionality::<ndarray::Ix1>()?
                        .to_vec().into_iter().map(IndexKey::Str).collect())),
                    _ => Err("Indexing keys may not be created from 2+ dimensional arrays.".into())
                }
            }
            Array::Bool(array) => {
                match array.ndim() {
                    0 => Ok(IndexKey::Bool(*array.first().unwrap())),
                    1 => Ok(IndexKey::Tuple(array.into_dimensionality::<ndarray::Ix1>()?
                        .to_vec().into_iter().map(IndexKey::Bool).collect())),
                    _ => Err("Indexing keys may not be created from 2+ dimensional arrays.".into())
                }
            }
            Array::Float(_) => Err("Floats may not be index keys, because they are not comparable".into())
        }
    }
}

impl From<String> for IndexKey {
    fn from(value: String) -> Self {
        IndexKey::Str(value)
    }
}

impl From<&str> for IndexKey {
    fn from(value: &str) -> Self {
        IndexKey::Str(value.to_string())
    }
}

impl From<bool> for IndexKey {
    fn from(value: bool) -> Self {
        IndexKey::Bool(value)
    }
}

impl From<Integer> for IndexKey {
    fn from(value: Integer) -> Self {
        IndexKey::Int(value)
    }
}

#[derive(Clone, Debug)]
pub struct ReleaseNode {
    pub value: Value,
    pub privacy_usages: Option<Vec<proto::PrivacyUsage>>,
    pub public: bool
}

impl ReleaseNode {
    pub fn new(value: Value) -> ReleaseNode {
        ReleaseNode {
            value,
            privacy_usages: None,
            public: false
        }
    }
}

#[derive(Default, Debug)]
pub struct ComponentExpansion {
    pub computation_graph: HashMap<u32, proto::Component>,
    pub properties: HashMap<u32, ValueProperties>,
    pub releases: HashMap<u32, ReleaseNode>,
    pub traversal: Vec<u32>,
    pub warnings: Vec<Error>
}

impl ComponentExpansion {
    pub fn is_valid(&self, component_id: u32) -> Result<()> {
        let offset = if self.computation_graph.contains_key(&component_id) { 1 } else { 0 };
        let score = (self.computation_graph.len() as i64 - (self.properties.len() + self.traversal.len()) as i64).abs();

        if score > offset {
            println!("WARNING FOR: {:?}", self);
            Err("computation graph patch must be same length as the number of properties".into())
        } else { Ok(()) }
    }
}

impl proto::Component {
    pub fn insert_argument(&mut self, key: &IndexKey, value: u32) {

        let key = serialize_index_key(key.clone());
        match &mut self.arguments {
            Some(arguments) => match arguments.keys.iter()
                .position(|idx| idx == &key) {
                Some(idx) => arguments.values[idx] = value,
                None => {
                    arguments.keys.push(key);
                    arguments.values.push(value)
                }
            },
            None => self.arguments = Some(proto::ArgumentNodeIds {
                keys: vec![key],
                values: vec![value]
            })
        };
    }

    pub fn arguments(&self) -> IndexMap<IndexKey, u32> {
        match &self.arguments {
            Some(arguments) => parse_argument_node_ids(arguments.clone()),
            None => IndexMap::new()
        }
    }
}

impl proto::ArgumentNodeIds {
    pub fn new(arguments: IndexMap<base::IndexKey, u32>) -> Self {
        proto::ArgumentNodeIds {
            keys: arguments.keys().map(|k| serialize_index_key(k.clone())).collect(),
            values: arguments.values().cloned().collect()
        }
    }
}

// The properties for a node consists of Properties for each of its arguments.
pub type NodeProperties = IndexMap<base::IndexKey, ValueProperties>;


impl proto::PrivacyUsage {
    pub(crate) fn actual_to_effective(&self, p: f64, c_stability: f64, group_size: u32) -> Result<Self> {
        Ok(proto::PrivacyUsage {
            distance: Some(match self.distance.as_ref().ok_or_else(|| "distance must be defined")? {
                proto::privacy_usage::Distance::Approximate(app) => proto::privacy_usage::Distance::Approximate(proto::privacy_usage::DistanceApproximate {
                    epsilon: app.epsilon / c_stability / p / group_size as f64,
                    delta: app.delta / c_stability / p / ((group_size as f64 * app.epsilon).exp() - 1.) / (app.epsilon.exp() - 1.),
                })
            })
        })
    }

    pub(crate) fn effective_to_actual(&self, p: f64, c_stability: f64, group_size: u32) -> Result<Self> {
        Ok(proto::PrivacyUsage {
            distance: Some(match self.distance.as_ref().ok_or_else(|| "distance must be defined")? {
                proto::privacy_usage::Distance::Approximate(app) => proto::privacy_usage::Distance::Approximate(proto::privacy_usage::DistanceApproximate {
                    epsilon: app.epsilon * c_stability * p * group_size as f64,
                    delta: app.delta * c_stability * p * ((group_size as f64 * app.epsilon).exp() - 1.) / (app.epsilon.exp() - 1.),
                })
            })
        })
    }
}


impl Add<proto::PrivacyUsage> for proto::PrivacyUsage {
    type Output = Result<proto::PrivacyUsage>;

    fn add(mut self, rhs: proto::PrivacyUsage) -> Self::Output {
        let left_distance = self.distance.ok_or_else(|| "distance must be defined")?;
        let right_distance = rhs.distance.ok_or_else(|| "distance must be defined")?;

        use proto::privacy_usage::Distance;

        self.distance = Some(match (left_distance, right_distance) {
            (Distance::Approximate(lhs), Distance::Approximate(rhs)) => proto::privacy_usage::Distance::Approximate(proto::privacy_usage::DistanceApproximate {
                epsilon: lhs.epsilon + rhs.epsilon,
                delta: lhs.delta + rhs.delta,
            })
        });
        Ok(self)
    }
}


impl Mul<f64> for proto::PrivacyUsage {
    type Output = Result<proto::PrivacyUsage>;

    fn mul(mut self, rhs: f64) -> Self::Output {
        self.distance = Some(match self.distance.ok_or_else(|| "distance must be defined")? {
            proto::privacy_usage::Distance::Approximate(approximate) => proto::privacy_usage::Distance::Approximate(proto::privacy_usage::DistanceApproximate {
                epsilon: approximate.epsilon * rhs,
                delta: approximate.delta * rhs,
            })
        });
        Ok(self)
    }
}

impl Div<f64> for proto::PrivacyUsage {
    type Output = Result<proto::PrivacyUsage>;

    fn div(mut self, rhs: f64) -> Self::Output {
        self.distance = Some(match self.distance.ok_or_else(|| "distance must be defined")? {
            proto::privacy_usage::Distance::Approximate(approximate) => proto::privacy_usage::Distance::Approximate(proto::privacy_usage::DistanceApproximate {
                epsilon: approximate.epsilon / rhs,
                delta: approximate.delta / rhs,
            })
        });
        Ok(self)
    }
}

#[cfg(test)]
pub mod test_data {

    use crate::base::Value;

    pub fn array1d_f64_0() -> Value {
        ndarray::arr1::<f64>(&[]).into()
    }

    pub fn array1d_i64_0() -> Value {
        ndarray::arr1::<i64>(&[]).into()
    }

    pub fn array1d_bool_0() -> Value {
        ndarray::arr1::<bool>(&[]).into()
    }

    pub fn array1d_string_0() -> Value {
        ndarray::arr1::<String>(&[]).into()
    }

    pub fn array1d_f64_10_uniform() -> Value {
        ndarray::arr1(&[
            0.2642, 0.0674, 0.3674, 0.6783, 0.0139, 0.2740, 0.2942, 0.3816, 0.9062, 0.2864
        ]).into()
    }

    pub fn array1d_i64_10_uniform() -> Value {
        ndarray::arr1(&[
            5, 6, 1, 2, 7, 2, 1, 9, 3, 6
        ]).into()
    }

    pub fn array1d_bool_10_uniform() -> Value {
        ndarray::arr1(&[
            false, true, false, false, false, true, true, false, false, true,
        ]).into()
    }

    pub fn array1d_string_10_uniform() -> Value {
        ndarray::arr1(&[
            "b", "a", "b", "b", "a", "b", "b", "a", "a", "a"
        ]).mapv(|v| v.to_string()).into()
    }

    pub fn array2d_f64_0() -> Value {
        ndarray::arr2::<f64, [f64; 0]>(&[]).into()
    }

    pub fn array2d_i64_0() -> Value {
        ndarray::arr2::<i64, [i64; 0]>(&[]).into()
    }

    pub fn array2d_bool_0() -> Value {
        ndarray::arr2::<bool, [bool; 0]>(&[]).into()
    }

    pub fn array2d_string_0() -> Value {
        ndarray::arr2::<String, [String; 0]>(&[]).into()
    }

    pub fn array2d_f64_10() -> Value {
        ndarray::arr2(&[
            [0., 0., 02., 0.1789],
            [1., 0., 03., 0.9004],
            [2., 1., 05., 0.8419],
            [3., 1., 07., 0.0845],
            [4., 2., 11., 0.6996],
            [5., 2., 13., 0.9594],
            [6., 3., 17., 0.2823],
            [7., 3., 19., 0.0514],
            [8., 4., 23., 0.3068],
            [9., 4., 29., 0.3553],
        ]).into()
    }

    pub fn array2d_bool_8() -> Value {
        ndarray::arr2(&[
            [false, false, false],
            [false, false, true],
            [false, true, false],
            [false, true, true],
            [true, false, false],
            [true, false, true],
            [true, true, false],
            [true, true, true],
        ]).into()
    }
}