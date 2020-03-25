//! Core data structures

use crate::errors::*;


use crate::proto;





use ndarray::prelude::Ix1;

use std::collections::{HashMap};





use ndarray::{ArrayD};

use crate::utilities::standardize_categorical_argument;

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
    ArrayND(ArrayND),
    /// A hash-map, where the keys are enum-typed and the values are of type Value
    Hashmap(Hashmap<Value>),
    /// A 2D homogeneously typed matrix, where the columns may be unknown and the column lengths may be inconsistent
    Vector2DJagged(Vector2DJagged),
}

impl Value {
    /// Retrieve an ArrayND from a Value, assuming the Value contains an ArrayND
    pub fn get_arraynd<'a>(&'a self) -> Result<&'a ArrayND> {
        match self {
            Value::ArrayND(array) => Ok(array),
            _ => Err("value must be wrapped in an ArrayND".into())
        }
    }
    /// Retrieve a Vector2DJagged from a Value, assuming the Value contains a Vector2DJagged
    pub fn get_jagged<'a>(&'a self) -> Result<&'a Vector2DJagged> {
        match self {
            Value::Vector2DJagged(jagged) => Ok(jagged),
            _ => Err("value must be wrapped in a Vector2DJagged".into())
        }
    }

    /// Retrieve the first f64 from a Value, assuming a Value contains an ArrayND of type f64
    pub fn get_first_f64(&self) -> Result<f64> {
        match self {
            Value::ArrayND(array) => array.get_first_f64(),
            _ => Err("cannot retrieve first float".into())
        }
    }
    /// Retrieve the first i64 from a Value, assuming a Value contains an ArrayND of type i64
    pub fn get_first_i64(&self) -> Result<i64> {
        match self {
            Value::ArrayND(array) => array.get_first_i64(),
            _ => Err("cannot retrieve integer".into())
        }
    }
    /// Retrieve the first String from a Value, assuming a Value contains an ArrayND of type String
    pub fn get_first_str(&self) -> Result<String> {
        match self {
            Value::ArrayND(array) => array.get_first_str(),
            _ => Err("cannot retrieve string".into())
        }
    }
    /// Retrieve the first bool from a Value, assuming a Value contains an ArrayND of type bool
    pub fn get_first_bool(&self) -> Result<bool> {
        match self {
            Value::ArrayND(array) => array.get_first_bool(),
            _ => Err("cannot retrieve bool".into())
        }
    }
}


// build Value from other types with .into()
impl From<ArrayD<bool>> for Value {
    fn from(value: ArrayD<bool>) -> Self {
        Value::ArrayND(ArrayND::Bool(value))
    }
}

impl From<ArrayD<f64>> for Value {
    fn from(value: ArrayD<f64>) -> Self {
        Value::ArrayND(ArrayND::F64(value))
    }
}

impl From<ArrayD<i64>> for Value {
    fn from(value: ArrayD<i64>) -> Self {
        Value::ArrayND(ArrayND::I64(value))
    }
}

impl From<ArrayD<String>> for Value {
    fn from(value: ArrayD<String>) -> Self {
        Value::ArrayND(ArrayND::Str(value))
    }
}

impl From<HashMap<bool, Value>> for Value {
    fn from(value: HashMap<bool, Value>) -> Self {
        Value::Hashmap(Hashmap::<Value>::Bool(value))
    }
}

impl From<HashMap<i64, Value>> for Value {
    fn from(value: HashMap<i64, Value>) -> Self {
        Value::Hashmap(Hashmap::<Value>::I64(value))
    }
}

impl From<HashMap<String, Value>> for Value {
    fn from(value: HashMap<String, Value>) -> Self {
        Value::Hashmap(Hashmap::<Value>::Str(value))
    }
}

impl From<HashMap<bool, ValueProperties>> for Hashmap<ValueProperties> {
    fn from(value: HashMap<bool, ValueProperties>) -> Self {
        Hashmap::<ValueProperties>::Bool(value)
    }
}

impl From<HashMap<i64, ValueProperties>> for Hashmap<ValueProperties> {
    fn from(value: HashMap<i64, ValueProperties>) -> Self {
        Hashmap::<ValueProperties>::I64(value)
    }
}

impl From<HashMap<String, ValueProperties>> for Hashmap<ValueProperties> {
    fn from(value: HashMap<String, ValueProperties>) -> Self {
        Hashmap::<ValueProperties>::Str(value)
    }
}

impl From<ArrayNDProperties> for ValueProperties {
    fn from(value: ArrayNDProperties) -> Self {
        ValueProperties::ArrayND(value)
    }
}

impl From<HashmapProperties> for ValueProperties {
    fn from(value: HashmapProperties) -> Self {
        ValueProperties::Hashmap(value)
    }
}

impl From<Vector2DJaggedProperties> for ValueProperties {
    fn from(value: Vector2DJaggedProperties) -> Self {
        ValueProperties::Vector2DJagged(value)
    }
}

impl From<ndarray::ShapeError> for Error {
    fn from(_: ndarray::ShapeError) -> Self {
        "ndarray: invalid shape provided".into()
    }
}


/// The universal n-dimensional array representation.
///
/// ndarray ArrayD's are artificially allowed to be 0, 1 or 2-dimensional.
/// The first axis denotes the number rows/observations. The second axis the number of columns.
///
/// The ArrayND has a one-to-one mapping to a protobuf ArrayND.
#[derive(Clone, Debug)]
pub enum ArrayND {
    Bool(ArrayD<bool>),
    I64(ArrayD<i64>),
    F64(ArrayD<f64>),
    Str(ArrayD<String>),
}

impl ArrayND {
    /// Retrieve the f64 ndarray, assuming the data type of the ArrayND is f64
    pub fn get_f64(&self) -> Result<&ArrayD<f64>> {
        match self {
            ArrayND::F64(x) => Ok(x),
            _ => Err("expected a float on a non-float ArrayND".into())
        }
    }
    pub fn get_first_f64(&self) -> Result<f64> {
        match self {
            ArrayND::Bool(x) => {
                if x.len() != 1 {
                    return Err("non-singleton array passed for an argument that must be scalar".into());
                }
                Ok(if *x.first().unwrap() { 1. } else { 0. })
            }
            ArrayND::I64(x) => {
                if x.len() != 1 {
                    return Err("non-singleton array passed for an argument that must be scalar".into());
                }
                Ok(f64::from(*x.first().unwrap() as i32))
            }
            ArrayND::F64(x) => {
                if x.len() != 1 {
                    return Err("non-singleton array passed for an argument that must be scalar".into());
                }
                Ok(x.first().unwrap().to_owned())
            }
            _ => Err("value must be numeric".into())
        }
    }
    pub fn get_vec_f64(&self, optional_length: Option<i64>) -> Result<Vec<f64>> {
        let data = self.get_f64()?;
        let err_msg = "failed attempt to cast f64 ArrayD to vector".into();
        match data.ndim().clone() {
            0 => match (optional_length, data.first()) {
                (Some(length), Some(v)) => Ok((0..length).map(|_| v.clone()).collect()),
                _ => Err(err_msg)
            },
            1 => Ok(data.clone().into_dimensionality::<Ix1>().unwrap().to_vec()),
            _ => Err(err_msg)
        }
    }
    /// Retrieve the i64 ndarray, assuming the data type of the ArrayND is i64
    pub fn get_i64(&self) -> Result<&ArrayD<i64>> {
        match self {
            ArrayND::I64(x) => Ok(x),
            _ => Err("expected a float on a non-float ArrayND".into())
        }
    }
    pub fn get_first_i64(&self) -> Result<i64> {
        match self {
            ArrayND::Bool(x) => {
                if x.len() != 1 {
                    return Err("non-singleton array passed for an argument that must be scalar".into());
                }
                Ok(if *x.first().unwrap() { 1 } else { 0 })
            }
            ArrayND::I64(x) => {
                if x.len() != 1 {
                    return Err("non-singleton array passed for an argument that must be scalar".into());
                }
                Ok(x.first().unwrap().to_owned())
            }
            _ => Err("value must be numeric".into())
        }
    }
    pub fn get_vec_i64(&self, optional_length: Option<i64>) -> Result<Vec<i64>> {
        let data = self.get_i64()?;
        let err_msg = "failed attempt to cast i64 ArrayD to vector".into();
        match data.ndim().clone() {
            0 => match (optional_length, data.first()) {
                (Some(length), Some(v)) => Ok((0..length).map(|_| v.clone()).collect()),
                _ => Err(err_msg)
            },
            1 => Ok(data.clone().into_dimensionality::<Ix1>().unwrap().to_vec()),
            _ => Err(err_msg)
        }
    }
    /// Retrieve the String ndarray, assuming the data type of the ArrayND is String
    pub fn get_str(&self) -> Result<&ArrayD<String>> {
        match self {
            ArrayND::Str(x) => Ok(x),
            _ => Err("value must be a string".into())
        }
    }
    pub fn get_first_str(&self) -> Result<String> {
        match self {
            ArrayND::Str(x) => {
                if x.len() != 1 {
                    return Err("non-singleton array passed for an argument that must be scalar".into());
                }
                Ok(x.first().unwrap().to_owned())
            }
            _ => Err("value must be a string".into())
        }
    }
    /// Retrieve the bool ndarray, assuming the data type of the ArrayND is bool
    pub fn get_bool(&self) -> Result<&ArrayD<bool>> {
        match self {
            ArrayND::Bool(x) => Ok(x),
            _ => Err("value must be a bool".into())
        }
    }
    pub fn get_first_bool(&self) -> Result<bool> {
        match self {
            ArrayND::Bool(x) => {
                if x.len() != 1 {
                    return Err("non-singleton array passed for an argument that must be scalar".into());
                }
                Ok(x.first().unwrap().to_owned())
            }
            _ => Err("value must be a bool".into())
        }
    }
}

/// The universal jagged array representation.
///
/// Typically used to store categorically clamped values.
/// In practice, use is limited to public categories over multiple columns, and the upper triangular covariance matrix
///
/// The Vector2DJagged has a one-to-one mapping to a protobuf Vector2DJagged.
#[derive(Clone, Debug)]
pub enum Vector2DJagged {
    Bool(Vec<Option<Vec<bool>>>),
    I64(Vec<Option<Vec<i64>>>),
    F64(Vec<Option<Vec<f64>>>),
    Str(Vec<Option<Vec<String>>>),
}

impl Vector2DJagged {
    /// Retrieve the f64 jagged matrix, assuming the data type of the jagged matrix is f64, and assuming all columns are defined
    pub fn get_f64(&self) -> Result<Vec<Vec<f64>>> {
        self.get_f64_option()?.iter().cloned().collect::<Option<Vec<Vec<f64>>>>()
            .ok_or::<Error>("not all columns are known in float Vector2DJagged".into())
    }
    /// Retrieve the f64 jagged matrix, assuming the data type of the jagged matrix is f64
    pub fn get_f64_option<'a>(&'a self) -> Result<&'a Vec<Option<Vec<f64>>>> {
        match self {
            Vector2DJagged::F64(data) => Ok(data),
            _ => Err("expected float type on a non-float Vector2DJagged".into())
        }
    }
    /// Retrieve the i64 jagged matrix, assuming the data type of the jagged matrix is i64
    pub fn get_i64(&self) -> Result<Vec<Vec<i64>>> {
        match self {
            Vector2DJagged::I64(data) => data.iter().cloned().collect::<Option<Vec<Vec<i64>>>>()
                .ok_or::<Error>("not all columns are known in int Vector2DJagged".into()),
            _ => Err("expected int type on a non-int Vector2DJagged".into())
        }
    }
    /// Retrieve the String jagged matrix, assuming the data type of the jagged matrix is String
    pub fn get_str(&self) -> Result<Vec<Vec<String>>> {
        match self {
            Vector2DJagged::Str(data) => data.iter().cloned().collect::<Option<Vec<Vec<String>>>>()
                .ok_or::<Error>("not all columns are known in string Vector2DJagged".into()),
            _ => Err("expected string type on a non-string Vector2DJagged".into())
        }
    }
    /// Retrieve the bool jagged matrix, assuming the data type of the jagged matrix is bool
    pub fn get_bool(&self) -> Result<Vec<Vec<bool>>> {
        match self {
            Vector2DJagged::Bool(data) => data.iter().cloned().collect::<Option<Vec<Vec<bool>>>>()
                .ok_or::<Error>("not all columns are known in bool Vector2DJagged".into()),
            _ => Err("expected bool type on a non-bool Vector2DJagged".into())
        }
    }
}

/// The universal hash-map representation.
///
/// Used for any component that has multiple outputs.
/// In practice, the only components that can emit multiple outputs are materialize (by columns) and partition (by rows)
///
/// The Hashmap has a one-to-one mapping to a protobuf Hashmap.
#[derive(Clone, Debug)]
pub enum Hashmap<T> {
    Bool(HashMap<bool, T>),
    I64(HashMap<i64, T>),
    Str(HashMap<String, T>),
}

/// Derived properties for the universal value.
///
/// The ValueProperties has a one-to-one mapping to a protobuf ValueProperties.
#[derive(Clone, Debug)]
pub enum ValueProperties {
    Hashmap(HashmapProperties),
    ArrayND(ArrayNDProperties),
    Vector2DJagged(Vector2DJaggedProperties),
}


impl ValueProperties {
    /// Retrieve properties corresponding to an ArrayND, assuming the corresponding data value is actually the ArrayND variant
    pub fn get_arraynd(&self) -> Result<&ArrayNDProperties> {
        match self {
            ValueProperties::ArrayND(array) => Ok(array),
            _ => Err("value must be an array".into())
        }
    }
    /// Retrieve properties corresponding to an Hashmap, assuming the corresponding data value is actually the Hashmap variant
    pub fn get_hashmap(&self) -> Result<&HashmapProperties> {
        match self {
            ValueProperties::Hashmap(value) => Ok(value),
            _ => Err("value must be a hashmap".into())
        }
    }
    /// Retrieve properties corresponding to an Vector2DJagged, assuming the corresponding data value is actually the Vector2DJagged variant
    pub fn get_jagged(&self) -> Result<&Vector2DJaggedProperties> {
        match self {
            ValueProperties::Vector2DJagged(value) => Ok(value),
            _ => Err("value must be a ragged matrix".into())
        }
    }
}


/// Derived properties for the universal Hashmap.
///
/// The HashmapProperties has a one-to-one mapping to a protobuf HashmapProperties.
#[derive(Clone, Debug)]
pub struct HashmapProperties {
    /// global count over all partitions
    pub num_records: Option<i64>,
    /// records within the values of the hashmap come from a partition of the rows
    pub disjoint: bool,
    /// properties for each of the values in the hashmap
    pub value_properties: Hashmap<ValueProperties>,
}


/// Derived properties for the universal ArrayND.
///
/// The ArrayNDProperties has a one-to-one mapping to a protobuf ArrayNDProperties.
#[derive(Clone, Debug)]
pub struct ArrayNDProperties {
    /// Defined if the number of records is known statically (set by the resize component)
    pub num_records: Option<i64>,
    pub num_columns: Option<i64>,
    /// true if the data may contain null values
    pub nullity: bool,
    /// set to true by the mechanisms. Acts as a filter on the values in the release
    pub releasable: bool,
    /// amplification of privacy usage by unstable data transformations, or possibility of duplicated records
    pub c_stability: Vec<f64>,
    /// set when data is aggregated, used to help compute sensitivity from the mechanisms
    pub aggregator: Option<AggregatorProperties>,
    /// either min/max or categories
    pub nature: Option<Nature>,
    /// f64, i64, bool, String
    pub data_type: DataType,
    /// index of last Materialize or Filter node, where dataset was created
    /// used to determine if arrays are conformable even when N is not known
    pub dataset_id: Option<i64>,
}


/// Derived properties for the universal Vector2DJagged.
///
/// The Vector2DJagged has a one-to-one mapping to a protobuf Vector2DJagged.
#[derive(Clone, Debug)]
pub struct Vector2DJaggedProperties {}

impl ArrayNDProperties {
    pub fn get_min_f64_option(&self) -> Result<Vec<Option<f64>>> {
        match self.nature.to_owned() {
            Some(value) => match value {
                Nature::Continuous(continuous) => match continuous.min {
                    Vector1DNull::F64(bound) => Ok(bound),
                    _ => Err("min must be composed of floats".into())
                },
                _ => Err("min must be an array".into())
            },
            None => Err("continuous nature for min is not defined".into())
        }
    }
    pub fn get_min_f64(&self) -> Result<Vec<f64>> {
        let bound = self.get_min_f64_option()?;
        let value = bound.iter().filter_map(|v| v.to_owned()).collect::<Vec<f64>>();
        match bound.len() == value.len() {
            true => Ok(value),
            false => Err("not all min are known".into())
        }
    }
    pub fn get_max_f64_option(&self) -> Result<Vec<Option<f64>>> {
        match self.nature.to_owned() {
            Some(value) => match value {
                Nature::Continuous(continuous) => match continuous.max {
                    Vector1DNull::F64(bound) => Ok(bound),
                    _ => Err("max must be composed of floats".into())
                },
                _ => Err("max must be an array".into())
            },
            None => Err("continuous nature for max is not defined".into())
        }
    }
    pub fn get_max_f64(&self) -> Result<Vec<f64>> {
        let bound = self.get_max_f64_option()?;
        let value = bound.iter().filter_map(|v| v.to_owned()).collect::<Vec<f64>>();
        match bound.len() == value.len() {
            true => Ok(value),
            false => Err("not all max are known".into())
        }
    }

    pub fn get_categories(&self) -> Result<Vector2DJagged> {
        match self.nature.to_owned() {
            Some(nature) => match nature {
                Nature::Categorical(nature) => Ok(nature.categories),
                _ => Err("categories is not defined".into())
            },
            None => Err("categorical nature is not defined".into())
        }
    }
    pub fn get_categories_lengths(&self) -> Result<Vec<Option<i64>>> {
        let num_columns = self.get_num_columns()?;

        match self.get_categories() {
            Ok(categories) => Ok(match categories {
                Vector2DJagged::Str(categories) =>
                    standardize_categorical_argument(&categories, &num_columns)?.iter()
                        .map(|cats| Some(cats.len() as i64)).collect(),
                Vector2DJagged::Bool(categories) =>
                    standardize_categorical_argument(&categories, &num_columns)?.iter()
                        .map(|cats| Some(cats.len() as i64)).collect(),
                Vector2DJagged::I64(categories) =>
                    standardize_categorical_argument(&categories, &num_columns)?.iter()
                        .map(|cats| Some(cats.len() as i64)).collect(),
                Vector2DJagged::F64(categories) =>
                    standardize_categorical_argument(&categories, &num_columns)?.iter()
                        .map(|cats| Some(cats.len() as i64)).collect(),
            }),
            Err(_) => Ok((0..num_columns).map(|_| Some(1)).collect())
        }
    }
    pub fn assert_categorical(&self) -> Result<()> {
        self.get_categories_lengths()?
            .iter().cloned().collect::<Option<Vec<i64>>>()
            .ok_or::<Error>("categories on all columns must be defined".into())?;

        Ok(())
    }
    pub fn assert_non_null(&self) -> Result<()> {
        match self.nullity {
            false => Ok(()),
            true => Err("data may contain nullity when non-nullity is required".into())
        }
    }
    pub fn assert_is_releasable(&self) -> Result<()> {
        match self.releasable {
            false => Ok(()),
            true => Err("data is not releasable when releasability is required".into())
        }
    }
    pub fn get_num_columns(&self) -> Result<i64> {
        self.num_columns.ok_or::<Error>("number of columns is not defined".into())
    }
    pub fn get_num_records(&self) -> Result<i64> {
        self.num_records.ok_or::<Error>("number of rows is not defined".into())
    }
    pub fn assert_is_not_aggregated(&self) -> Result<()> {
        match self.aggregator.to_owned() {
            Some(_aggregator) => Err("aggregated data may not be manipulated".into()),
            None => Ok(())
        }
    }
}

/// Fundamental data types for ArrayNDs and Vector2DJagged Values.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DataType {
    Bool,
    Str,
    F64,
    I64,
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
    pub properties: HashMap<String, ValueProperties>,
}

#[derive(Clone, Debug)]
pub enum Nature {
    Continuous(NatureContinuous),
    Categorical(NatureCategorical),
}

#[derive(Clone, Debug)]
pub struct NatureCategorical {
    pub categories: Vector2DJagged
}

#[derive(Clone, Debug)]
pub struct NatureContinuous {
    pub min: Vector1DNull,
    pub max: Vector1DNull,
}

#[derive(Clone, Debug)]
pub enum Vector1DNull {
    Bool(Vec<Option<bool>>),
    I64(Vec<Option<i64>>),
    F64(Vec<Option<f64>>),
    Str(Vec<Option<String>>),
}

impl Vector1DNull {
    /// Retrieve the f64 vec, assuming the data type of the ArrayND is f64
    pub fn get_f64(&self) -> Result<&Vec<Option<f64>>> {
        match self {
            Vector1DNull::F64(x) => Ok(x),
            _ => Err("expected a float on a non-float Vector1DNull".into())
        }
    }
    /// Retrieve the i64 vec, assuming the data type of the ArrayND is i64
    pub fn get_i64(&self) -> Result<&Vec<Option<i64>>> {
        match self {
            Vector1DNull::I64(x) => Ok(x),
            _ => Err("expected an integer on a non-integer Vector1DNull".into())
        }
    }
}

#[derive(Clone, Debug)]
pub enum Vector1D {
    Bool(Vec<bool>),
    I64(Vec<i64>),
    F64(Vec<f64>),
    Str(Vec<String>),
}

/// Accepted spaces for sensitivity to be computed within.
pub enum Sensitivity {
    /// KNorm(1) is L1, KNorm(2) is L2.
    KNorm(u32),
    /// Infinity norm.
    InfNorm,
    Exponential,
}
/// A release consists of Values for each node id.
pub type Release = HashMap<u32, Value>;

// The properties for a node consists of Properties for each of its arguments.
pub type NodeProperties = HashMap<String, ValueProperties>;
