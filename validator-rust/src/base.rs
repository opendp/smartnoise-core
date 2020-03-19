//! Core logic and data structures

use crate::errors::*;


use crate::proto;
use itertools::Itertools;

use crate::utilities;

use crate::components::*;
use ndarray::prelude::Ix1;

use std::collections::{HashMap, HashSet};

use crate::utilities::serial::{serialize_value, parse_release};
use crate::utilities::json::{JSONRelease};

use std::ops::Deref;
use ndarray::{ArrayD, Array};
use crate::utilities::inference::infer_property;

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
            ArrayND::Bool(x) => Ok(if *x.first().unwrap() { 1. } else { 0. }),
            ArrayND::I64(x) => Ok(f64::from(*x.first().unwrap() as i32)),
            ArrayND::F64(x) => Ok(x.first().unwrap().to_owned()),
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
            ArrayND::Bool(x) => Ok(if *x.first().unwrap() { 1 } else { 0 }),
            ArrayND::I64(x) => Ok(x.first().unwrap().to_owned()),
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
            ArrayND::Str(x) => Ok(x.first().unwrap().to_owned()),
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
            ArrayND::Bool(x) => Ok(x.first().unwrap().to_owned()),
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
    Vector2DJagged(Vector2DJaggedProperties)
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
    pub data_type: DataType
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
            Some(aggregator) => Err("aggregated data may not be manipulated".into()),
            None => Ok(())
        }
    }
}

/// Fundamental data types for ArrayNDs and Vector2DJagged Values.
#[derive(Clone, Debug)]
pub enum DataType {
    Bool, Str, F64, I64
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
    pub properties: HashMap<String, ValueProperties>
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
    Exponential
}

#[doc(hidden)]
pub fn prepend(text: &str) -> impl Fn(Error) -> Error + '_ {
    move |e| format!("{} {}", text, e).into()
}

/// A release consists of Values for each node id.
pub type Release = HashMap<u32, Value>;

// The properties for a node consists of Properties for each of its arguments.
pub type NodeProperties = HashMap<String, ValueProperties>;

/// Retrieve the Values for each of the arguments of a component from the Release.
pub fn get_input_arguments(
    component: &proto::Component,
    graph_evaluation: &Release
) -> Result<HashMap<String, Value>> {
    let mut arguments = HashMap::<String, Value>::new();
    for (field_id, field) in component.arguments.clone() {
        if let Some(evaluation) = graph_evaluation.get(&field) {
            arguments.insert(field_id.to_owned(), evaluation.to_owned());
        }
    }
    Ok(arguments)
}

/// Retrieve the specified Value from the arguments to a component.
pub fn get_argument<'a>(
    arguments: &HashMap<String, &'a Value>,
    name: &str
) -> Result<&'a Value> {
    match arguments.get(name) {
        Some(argument) => Ok(argument),
        _ => Err((name.to_string() + " is not defined").into())
    }
}

/// Retrieve the ValueProperties for each of the arguments of a component from the Release.
pub fn get_input_properties<T>(
    component: &proto::Component,
    graph_properties: &HashMap<u32, T>,
) -> Result<HashMap<String, T>> where T: std::clone::Clone {
    let mut properties = HashMap::<String, T>::new();
    for (field_id, field) in component.arguments.clone() {
        if let Some(property) = graph_properties.get(&field).clone() {
            properties.insert(field_id.to_owned(), property.clone());
        }
    }
    Ok(properties)
}

/// Given an analysis and release, attempt to propagate properties across the entire computation graph.
///
/// The graph is traversed, and every node is attempted to be expanded, so that validation occurs at the most granular level.
/// Each component in the graph implements the Component trait, which contains the propagate_properties function.
/// While traversing, properties are checked and propagated forward at every point in the graph.
/// If the requirements for any node are not met, the propagation fails, and the analysis is not valid.
///
/// # Returns
/// * `0` - Properties for every node in the expanded graph
/// * `1` - The expanded graph
pub fn propagate_properties(
    analysis: &proto::Analysis,
    release: &proto::Release,
) -> Result<(HashMap<u32, ValueProperties>, HashMap<u32, proto::Component>)> {
    // compute properties for every node in the graph

    let privacy_definition = analysis.privacy_definition.to_owned().unwrap();
    let mut graph: HashMap<u32, proto::Component> = analysis.computation_graph.to_owned().unwrap().value.to_owned();
    let mut traversal: Vec<u32> = get_traversal(&graph)?;
    traversal.reverse();

    let graph_evaluation: Release = parse_release(&release)?;
//    println!("GRAPH EVALUATION: {:?}", graph_evaluation);
    let mut graph_properties = HashMap::<u32, ValueProperties>::new();

    let mut maximum_id = graph.keys().cloned()
        .fold(0, std::cmp::max);

    while !traversal.is_empty() {
        let node_id = traversal.last().unwrap().clone();

        let component: proto::Component = graph.get(&node_id).unwrap().to_owned();
//        println!("Propagating properties at node_id {:?} {:?}", node_id, component);

        let properties = match graph_evaluation.get(&node_id) {
            // if node has already been evaluated, infer properties directly from the public data
            Some(value) => {
                traversal.pop();
                infer_property(&value)?
            },

            // if node has not been evaluated, propagate properties over it
            None => {
                let component: proto::Component = graph.get(&node_id).unwrap().to_owned();
                let input_properties = get_input_properties(&component, &graph_properties)?;
                let public_arguments = get_input_arguments(&component, &graph_evaluation)?;

                let result = component.clone().variant.unwrap().expand_component(
                    &privacy_definition,
                    &component,
                    &input_properties,
                    node_id.clone(),
                    maximum_id.clone(),
                )?;

                // patch the computation graph
                graph.extend(result.1);

//                println!("maximum id {:?}", maximum_id);
                // if patch added nodes, extend the traversal
                if result.0 > maximum_id {
                    let mut new_nodes = ((maximum_id + 1)..(result.0 + 1)).collect::<Vec<u32>>();
                    new_nodes.reverse();
                    traversal.extend(new_nodes);
                    maximum_id = result.0;
                    continue;
                }
                traversal.pop();

                component.clone().variant.unwrap().propagate_property(
                    &privacy_definition, &public_arguments, &input_properties)
                    .chain_err(|| format!("at node_id {:?},", node_id))?
            }
        };
        graph_properties.insert(node_id.clone(), properties);
    }
    Ok((graph_properties, graph))
}

/// Given a computation graph, return an ordering of nodes that ensures all dependencies of any node have been visited
///
/// The traversal also fails upon detecting cyclic dependencies,
/// and attempts to optimize traversal order to minimize caching of intermediate results.
pub fn get_traversal(
    graph: &HashMap<u32, proto::Component>
) -> Result<Vec<u32>> {

    // track node parents
    let mut parents = HashMap::<u32, HashSet<u32>>::new();
    graph.iter().for_each(|(node_id, component)| {
        if !parents.contains_key(node_id) {
            parents.insert(*node_id, HashSet::<u32>::new());
        }
        component.arguments.values().for_each(|argument_node_id| {
            parents.entry(*argument_node_id)
                .or_insert_with(HashSet::<u32>::new)
                .insert(*node_id);
        })
    });

    // store the optimal computation order of node ids
    let mut traversal = Vec::new();

    // collect all sources (nodes with zero arguments)
    let mut queue: Vec<u32> = graph.iter()
        .filter(|(_node_id, component)| component.arguments.is_empty())
        .map(|(node_id, _component)| node_id.to_owned()).collect();

    let mut visited = HashMap::new();

    while !queue.is_empty() {
        let queue_node_id: u32 = *queue.last().unwrap();
        queue.pop();
        traversal.push(queue_node_id);

        let mut is_cyclic = false;

        parents.get(&queue_node_id).unwrap().iter().for_each(|parent_node_id| {
            let parent_arguments = graph.get(parent_node_id).unwrap().to_owned().arguments;

            // if parent has been reached more times than it has arguments, then it is cyclic
            let count = visited.entry(*parent_node_id).or_insert(0);
            *count += 1;
            if visited.get(parent_node_id).unwrap() > &parent_arguments.len() {
                is_cyclic = true;
            }

            // check that all arguments of parent_node have been evaluated before adding to queue
            if parent_arguments.values().all(|argument_node_id| traversal.contains(argument_node_id)) {
                queue.push(*parent_node_id);
            }
        });

        if is_cyclic {
            return Err("Graph is cyclic.".into())
        }

    }
    return Ok(traversal);
}

/// Given an array, conduct well-formedness checks and broadcast
///
/// Typically used by functions when standardizing numeric arguments, but generally applicable.
#[doc(hidden)]
pub fn standardize_numeric_argument<T: Clone>(value: &ArrayD<T>, length: &i64) -> Result<ArrayD<T>> {
    match value.ndim() {
        0 => match value.first() {
            Some(scalar) => Ok(Array::from((0..*length).map(|_| scalar.clone()).collect::<Vec<T>>()).into_dyn()),
            None => Err("value must be non-empty".into())
        },
        1 => match value.len() as i64 == *length {
            true => Ok(value.clone()),
            false => Err("value is of incompatible length".into())
        },
        _ => Err("value must be a scalar or vector".into())
    }
}

#[doc(hidden)]
pub fn uniform_density(length: usize) -> Vec<f64> {
    (0..length).map(|_| 1. / (length as f64)).collect()
}


/// Convert weights to probabilities
///
/// TODO: add more checks
#[doc(hidden)]
pub fn normalize_probabilities(weights: &Vec<f64>) -> Vec<f64> {
    let sum: f64 = weights.iter().sum();
    weights.iter().map(|prob| prob / sum).collect()
}

/// Given a jagged categories array, conduct well-formedness checks and broadcast
#[doc(hidden)]
pub fn standardize_categorical_argument<T: Clone>(
    categories: &Vec<Option<Vec<T>>>,
    length: &i64
) -> Result<Vec<Vec<T>>> {
    // check that no categories are explicitly None
    let mut categories = categories.iter()
        .map(|v| v.clone())
        .collect::<Option<Vec<Vec<T>>>>()
        .ok_or::<Error>("categories must be defined for all columns".into())?;

    if categories.len() == 0 {
        return Err("no categories are defined".into());
    }
    // broadcast categories across all columns, if only one categories set is defined
    if categories.len() == 1 {
        categories = (0..*length).map(|_| categories.first().unwrap().clone()).collect();
    }

    Ok(categories)
}


/// Given a jagged null values array, conduct well-formedness checks, broadcast along columns, and flatten along rows.
#[doc(hidden)]
pub fn standardize_null_argument<T: Clone>(
    value: &Vec<Option<Vec<T>>>,
    length: &i64
) -> Result<Vec<T>> {
    let value = value.iter()
        .map(|v| v.clone())
        .collect::<Option<Vec<Vec<T>>>>()
        .ok_or::<Error>("null must be defined for all columns".into())?;

    if value.len() == 0 {
        return Err("null values cannot be an empty vector".into());
    }

    let mut value: Vec<T> = value.iter().map(|v| match v.len() {
        1 => Ok(v.clone().first().unwrap().clone()),
        _ => Err("only one null value may be defined".into())
    }).collect::<Result<Vec<T>>>()?;

    // broadcast nulls across all columns, if only one null set is defined
    if value.len() == 1 {
        value = (0..*length).map(|_| value.clone().first().unwrap().clone()).collect();
    }
    Ok(value)
}

/// Given categories and a jagged categories weights array, conduct well-formedness checks and return a standardized set of probabilities.
#[doc(hidden)]
pub fn standardize_weight_argument<T>(
    categories: &Vec<Vec<T>>,
    weights: &Vec<Option<Vec<f64>>>
) -> Result<Vec<Vec<f64>>> {
    match weights.len() {
        0 => Ok(categories.iter()
            .map(|cats| uniform_density(cats.len()))
            .collect::<Vec<Vec<f64>>>()),
        1 => {
            let weights = match weights[0].clone() {
                Some(weights) => normalize_probabilities(&weights),
                None => uniform_density(categories[0].len())
            };

            categories.iter().map(|cats| match cats.len() == weights.len() {
                true => Ok(weights.clone()),
                false => Err("length of weights does not match number of categories".into())
            }).collect::<Result<Vec<Vec<f64>>>>()
        },
        _ => match categories.len() == weights.len() {
            true => categories.iter().zip(weights.iter()).map(|(_cats, weights)| match weights {
                Some(weights) => Ok(normalize_probabilities(weights)),
                None => Err("category weights must be set once, for all categories, or none".into())
            }).collect::<Result<Vec<Vec<f64>>>>(),
            false => return Err("category weights must be the same length as categories, or none".into())
        }
    }
}

/// Utility for building extra Components to pass back when conducting expansions.
#[doc(hidden)]
pub fn get_constant(value: &Value, batch: &u32) -> proto::Component {
    proto::Component {
        arguments: HashMap::new(),
        variant: Some(proto::component::Variant::Constant(proto::Constant {
            value: serialize_value(&value).ok()
        })),
        omit: true,
        batch: batch.clone()
    }
}

#[doc(hidden)]
pub fn validate_analysis(
    analysis: &proto::Analysis,
    release: &proto::Release
) -> Result<proto::response_validate_analysis::Validated> {
    let _graph = analysis.computation_graph.to_owned()
        .ok_or("the computation graph must be defined in an analysis")?
        .value;

    propagate_properties(&analysis, &release)?;

    return Ok(proto::response_validate_analysis::Validated {
        value: true,
        message: "The analysis is valid.".to_string()
    });
}

pub fn compute_privacy_usage(
    analysis: &proto::Analysis, release: &proto::Release,
) -> Result<proto::PrivacyUsage> {

    let (graph_properties, graph) = propagate_properties(&analysis, &release)?;

    println!("graph: {:?}", graph);
    let usage_option = graph.iter()
        // return the privacy usage from the release, else from the analysis
        .filter_map(|(node_id, component)| get_component_privacy_usage(component, release.values.get(node_id)))
        // linear sum
        .fold1(|usage_1, usage_2| privacy_usage_reducer(
            &usage_1, &usage_2, &|l, r| l + r));

    // TODO: this should probably return a proto::PrivacyUsage with zero based on the privacy definition
    match usage_option {
        Some(x) => Ok(x),
        None => Err("no information is released; privacy usage is none".into())
    }
}

pub fn get_component_privacy_usage(
    component: &proto::Component,
    release_node: Option<&proto::ReleaseNode>,
) -> Option<proto::PrivacyUsage> {

    // get the maximum possible usage allowed to the component
    let mut privacy_usage: Vec<proto::PrivacyUsage> = match component.to_owned().variant? {
        proto::component::Variant::LaplaceMechanism(x) => x.privacy_usage,
        proto::component::Variant::GaussianMechanism(x) => x.privacy_usage,
        proto::component::Variant::ExponentialMechanism(x) => x.privacy_usage,
        proto::component::Variant::SimpleGeometricMechanism(x) => x.privacy_usage,
        _ => return None
    };

    // if release usage is defined, then use the actual eps, etc. from the release
    if let Some(release_node) = release_node {
        let release_privacy_usage = (*release_node.privacy_usage).to_vec();
        if release_privacy_usage.len() > 0 {
            privacy_usage = release_privacy_usage
        }
    }

    // sum privacy usage within the node
    privacy_usage.into_iter()
        .fold1(|usage_a, usage_b|
            privacy_usage_reducer(&usage_a, &usage_b, &|a, b| a + b))
}

pub fn privacy_usage_reducer(
    left: &proto::PrivacyUsage,
    right: &proto::PrivacyUsage,
    operator: &dyn Fn(f64, f64) -> f64,
) -> proto::PrivacyUsage {
    use proto::privacy_usage::Distance as Distance;

    proto::PrivacyUsage {
        distance: match (left.distance.to_owned().unwrap(), right.distance.to_owned().unwrap()) {
            (Distance::DistancePure(x), Distance::DistancePure(y)) => Some(Distance::DistancePure(proto::privacy_usage::DistancePure {
                epsilon: operator(x.epsilon, y.epsilon)
            })),
            (Distance::DistanceApproximate(x), Distance::DistanceApproximate(y)) => Some(Distance::DistanceApproximate(proto::privacy_usage::DistanceApproximate {
                epsilon: operator(x.epsilon, y.epsilon),
                delta: operator(x.delta, y.delta),
            })),
            _ => None
        }
    }
}

pub fn expand_component(
    privacy_definition: &proto::PrivacyDefinition,
    component: &proto::Component,
    properties: &HashMap<String, proto::ValueProperties>,
    arguments: &HashMap<String, Value>,
    node_id_output: u32,
    node_id_maximum: u32
) -> Result<proto::response_expand_component::ExpandedComponent> {

//    println!("expanding node id: {}", node_id_output);
//    println!("expansion properties before {:?}", properties);
    let mut properties: NodeProperties = properties.iter()
        .map(|(k, v)| (k.to_owned(), utilities::serial::parse_value_properties(&v)))
        .collect();

    for (k, v) in arguments {
        properties.insert(k.clone(), infer_property(&v)?);
    }

//    println!("expanding node id: {}", node_id_output);
//    println!("expansion properties after {:?}", properties);
//    println!("\n\n");
    let result = component.clone().variant.unwrap().expand_component(
        privacy_definition,
        component,
        &properties,
        node_id_output,
        node_id_maximum,
    ).chain_err(|| format!("at node_id {:?},", node_id_output))?;

    Ok(proto::response_expand_component::ExpandedComponent {
        computation_graph: Some(proto::ComputationGraph { value: result.1 }),
        properties: match result.0 > node_id_maximum {
            true => None,
            false => Some(utilities::serial::serialize_value_properties(&component.clone().variant.unwrap()
                .propagate_property(privacy_definition, arguments, &properties)
                .chain_err(|| format!("at node_id {:?},", node_id_output))?))
        },
        maximum_id: result.0
    })
}

// TODO: move this logic into lib
pub fn generate_report(
    analysis: &proto::Analysis,
    release: &proto::Release,

) -> Result<String>  {

    let graph = analysis.computation_graph.to_owned()
        .ok_or("the computation graph must be defined in an analysis")?
        .value;

    let (graph_properties, graph_expanded) = propagate_properties(&analysis, &release)?;
    let release = parse_release(&release)?;

    let release_schemas = graph.iter()
        .filter_map(|(node_id, component)| {
            let public_arguments = get_input_arguments(&component, &release).ok()?;
            let input_properties = get_input_properties(&component, &graph_properties).ok()?;
            let node_release = release.get(node_id)?;
            component.variant.clone().unwrap().summarize(
                &node_id,
                &component,
                &public_arguments,
                &input_properties,
                &node_release).ok()?
        })
        .flat_map(|v| v)
        .collect::<Vec<JSONRelease>>();

    match serde_json::to_string(&release_schemas) {
        Ok(serialized) => Ok(serialized),
        Err(_) => Err("unable to parse report into json".into())
    }
}
