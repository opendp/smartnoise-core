use crate::errors::*;
use crate::ErrorKind::{PrivateError, PublicError};

use crate::proto;
use itertools::Itertools;

use crate::utilities;

use crate::components::*;
use ndarray::prelude::Ix1;

use std::collections::HashMap;


use crate::utilities::serial::{parse_value, serialize_value, parse_release};
use std::ops::Deref;
use ndarray::ArrayD;
use crate::utilities::inference::infer_property;

// equivalent to proto Release
pub type Release = HashMap<u32, Value>;

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

#[derive(Clone, Debug)]
pub enum ArrayND {
    Bool(ArrayD<bool>),
    I64(ArrayD<i64>),
    F64(ArrayD<f64>),
    Str(ArrayD<String>),
}

// used for categorical properties
#[derive(Clone, Debug)]
pub enum Vector2DJagged {
    Bool(Vec<Option<Vec<bool>>>),
    I64(Vec<Option<Vec<i64>>>),
    F64(Vec<Option<Vec<f64>>>),
    Str(Vec<Option<Vec<String>>>),
}

// used exclusively in the runtime for node evaluation
#[derive(Clone, Debug)]
pub enum Value {
    ArrayND(ArrayND),
    HashmapString(HashMap<String, Value>),
    Vector2DJagged(Vector2DJagged),
}

impl Value {
    pub fn get_arraynd(self) -> Result<ArrayND> {
        match self {
            Value::ArrayND(array) => Ok(array.to_owned()),
            _ => Err("value must be wrapped in an ArrayND".into())
        }
    }

    pub fn get_first_f64(self) -> Result<f64> {
        match self {
            Value::ArrayND(array) => array.get_first_f64(),
            _ => Err("cannot retrieve first float".into())
        }
    }
    pub fn get_first_i64(self) -> Result<i64> {
        match self {
            Value::ArrayND(array) => array.get_first_i64(),
            _ => Err("cannot retrieve integer".into())
        }
    }
    pub fn get_first_str(self) -> Result<String> {
        match self {
            Value::ArrayND(array) => array.get_first_str(),
            _ => Err("cannot retrieve string".into())
        }
    }
    pub fn get_first_bool(self) -> Result<bool> {
        match self {
            Value::ArrayND(array) => array.get_first_bool(),
            _ => Err("cannot retrieve bool".into())
        }
    }
}

impl ArrayND {
    pub fn get_f64(self) -> Result<ArrayD<f64>> {
        match self {
            ArrayND::Bool(x) => Ok(x.mapv(|v| if v { 1. } else { 0. })),
            ArrayND::I64(x) => Ok(x.mapv(|v| f64::from(v as i32))),
            ArrayND::F64(x) => Ok(x.to_owned()),
            _ => Err("expected a float on a non-float ArrayND".into())
        }
    }
    pub fn get_first_f64(self) -> Result<f64> {
        match self {
            ArrayND::Bool(x) => Ok(if *x.first().unwrap() { 1. } else { 0. }),
            ArrayND::I64(x) => Ok(f64::from(*x.first().unwrap() as i32)),
            ArrayND::F64(x) => Ok(x.first().unwrap().to_owned()),
            _ => Err("value must be numeric".into())
        }
    }
    pub fn get_vec_f64(self, optional_length: Option<i64>) -> Result<Vec<f64>> {
        let data = self.get_f64()?;
        let err_msg = "failed attempt to cast f64 ArrayD to vector".into();
        match data.ndim().clone() {
            0 => match (optional_length, data.first()) {
                (Some(length), Some(v)) => Ok((0..length).map(|_| v.clone()).collect()),
                _ => Err(err_msg)
            },
            1 => Ok(data.into_dimensionality::<Ix1>().unwrap().to_vec()),
            _ => Err(err_msg)
        }
    }
    pub fn get_i64(self) -> Result<ArrayD<i64>> {
        match self {
            ArrayND::Bool(x) => Ok(x.mapv(|v| if v { 1 } else { 0 })),
            ArrayND::I64(x) => Ok(x.to_owned()),
            _ => Err("expected a float on a non-float ArrayND".into())
        }
    }
    pub fn get_first_i64(self) -> Result<i64> {
        match self {
            ArrayND::Bool(x) => Ok(if *x.first().unwrap() { 1 } else { 0 }),
            ArrayND::I64(x) => Ok(x.first().unwrap().to_owned()),
            _ => Err("value must be numeric".into())
        }
    }
    pub fn get_vec_i64(self, optional_length: Option<i64>) -> Result<Vec<i64>> {
        let data = self.get_i64()?;
        let err_msg = "failed attempt to cast i64 ArrayD to vector".into();
        match data.ndim().clone() {
            0 => match (optional_length, data.first()) {
                (Some(length), Some(v)) => Ok((0..length).map(|_| v.clone()).collect()),
                _ => Err(err_msg)
            },
            1 => Ok(data.into_dimensionality::<Ix1>().unwrap().to_vec()),
            _ => Err(err_msg)
        }
    }
    pub fn get_str(self) -> Result<ArrayD<String>> {
        match self {
            ArrayND::Str(x) => Ok(x.to_owned()),
            _ => Err("value must be a string".into())
        }
    }
    pub fn get_first_str(self) -> Result<String> {
        match self {
            ArrayND::Str(x) => Ok(x.first().unwrap().to_owned()),
            _ => Err("value must be a string".into())
        }
    }
    pub fn get_bool(self) -> Result<ArrayD<bool>> {
        match self {
            ArrayND::Bool(x) => Ok(x.to_owned()),
            _ => Err("value must be a bool".into())
        }
    }
    pub fn get_first_bool(self) -> Result<bool> {
        match self {
            ArrayND::Bool(x) => Ok(x.first().unwrap().to_owned()),
            _ => Err("value must be a bool".into())
        }
    }
}

#[derive(Clone, Debug)]
pub struct Properties {
    pub nullity: bool,
    pub releasable: bool,
    pub nature: Option<Nature>,
    pub c_stability: Vec<f64>,
    pub num_columns: Option<i64>,
    // vector because some types, like the jagged matrix and hash table, may have mixed lengths
    pub num_records: Vec<Option<i64>>,
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

impl Properties {
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
    // just for consistency
    pub fn get_n_option(&self) -> Result<Vec<Option<i64>>> {
        Ok(self.num_records.clone())
    }
    pub fn get_n(&self) -> Result<Vec<i64>> {
        let value = self.num_records.iter().filter_map(|v| v.to_owned()).collect::<Vec<i64>>();
        match self.num_records.len() == value.len() {
            true => Ok(value),
            false => Err("n is not known".into())
        }
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
    pub fn get_categories(&self) -> Result<Vector2DJagged> {
        match self.nature.to_owned() {
            Some(nature) => match nature {
                Nature::Categorical(nature) => Ok(nature.categories),
                _ => Err("categories is not defined".into())
            },
            None => Err("categorical nature is not defined".into())
        }
    }
}

// properties for each node in the graph
pub type GraphProperties = HashMap<u32, Properties>;

// properties for each argument for a node
pub type NodeProperties = HashMap<String, Properties>;

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

pub fn get_argument(
    arguments: &HashMap<String, &Value>,
    name: &str
) -> Result<Value> {
    match arguments.get(name) {
        Some(argument) => Ok(argument.deref().to_owned()),
        _ => Err((name.to_string() + " is not defined").into())
    }
}

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

pub fn propagate_properties(
    analysis: &proto::Analysis,
    release: &proto::Release,
) -> Result<GraphProperties> {
    // compute properties for every node in the graph

    let graph: HashMap<u32, proto::Component> = analysis.computation_graph.to_owned().unwrap().value.to_owned();
    let traversal: Vec<u32> = utilities::graph::get_traversal(analysis)?;

    let graph_evaluation: Release = parse_release(&release)?;
    println!("GRAPH EVALUATION: {:?}", graph_evaluation);
    let mut graph_property = GraphProperties::new();

    for node_id in traversal {

        println!("node_id, {:?}", node_id);
        println!("{:?}", graph_evaluation.contains_key(&node_id));

        let property = match graph_evaluation.get(&node_id) {
            Some(value) => infer_property(&value)?,
            None => {
                let component: proto::Component = graph.get(&node_id).unwrap().to_owned();
                let input_properties = get_input_properties(&component, &graph_property)?;
                let public_arguments = get_input_arguments(&component, &graph_evaluation)?;

                component.value.unwrap().propagate_property(&public_arguments, &input_properties)?
            }
        };
        graph_property.insert(node_id.clone(), property);
    }
    Ok(graph_property)
}

pub fn get_constant(value: &Value, batch: &u32) -> proto::Component {
    proto::Component {
        arguments: HashMap::new(),
        value: Some(proto::component::Value::Constant(proto::Constant {
            value: serialize_value(&value).ok()
        })),
        omit: true,
        batch: batch.clone(),
    }
}

pub fn validate_analysis(
    analysis: &proto::Analysis,
    release: &proto::Release
) -> Result<proto::response_validate_analysis::Validated> {
    let graph = analysis.computation_graph.to_owned()
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
    let graph: &HashMap<u32, proto::Component> = &analysis.computation_graph.to_owned().unwrap().value;

    let usage_option = graph.iter()
        // optionally extract the minimum usage between the analysis and release
        .map(|(node_id, component)| get_component_privacy_usage(component, release.values.get(node_id)))
        // ignore nodes without privacy usage
        .filter(|privacy_usage| privacy_usage.is_some())
        .map(|privacy_usage| privacy_usage.unwrap())
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
    let privacy_usage_option: Option<proto::PrivacyUsage> = match component.to_owned().value? {
        proto::component::Value::Dpsum(x) => x.privacy_usage,
        proto::component::Value::Dpcount(x) => x.privacy_usage,
        proto::component::Value::Dpmean(x) => x.privacy_usage,
        proto::component::Value::Dpvariance(x) => x.privacy_usage,
        proto::component::Value::Dpmomentraw(x) => x.privacy_usage,
        proto::component::Value::Dpvariance(x) => x.privacy_usage,
        _ => None
    };

    if privacy_usage_option.is_none() {
        return None;
    }

    if let Some(release_node) = release_node {
        if let Some(release_node_usage) = &release_node.privacy_usage {
            return Some(privacy_usage_reducer(
                &privacy_usage_option.unwrap(),
                &release_node_usage,
                &|l, r| l.min(r)));
        }
    }
    privacy_usage_option
}

pub fn privacy_usage_reducer(
    left: &proto::PrivacyUsage,
    right: &proto::PrivacyUsage,
    operator: &dyn Fn(f64, f64) -> f64,
) -> proto::PrivacyUsage {
    use proto::privacy_usage::Usage as Usage;

    proto::PrivacyUsage {
        usage: match (left.usage.to_owned().unwrap(), right.usage.to_owned().unwrap()) {
            (Usage::DistancePure(x), Usage::DistancePure(y)) => Some(Usage::DistancePure(proto::privacy_usage::DistancePure {
                epsilon: operator(x.epsilon, y.epsilon)
            })),
            (Usage::DistanceApproximate(x), Usage::DistanceApproximate(y)) => Some(Usage::DistanceApproximate(proto::privacy_usage::DistanceApproximate {
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
    properties: &HashMap<String, proto::Properties>,
    arguments: &HashMap<String, Value>,
    node_id_output: u32,
    node_id_maximum: u32
) -> Result<proto::response_expand_component::ExpandedComponent> {
    let mut properties: NodeProperties = properties.iter()
        .map(|(k, v)| (k.to_owned(), utilities::serial::parse_properties(&v)))
        .collect();

    for (k, v) in arguments {
        properties.insert(k.clone(), infer_property(&v)?);
    }

    let result = component.clone().value.unwrap().expand_graph(
        privacy_definition,
        component,
        &properties,
        node_id_output,
        node_id_maximum,
    )?;

    let properties = component.clone().value.unwrap().propagate_property(arguments, &properties)?;

    Ok(proto::response_expand_component::ExpandedComponent {
        computation_graph: Some(proto::ComputationGraph { value: result.1 }),
        properties: Some(utilities::serial::serialize_properties(&properties)),
        maximum_id: result.0
    })
}

// TODO: create report json
pub fn generate_report(
    _analysis: &proto::Analysis,
    _release: &proto::Release,
) -> Result<String> {
    return Ok("{\"key\": \"value\"}".to_owned());
}