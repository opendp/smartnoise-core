use crate::errors::*;


use crate::proto;
use itertools::Itertools;

use crate::utilities;

use crate::components::*;
use ndarray::prelude::Ix1;

use std::collections::HashMap;

use crate::utilities::serial::{serialize_value, parse_release};
use crate::utilities::json::{JSONRelease};

use std::ops::Deref;
use ndarray::{ArrayD, Array};
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
    pub fn get_arraynd<'a>(&'a self) -> Result<&'a ArrayND> {
        match self {
            Value::ArrayND(array) => Ok(array),
            _ => Err("value must be wrapped in an ArrayND".into())
        }
    }

    pub fn get_first_f64(&self) -> Result<f64> {
        match self {
            Value::ArrayND(array) => array.get_first_f64(),
            _ => Err("cannot retrieve first float".into())
        }
    }
    pub fn get_first_i64(&self) -> Result<i64> {
        match self {
            Value::ArrayND(array) => array.get_first_i64(),
            _ => Err("cannot retrieve integer".into())
        }
    }
    pub fn get_first_str(&self) -> Result<String> {
        match self {
            Value::ArrayND(array) => array.get_first_str(),
            _ => Err("cannot retrieve string".into())
        }
    }
    pub fn get_first_bool(&self) -> Result<bool> {
        match self {
            Value::ArrayND(array) => array.get_first_bool(),
            _ => Err("cannot retrieve bool".into())
        }
    }

    pub fn get_jagged<'a>(&'a self) -> Result<&'a Vector2DJagged> {
        match self {
            Value::Vector2DJagged(jagged) => Ok(jagged),
            _ => Err("value must be wrapped in an Vector2DJagged".into())
        }
    }
}

impl ArrayND {
    pub fn get_f64(&self) -> Result<&ArrayD<f64>> {
        match self {
//            ArrayND::Bool(x) => Ok(x.mapv(|v| if v { 1. } else { 0. })),
//            ArrayND::I64(x) => Ok(x.mapv(|v| f64::from(v as i32))),
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

impl Vector2DJagged {
    pub fn get_f64(&self) -> Result<Vec<Vec<f64>>> {
        match self {
            Vector2DJagged::F64(data) => data.iter().cloned().collect::<Option<Vec<Vec<f64>>>>()
                .ok_or::<Error>("not all columns are known in float Vector2DJagged".into()),
            _ => Err("expected float type on a non-float Vector2DJagged".into())
        }
    }
    pub fn get_i64(&self) -> Result<Vec<Vec<i64>>> {
        match self {
            Vector2DJagged::I64(data) => data.iter().cloned().collect::<Option<Vec<Vec<i64>>>>()
                .ok_or::<Error>("not all columns are known in int Vector2DJagged".into()),
            _ => Err("expected int type on a non-int Vector2DJagged".into())
        }
    }
    pub fn get_str(&self) -> Result<Vec<Vec<String>>> {
        match self {
            Vector2DJagged::Str(data) => data.iter().cloned().collect::<Option<Vec<Vec<String>>>>()
                .ok_or::<Error>("not all columns are known in string Vector2DJagged".into()),
            _ => Err("expected string type on a non-string Vector2DJagged".into())
        }
    }
    pub fn get_bool(&self) -> Result<Vec<Vec<bool>>> {
        match self {
            Vector2DJagged::Bool(data) => data.iter().cloned().collect::<Option<Vec<Vec<bool>>>>()
                .ok_or::<Error>("not all columns are known in bool Vector2DJagged".into()),
            _ => Err("expected bool type on a non-bool Vector2DJagged".into())
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
    pub aggregator: Option<AggregatorProperties>
}

#[derive(Clone, Debug)]
pub struct AggregatorProperties {
    pub component: proto::component::Variant,
    pub properties: HashMap<String, Properties>
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
    pub fn assert_is_not_aggregated(&self) -> Result<()> {
        match self.aggregator.to_owned() {
            Some(aggregator) => Err("aggregated data may not be manipulated".into()),
            None => Ok(())
        }
    }
    pub fn get_num_columns(&self) -> Result<i64> {
        match self.num_columns {
            Some(num_columns) => Ok(num_columns),
            None => Err("number of columns is not defined".into())
        }
    }
}

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

pub fn get_argument<'a>(
    arguments: &HashMap<String, &'a Value>,
    name: &str
) -> Result<&'a Value> {
    match arguments.get(name) {
        Some(argument) => Ok(argument),
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
) -> Result<HashMap<u32, Properties>> {
    // compute properties for every node in the graph

    let privacy_definition = analysis.privacy_definition.to_owned().unwrap();
    let mut graph: HashMap<u32, proto::Component> = analysis.computation_graph.to_owned().unwrap().value.to_owned();
    let mut traversal: Vec<u32> = utilities::graph::get_traversal(&graph)?;
    traversal.reverse();

    let graph_evaluation: Release = parse_release(&release)?;
//    println!("GRAPH EVALUATION: {:?}", graph_evaluation);
    let mut graph_properties = HashMap::<u32, Properties>::new();

    let mut maximum_id = graph.keys().cloned()
        .fold(0, std::cmp::max);

    while !traversal.is_empty() {
        let node_id = traversal.last().unwrap().clone();

        let component: proto::Component = graph.get(&node_id).unwrap().to_owned();
        println!("Propagating properties at node_id {:?} {:?}", node_id, component);

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

                let result = component.clone().variant.unwrap().expand_graph(
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
                    &privacy_definition, &public_arguments, &input_properties)?
            }
        };
        graph_properties.insert(node_id.clone(), properties);
    }
    Ok(graph_properties)
}

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

pub fn uniform_density(length: usize) -> Vec<f64> {
    (0..length).map(|_| 1. / (length as f64)).collect()
}

pub fn normalize_probabilities(probabilities: &Vec<f64>) -> Vec<f64> {
    let sum: f64 = probabilities.iter().sum();
    probabilities.iter().map(|prob| prob / sum).collect()
}

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
        i if i == 1 => Ok(v.clone().first().unwrap().clone()),
        _ => Err("only one null value may be defined".into())
    }).collect::<Result<Vec<T>>>()?;

    // broadcast nulls across all columns, if only one null set is defined
    if value.len() == 1 {
        value = (0..*length).map(|_| value.clone().first().unwrap().clone()).collect();
    }
    Ok(value)
}

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
    let graph: &HashMap<u32, proto::Component> = &analysis.computation_graph.to_owned().unwrap().value;

    let usage_option = graph.iter()
        // optionally extract the minimum usage between the analysis and release
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

    // otherwise return the maximum possible usage allowed to the component
    let mut privacy_usage: Vec<proto::PrivacyUsage> = match component.to_owned().variant? {
        proto::component::Variant::Dpsum(x) => x.privacy_usage,
        proto::component::Variant::Dpcount(x) => x.privacy_usage,
        proto::component::Variant::Dpmean(x) => x.privacy_usage,
        proto::component::Variant::Dpvariance(x) => x.privacy_usage,
        proto::component::Variant::Dpmomentraw(x) => x.privacy_usage,
        _ => return None
    };

    // if release usage is defined, then use the actual eps, etc. from the release
    if let Some(release_node) = release_node {
        privacy_usage = (*release_node.privacy_usage).to_vec();
    }

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

    let result = component.clone().variant.unwrap().expand_graph(
        privacy_definition,
        component,
        &properties,
        node_id_output,
        node_id_maximum,
    )?;

    Ok(proto::response_expand_component::ExpandedComponent {
        computation_graph: Some(proto::ComputationGraph { value: result.1 }),
        properties: match result.0 > node_id_maximum {
            false => Some(utilities::serial::serialize_properties(&component.clone().variant.unwrap()
                .propagate_property(privacy_definition, arguments, &properties)?)),
            true => None
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

    let graph_properties = propagate_properties(&analysis, &release)?;
    let release = parse_release(&release)?;


    let release_schemas = graph.iter()
        .filter_map(|(node_id, component)| {
            let input_properties = get_input_properties(&component, &graph_properties).ok()?;
            let node_release = release.get(node_id)?;
            component.variant.clone().unwrap().summarize(&node_id, &component, &input_properties, &node_release)
        })
        .flat_map(|v| v)
        .collect::<Vec<JSONRelease>>();

    match serde_json::to_string(&release_schemas) {
        Ok(serialized) => Ok(serialized),
        Err(_) => Err("unable to parse report into json".into())
    }
}
