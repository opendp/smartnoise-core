use std::collections::HashMap;
use crate::hashmap;
use crate::proto;
use crate::utilities;

use crate::components;
use crate::components::Component;
use itertools::Itertools;
use crate::utilities::constraint::Nature::{Continuous, Categorical};
use ndarray::Array;
use crate::utilities::serial::{Array1DNull, Array2DJagged, parse_array2d_jagged, serialize_array2d_jagged, serialize_array1d_i64_null, parse_array1d_i64_null, parse_array1d_f64_null, serialize_array1d_f64_null, serialize_array1d_f64, parse_array1d_f64};
use crate::utilities::serial::{parse_array1d_null, serialize_array1d_null};


#[derive(Clone, Debug)]
pub struct Constraint {
    pub nullity: bool,
    pub releasable: bool,
    pub nature: Option<Nature>,
    pub c_stability: Vec<f64>,
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
    pub categories: Array2DJagged
}

#[derive(Clone, Debug)]
pub struct NatureContinuous {
    pub min: Array1DNull,
    pub max: Array1DNull,
}

// TODO: implement constraint struct to/from proto
impl Constraint {
    pub fn to_proto(&self) -> proto::Constraint {
        proto::Constraint {
            num_records: Some(serialize_array1d_i64_null(&self.num_records)),
            nullity: self.nullity,
            releasable: self.releasable,
            c_stability: Some(serialize_array1d_f64(&self.c_stability)),
            nature: match &self.nature {
                Some(nature) => match nature {
                    Nature::Categorical(categorical) => Some(proto::constraint::Nature::Categorical(proto::constraint::NatureCategorical {
                        categories: Some(serialize_array2d_jagged(&categorical.categories))
                    })),
                    Nature::Continuous(x) => Some(proto::constraint::Nature::Continuous(proto::constraint::NatureContinuous {
                        minimum: Some(serialize_array1d_null(&x.min)),
                        maximum: Some(serialize_array1d_null(&x.max)),
                    }))
                },
                None => None
            },
        }
    }
    pub fn from_proto(other: &proto::Constraint) -> Constraint {
        Constraint {
            nullity: other.nullity,
            c_stability: parse_array1d_f64(&other.c_stability.to_owned().unwrap()),
            nature: match other.nature.to_owned() {
                Some(nature) => match nature {
                    proto::constraint::Nature::Continuous(continuous) =>
                        Some(Nature::Continuous(NatureContinuous {
                            min: parse_array1d_null(&continuous.minimum.unwrap()),
                            max: parse_array1d_null(&continuous.maximum.unwrap()),
                        })),
                    proto::constraint::Nature::Categorical(categorical) =>
                        Some(Nature::Categorical(NatureCategorical {
                            categories: parse_array2d_jagged(&categorical.categories.unwrap())
                        }))
                },
                None => None
            },
            releasable: other.releasable,
            num_records: parse_array1d_i64_null(&other.num_records.to_owned().unwrap())
        }
    }
}

// constraints for each node in the graph
pub type GraphConstraint = HashMap<u32, Constraint>;

// constraints for each argument for a node
pub type NodeConstraints = HashMap<String, Constraint>;

pub fn get_constraints<T>(
    component: &proto::Component, graph_constraints: &HashMap<u32, T>,
) -> HashMap<String, T> where T: std::clone::Clone {
    let mut constraints = HashMap::<String, T>::new();
    component.arguments.iter().for_each(|(field_id, field)| {
        let constraint: T = graph_constraints.get(&field).unwrap().clone();
        constraints.insert(field_id.to_owned(), constraint);
    });
    constraints
}

pub fn propagate_constraints(
    analysis: &proto::Analysis,
    release: &proto::Release,
) -> Result<GraphConstraint, String> {
    // compute properties for every node in the graph

    let mut graph: HashMap<u32, proto::Component> = analysis.computation_graph.to_owned().unwrap().value.to_owned();
    let traversal: Vec<u32> = utilities::graph::get_traversal(analysis)?;

    let mut graph_constraint = GraphConstraint::new();
    traversal.iter().for_each(|node_id| {
        let component: proto::Component = graph.get(node_id).unwrap().to_owned();
        let input_constraints = get_constraints(&component, &graph_constraint);
        let constraint = component.value.unwrap().propagate_constraint(&input_constraints).unwrap();
        graph_constraint.insert(node_id.clone(), constraint);
    });
    Ok(graph_constraint)
}

//pub fn map_options<T>(left: Vec<T>, right: Vec<T>, operator: &dyn Fn(T, T) -> T) -> Result<Vec<T>, String> {
//
//}

pub fn get_constraint<'a>(constraints: &'a NodeConstraints, argument: &str) -> Result<&'a Constraint, String> {
    match constraints.get(argument) {
        Some(constraint) => Ok(constraint),
        None => Err("constraint not found".to_string()),
    }
}

pub fn get_min(constraints: &NodeConstraints, argument: &str) -> Result<Array1DNull, String> {
    let nature = match &get_constraint(constraints, argument)?.nature {
        Some(nature) => match nature {
            Nature::Continuous(nature) => nature,
            _ => return Err("a categorical constraint is defined on a continuous argument".to_string())
        },
        None => return Err("no nature (min) is defined on a continuous argument".to_string())
    };
    Ok(nature.min.clone())
}

pub fn get_min_f64(constraints: &NodeConstraints, argument: &str) -> Result<Vec<Option<f64>>, String> {
    let min = get_min(constraints, argument)?;

    match min {
        Array1DNull::F64(value) => Ok(value.to_owned()),
        Array1DNull::I64(value) => Ok(value.iter()
            .map(|&x| match x {Some(x) => Some(x as f64), None => None}).collect()),
        _ => Err("the min must be a numeric type".to_string())
    }
}

pub fn get_max(constraints: &NodeConstraints, argument: &str) -> Result<Array1DNull, String> {
    let nature = match &get_constraint(constraints, argument)?.nature {
        Some(nature) => match nature {
            Nature::Continuous(nature) => nature,
            _ => return Err("a categorical constraint is defined on a continuous argument".to_string())
        },
        None => return Err("no nature (max) is defined on a continuous argument".to_string())
    };
    Ok(nature.max.clone())
}

pub fn get_max_f64(constraints: &NodeConstraints, argument: &str) -> Result<Vec<Option<f64>>, String> {
    let max = get_max(constraints, argument)?;

    match max {
        Array1DNull::F64(value) => Ok(value.to_owned()),
        Array1DNull::I64(value) => Ok(value.iter()
            .map(|&x| match x {Some(x) => Some(x as f64), None => None}).collect()),
        _ => Err("the max must be a numeric type".to_string())
    }
}

pub fn get_num_records(constraints: &NodeConstraints, argument: &str) -> Result<Vec<Option<i64>>, String> {
    Ok(get_constraint(constraints, argument)?.to_owned().num_records)
}

pub fn get_releasable_bool(constraints: &NodeConstraints, argument: &str) -> Result<bool, String> {
    match constraints.get(argument) {
        Some(constraint) => Ok(constraint.releasable),
        None => Err("constraint not found".to_string()),
    }
}

//pub fn get_conservative_bounds(
//    bounds: Vec<Option<Vec<f32>>>,
//    operator: &dyn Fn(&f64, &f64) -> f64
//) -> Option<Vec<f64>> {
//    if !bounds.iter().all(|bound| bound.is_some()) {
//        return None;
//    }
////    bounds.iter().fold1(|accum, bound| bound.unwrap().map())
//
//}