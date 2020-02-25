use std::collections::HashMap;

use crate::proto;
use crate::utilities;


use crate::components::Component;



use crate::utilities::serial;
use crate::utilities::serial::{Vector1DNull, Vector2DJagged};
use crate::utilities::buffer::{release_to_evaluations, GraphEvaluation, get_arguments_copy};


#[derive(Clone, Debug)]
pub struct Constraint {
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

// TODO: implement constraint struct to/from proto
impl Constraint {
    pub fn to_proto(&self) -> proto::Constraint {
        proto::Constraint {
            num_records: Some(serial::serialize_array1d_i64_null(&self.num_records)),
            num_columns: Some(serial::serialize_i64_null(&self.num_columns)),
            nullity: self.nullity,
            releasable: self.releasable,
            c_stability: Some(serial::serialize_array1d_f64(&self.c_stability)),
            nature: match &self.nature {
                Some(nature) => match nature {
                    Nature::Categorical(categorical) => Some(proto::constraint::Nature::Categorical(proto::constraint::NatureCategorical {
                        categories: Some(serial::serialize_array2d_jagged(&categorical.categories))
                    })),
                    Nature::Continuous(x) => Some(proto::constraint::Nature::Continuous(proto::constraint::NatureContinuous {
                        minimum: Some(serial::serialize_array1d_null(&x.min)),
                        maximum: Some(serial::serialize_array1d_null(&x.max)),
                    }))
                },
                None => None
            },
        }
    }
    pub fn from_proto(other: &proto::Constraint) -> Constraint {
        Constraint {
            num_records: serial::parse_array1d_i64_null(&other.num_records.to_owned().unwrap()),
            num_columns: serial::parse_i64_null(&other.num_columns.to_owned().unwrap()),
            nullity: other.nullity,
            releasable: other.releasable,
            c_stability: serial::parse_array1d_f64(&other.c_stability.to_owned().unwrap()),
            nature: match other.nature.to_owned() {
                Some(nature) => match nature {
                    proto::constraint::Nature::Continuous(continuous) =>
                        Some(Nature::Continuous(NatureContinuous {
                            min: serial::parse_array1d_null(&continuous.minimum.unwrap()),
                            max: serial::parse_array1d_null(&continuous.maximum.unwrap()),
                        })),
                    proto::constraint::Nature::Categorical(categorical) =>
                        Some(Nature::Categorical(NatureCategorical {
                            categories: serial::parse_array2d_jagged(&categorical.categories.unwrap())
                        }))
                },
                None => None
            }
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

    let graph: HashMap<u32, proto::Component> = analysis.computation_graph.to_owned().unwrap().value.to_owned();
    let traversal: Vec<u32> = utilities::graph::get_traversal(analysis)?;

    let graph_evaluation: GraphEvaluation = release_to_evaluations(&release)?;

    let mut graph_constraint = GraphConstraint::new();
    traversal.iter().for_each(|node_id| {
        let component: proto::Component = graph.get(node_id).unwrap().to_owned();
        let input_constraints = get_constraints(&component, &graph_constraint);

        let public_arguments = get_arguments_copy(&component, &graph_evaluation);
        let constraint = component.value.unwrap().propagate_constraint(&public_arguments, &input_constraints).unwrap();
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

pub fn get_min(constraints: &NodeConstraints, argument: &str) -> Result<Vector1DNull, String> {
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
        Vector1DNull::F64(value) => Ok(value.to_owned()),
        Vector1DNull::I64(value) => Ok(value.iter()
            .map(|&x| match x {Some(x) => Some(x as f64), None => None}).collect()),
        _ => Err("the min must be a numeric type".to_string())
    }
}

pub fn get_max(constraints: &NodeConstraints, argument: &str) -> Result<Vector1DNull, String> {
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
        Vector1DNull::F64(value) => Ok(value.to_owned()),
        Vector1DNull::I64(value) => Ok(value.iter()
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