use std::collections::HashMap;

use crate::proto;
use crate::utilities;


use crate::components::Component;



use crate::utilities::serial;
use crate::utilities::serial::{Vector1DNull, Vector2DJagged, Value, serialize_value};
use crate::base::{release_to_evaluations, GraphEvaluation, get_arguments_copy};


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

    pub fn get_min_f64_option(&self) -> Result<Vec<Option<f64>>, String> {
        match self.nature.to_owned() {
            Some(value) => match value {
                Nature::Continuous(continuous) => match continuous.min {
                    Vector1DNull::F64(bound) => Ok(bound),
                    _ => Err("min must be composed of floats".to_string())
                },
                _ => Err("min must be an array".to_string())
            },
            None => Err("nature is not defined".to_string())
        }
    }
    pub fn get_min_f64(&self) -> Result<Vec<f64>, String> {
        let bound = self.get_min_f64_option()?;
        let value = bound.iter().filter_map(|v| v.to_owned()).collect::<Vec<f64>>();
        match bound.len() == value.len() {
            true => Ok(value),
            false => Err("not all min are known".to_string())
        }
    }
    pub fn get_max_f64_option(&self) -> Result<Vec<Option<f64>>, String> {
        match self.nature.to_owned() {
            Some(value) => match value {
                Nature::Continuous(continuous) => match continuous.max {
                    Vector1DNull::F64(bound) => Ok(bound),
                    _ => Err("max must be composed of floats".to_string())
                },
                _ => Err("max must be an array".to_string())
            },
            None => Err("nature is not defined".to_string())
        }
    }
    pub fn get_max_f64(&self) -> Result<Vec<f64>, String> {
        let bound = self.get_max_f64_option()?;
        let value = bound.iter().filter_map(|v| v.to_owned()).collect::<Vec<f64>>();
        match bound.len() == value.len() {
            true => Ok(value),
            false => Err("not all max are known".to_string())
        }
    }
    // just for consistency
    pub fn get_n_option(&self) -> Result<Vec<Option<i64>>, String> {
        Ok(self.num_records.clone())
    }
    pub fn get_n(&self) -> Result<Vec<i64>, String> {
        let value = self.num_records.iter().map(|v| v.to_owned().unwrap()).collect::<Vec<i64>>();
        match self.num_records.len() == value.len() {
            true => Ok(value),
            false => Err("n is not known".to_string())
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

pub fn get_releasable_bool(constraints: &NodeConstraints, argument: &str) -> Result<bool, String> {
    match constraints.get(argument) {
        Some(constraint) => Ok(constraint.releasable),
        None => Err("constraint not found".to_string()),
    }
}

pub fn get_literal(value: &Value, batch: &u32) -> proto::Component {
    proto::Component {
        arguments: HashMap::new(),
        value: Some(proto::component::Value::Literal(proto::Literal {
            value: serialize_value(&value).ok(),
            private: false
        })),
        omit: true,
        batch: batch.clone(),
    }
}