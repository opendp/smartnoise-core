use std::collections::HashMap;
use crate::hashmap;
use crate::proto;
use crate::utilities;

use crate::components;
use crate::components::Component;


#[derive(Clone, Debug)]
pub struct Constraint {
    pub nullity: bool,
    pub releasable: bool,
    pub nature: Option<Nature>,
    pub num_records: Option<u32>,
}

#[derive(Clone, Debug)]
pub enum Nature {
    Continuous(NatureContinuous),
    Categorical(NatureCategorical),
}

#[derive(Clone, Debug)]
pub struct NatureCategorical {
    pub categories: Vec<Option<ConstraintVector>>
}

#[derive(Clone, Debug)]
pub struct NatureContinuous {
    pub min: Option<ConstraintVector>,
    pub max: Option<ConstraintVector>,
}

#[derive(Clone, Debug)]
pub enum ConstraintVector {
    Bool(Vec<bool>),
    I64(Vec<i64>),
    F64(Vec<f64>),
    Str(Vec<String>),
}

// TODO: implement constraint struct to/from proto
impl Constraint {
    pub fn to_proto(&self) -> proto::Constraint {
        proto::Constraint {
            num_records: match self.num_records {
                Some(x) => x as i32,
                None => -1
            },
            nullity: self.nullity,
            releasable: self.releasable,
            nature: match &self.nature {
                Some(nature) => match nature {
                    Nature::Categorical(x) => Some(proto::constraint::Nature::Categorical(proto::constraint::NatureCategorical {
                        categories: x.categories.iter()
                            .map(|constraint_categories| proto::constraint::Categories {
                                data: match serialize_proto_vector(constraint_categories) {
                                    Some(data) => Some(proto::constraint::categories::Data::Option(data)),
                                    None => None
                                }
                            }).collect()
                    })),
                    Nature::Continuous(x) => Some(proto::constraint::Nature::Continuous(proto::constraint::NatureContinuous {
                        minimum: serialize_proto_vector(&x.min),
                        maximum: serialize_proto_vector(&x.max),
                    }))
                },
                None => None
            },
        }
    }
    pub fn from_proto(other: &proto::Constraint) -> Constraint {
        Constraint {
            nullity: other.nullity,
            nature: match other.nature.to_owned() {
                Some(nature) => match nature {
                    proto::constraint::Nature::Continuous(continuous) =>
                        Some(Nature::Continuous(NatureContinuous {
                            min: parse_proto_vector(&continuous.minimum),
                            max: parse_proto_vector(&continuous.maximum),
                        })),
                    proto::constraint::Nature::Categorical(categorical) =>
                        Some(Nature::Categorical(NatureCategorical {
                            categories: categorical.categories.iter()
                                .map(|categories: &proto::constraint::Categories| match &categories.data {
                                    Some(data) => match data {
                                        proto::constraint::categories::Data::Option(vector) => parse_proto_vector(&Some(vector.to_owned()))
                                    },
                                    None => None
                                }).collect::<Vec<Option<ConstraintVector>>>(),
                        }))
                },
                None => None
            },
            releasable: other.releasable,
            num_records: match other.num_records {
                x if x < 0 => None,
                x => Some(x as u32)
            },
        }
    }
}

pub fn parse_proto_vector(array: &Option<proto::Array1D>) -> Option<ConstraintVector> {
    match array {
        Some(array) => match array.data.to_owned() {
            Some(data) => match data {
                proto::array1_d::Data::Bool(typed) => Some(ConstraintVector::Bool(typed.data)),
                proto::array1_d::Data::I64(typed) => Some(ConstraintVector::I64(typed.data)),
                proto::array1_d::Data::F64(typed) => Some(ConstraintVector::F64(typed.data)),
                proto::array1_d::Data::String(typed) => Some(ConstraintVector::Str(typed.data))
            },
            None => None
        },
        None => None
    }
}

pub fn serialize_proto_vector(vector: &Option<ConstraintVector>) -> Option<proto::Array1D> {
    Some(proto::Array1D {
        data: match vector {
            Some(data) => match data {
                ConstraintVector::Bool(typed) => Some(proto::array1_d::Data::Bool(proto::Array1Dbool { data: typed.to_owned() })),
                ConstraintVector::I64(typed) => Some(proto::array1_d::Data::I64(proto::Array1Di64 { data: typed.to_owned() })),
                ConstraintVector::F64(typed) => Some(proto::array1_d::Data::F64(proto::Array1Df64 { data: typed.to_owned() })),
                ConstraintVector::Str(typed) => Some(proto::array1_d::Data::String(proto::Array1Dstr { data: typed.to_owned() })),
            },
            None => return None
        }
    })
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


pub fn get_min(constraints: &NodeConstraints, argument: &str) -> Result<Option<ConstraintVector>, String> {
    let constraint = match constraints.get(argument) {
        Some(constraint) => constraint,
        None => return Err("constraint not found".to_string()),
    };

    let nature = match &constraint.nature {
        Some(nature) => match nature {
            Nature::Continuous(nature) => nature,
            _ => return Err("a categorical constraint is defined on a continuous argument".to_string())
        },
        None => return Err("no nature (min) is defined on a continuous argument".to_string())
    };
    Ok(nature.min.clone())
}

pub fn get_min_f64(constraints: &NodeConstraints, argument: &str) -> Result<Vec<f64>, String> {
    let min = match get_min(constraints, argument)? {
        Some(min) => min.to_owned(),
        None => return Err("no min is defined on a continuous argument".to_string())
    };

    match min {
        ConstraintVector::F64(value) => Ok(value.to_owned()),
        ConstraintVector::I64(value) => Ok(value.iter().map(|&x| x as f64).collect()),
        _ => Err("the min must be a numeric type".to_string())
    }
}

pub fn get_max(constraints: &NodeConstraints, argument: &str) -> Result<Option<ConstraintVector>, String> {
    let constraint = match constraints.get(argument) {
        Some(constraint) => constraint,
        None => return Err("constraint not found".to_string()),
    };

    let nature = match &constraint.nature {
        Some(nature) => match nature {
            Nature::Continuous(nature) => nature,
            _ => return Err("a categorical constraint is defined on a continuous argument".to_string())
        },
        None => return Err("no nature (max) is defined on a continuous argument".to_string())
    };
    Ok(nature.max.clone())
}

pub fn get_max_f64(constraints: &NodeConstraints, argument: &str) -> Result<Vec<f64>, String> {
    let max = match get_max(constraints, argument)? {
        Some(max) => max.to_owned(),
        None => return Err("no max is defined on a continuous argument".to_string())
    };

    match max {
        ConstraintVector::F64(value) => Ok(value.to_owned()),
        ConstraintVector::I64(value) => Ok(value.iter().map(|&x| x as f64).collect()),
        _ => Err("the max must be a numeric type".to_string())
    }
}

pub fn get_num_records(constraints: &NodeConstraints, argument: &str) -> Result<Option<u32>, String> {
    match constraints.get(argument) {
        Some(constraint) => Ok(constraint.num_records),
        None => Err("constraint not found".to_string()),
    }
}

pub fn get_num_records_u32(constraints: &NodeConstraints, argument: &str) -> Result<u32, String> {
    match get_num_records(constraints, argument)? {
        Some(x) => Ok(x),
        None => Err("n is not known".to_string())
    }
}

pub fn get_releasable_bool(constraints: &NodeConstraints, argument: &str) -> Result<bool, String> {
    match constraints.get(argument) {
        Some(constraint) => Ok(constraint.releasable),
        None => Err("constraint not found".to_string()),
    }
}
