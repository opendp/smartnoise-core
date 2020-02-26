use std::collections::HashMap;

use crate::proto;
use crate::utilities;


use crate::components::Component;



use crate::utilities::serial;
use crate::utilities::serial::{Vector1DNull, Vector2DJagged, Value, serialize_value};
use crate::base::{release_to_evaluations, GraphEvaluation, get_arguments_copy};


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

// TODO: implement property struct to/from proto
impl Properties {
    pub fn to_proto(&self) -> proto::Properties {
        proto::Properties {
            num_records: Some(serial::serialize_array1d_i64_null(&self.num_records)),
            num_columns: Some(serial::serialize_i64_null(&self.num_columns)),
            nullity: self.nullity,
            releasable: self.releasable,
            c_stability: Some(serial::serialize_array1d_f64(&self.c_stability)),
            nature: match &self.nature {
                Some(nature) => match nature {
                    Nature::Categorical(categorical) => Some(proto::properties::Nature::Categorical(proto::properties::NatureCategorical {
                        categories: Some(serial::serialize_array2d_jagged(&categorical.categories))
                    })),
                    Nature::Continuous(x) => Some(proto::properties::Nature::Continuous(proto::properties::NatureContinuous {
                        minimum: Some(serial::serialize_array1d_null(&x.min)),
                        maximum: Some(serial::serialize_array1d_null(&x.max)),
                    }))
                },
                None => None
            },
        }
    }
    pub fn from_proto(other: &proto::Properties) -> Properties {
        Properties {
            num_records: serial::parse_array1d_i64_null(&other.num_records.to_owned().unwrap()),
            num_columns: serial::parse_i64_null(&other.num_columns.to_owned().unwrap()),
            nullity: other.nullity,
            releasable: other.releasable,
            c_stability: serial::parse_array1d_f64(&other.c_stability.to_owned().unwrap()),
            nature: match other.nature.to_owned() {
                Some(nature) => match nature {
                    proto::properties::Nature::Continuous(continuous) =>
                        Some(Nature::Continuous(NatureContinuous {
                            min: serial::parse_array1d_null(&continuous.minimum.unwrap()),
                            max: serial::parse_array1d_null(&continuous.maximum.unwrap()),
                        })),
                    proto::properties::Nature::Categorical(categorical) =>
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
    pub fn assert_non_null(&self) -> Result<(), String> {
        match self.nullity {
            false => Ok(()),
            true => Err("DPMean requires non-null data".to_string())
        }
    }
}

// properties for each node in the graph
pub type GraphProperties = HashMap<u32, Properties>;

// properties for each argument for a node
pub type NodeProperties = HashMap<String, Properties>;

pub fn get_input_properties<T>(
    component: &proto::Component, graph_properties: &HashMap<u32, T>,
) -> HashMap<String, T> where T: std::clone::Clone {
    let mut properties = HashMap::<String, T>::new();
    component.arguments.iter().for_each(|(field_id, field)| {
        let property: T = graph_properties.get(&field).unwrap().clone();
        properties.insert(field_id.to_owned(), property);
    });
    properties
}

pub fn propagate_properties(
    analysis: &proto::Analysis,
    release: &proto::Release,
) -> Result<GraphProperties, String> {
    // compute properties for every node in the graph

    let graph: HashMap<u32, proto::Component> = analysis.computation_graph.to_owned().unwrap().value.to_owned();
    let traversal: Vec<u32> = utilities::graph::get_traversal(analysis)?;

    let graph_evaluation: GraphEvaluation = release_to_evaluations(&release)?;

    let mut graph_property = GraphProperties::new();
    traversal.iter().for_each(|node_id| {
        let component: proto::Component = graph.get(node_id).unwrap().to_owned();
        let input_properties = get_input_properties(&component, &graph_property);

        let public_arguments = get_arguments_copy(&component, &graph_evaluation);
        let property = component.value.unwrap().propagate_property(&public_arguments, &input_properties).unwrap();
        graph_property.insert(node_id.clone(), property);
    });
    Ok(graph_property)
}

//pub fn map_options<T>(left: Vec<T>, right: Vec<T>, operator: &dyn Fn(T, T) -> T) -> Result<Vec<T>, String> {
//
//}

pub fn get_properties<'a>(properties: &'a NodeProperties, argument: &str) -> Result<&'a Properties, String> {
    match properties.get(argument) {
        Some(property) => Ok(property),
        None => Err("property not found".to_string()),
    }
}

pub fn get_releasable_bool(properties: &NodeProperties, argument: &str) -> Result<bool, String> {
    match properties.get(argument) {
        Some(property) => Ok(property.releasable),
        None => Err("property not found".to_string()),
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