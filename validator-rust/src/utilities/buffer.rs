use ndarray::prelude::*;

use std::collections::{HashMap, HashSet, VecDeque};
use crate::proto;


// equivalent to proto Value
#[derive(Debug, Clone)]
pub enum NodeEvaluation {
    //    Bytes(bytes::Bytes),
    Bool(ArrayD<bool>),
    I64(ArrayD<i64>),
    F64(ArrayD<f64>),
    Str(ArrayD<String>),
    HashmapString(HashMap<String, NodeEvaluation>),
    Vec(Vec<NodeEvaluation>),
}

// equivalent to proto Release
pub type GraphEvaluation = HashMap<u32, NodeEvaluation>;

// arguments to a node prior to evaluation
pub type NodeArguments<'a> = HashMap<String, &'a NodeEvaluation>;

pub fn get_arguments<'a>(component: &proto::Component, graph_evaluation: &'a GraphEvaluation) -> NodeArguments<'a> {
    let mut arguments = NodeArguments::new();
    component.arguments.iter().for_each(|(field_id, field)| {
        let evaluation: &'a NodeEvaluation = graph_evaluation.get(&field).unwrap();
        arguments.insert(field_id.to_owned(), evaluation);
    });
    arguments
}


pub fn get_f64(arguments: &NodeArguments, column: &str) -> f64 {
    match arguments.get(column).unwrap() {
        NodeEvaluation::Bool(x) => Ok(if *x.first().unwrap() { 1. } else { 0. }),
        NodeEvaluation::I64(x) => Ok(f64::from(*x.first().unwrap() as i32)),
        NodeEvaluation::F64(x) => Ok(x.first().unwrap().to_owned()),
        _ => Err(column.to_string() + " must be numeric")
    }.unwrap()
}

pub fn get_array_f64(arguments: &NodeArguments, column: &str) -> ArrayD<f64> {
    match arguments.get(column).unwrap() {
        NodeEvaluation::Bool(x) => Ok(x.mapv(|v| if v { 1. } else { 0. })),
        NodeEvaluation::I64(x) => Ok(x.mapv(|v| f64::from(v as i32))),
        NodeEvaluation::F64(x) => Ok(x.to_owned()),
        _ => Err(column.to_string() + " must be numeric")
    }.unwrap()
}

pub fn get_i64(arguments: &NodeArguments, column: &str) -> i64 {
    match arguments.get(column).unwrap() {
        NodeEvaluation::Bool(x) => Ok(if *x.first().unwrap() { 1 } else { 0 }),
        NodeEvaluation::I64(x) => Ok(x.first().unwrap().to_owned()),
        _ => Err(column.to_string() + " must be integer")
    }.unwrap()
}

pub fn get_array_i64(arguments: &NodeArguments, column: &str) -> ArrayD<i64> {
    match arguments.get(column).unwrap() {
        NodeEvaluation::Bool(x) => Ok(x.mapv(|v| if v { 1 } else { 0 })),
        NodeEvaluation::I64(x) => Ok(x.to_owned()),
        _ => Err(column.to_string() + " must be integer")
    }.unwrap()
}

pub fn get_str(arguments: &NodeArguments, column: &str) -> String {
    match arguments.get(column).unwrap() {
        NodeEvaluation::Str(x) => Ok(x.first().unwrap().to_owned()),
        _ => Err(column.to_string() + " must be string")
    }.unwrap()
}

pub fn get_array_str(arguments: &NodeArguments, column: &str) -> ArrayD<String> {
    match arguments.get(column).unwrap() {
        NodeEvaluation::Str(x) => Ok(x.to_owned()),
        _ => Err(column.to_string() + " must be string")
    }.unwrap()
}

pub fn get_bool(arguments: &NodeArguments, column: &str) -> bool {
    match arguments.get(column).unwrap() {
        NodeEvaluation::Bool(x) => Ok(x.first().unwrap().to_owned()),
        _ => Err(column.to_string() + " must be boolean")
    }.unwrap()
}

pub fn get_array_bool(arguments: &NodeArguments, column: &str) -> ArrayD<bool> {
    match arguments.get(column).unwrap() {
        NodeEvaluation::Bool(x) => Ok(x.to_owned()),
        _ => Err(column.to_string() + " must be boolean")
    }.unwrap()
}

pub fn release_to_evaluations(release: &proto::Release) -> Result<GraphEvaluation, String> {
    let mut evaluations = GraphEvaluation::new();

    for (node_id, node_release) in &release.values {
        evaluations.insert(*node_id, parse_proto_value(&node_release.value.to_owned().unwrap())?);
    }
    Ok(evaluations)
}

pub fn evaluations_to_release(evaluations: &GraphEvaluation) -> Result<proto::Release, String> {
    let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();
    for (node_id, node_eval) in evaluations {
        if let Ok(array_serialized) = serialize_proto_value(node_eval) {
            releases.insert(*node_id, proto::ReleaseNode {
                value: Some(array_serialized),
                privacy_usage: None,
            });
        }
    }
    Ok(proto::Release {
        values: releases
    })
}

pub fn parse_proto_value(value: &proto::Value) -> Result<NodeEvaluation, String> {
    let value = value.to_owned().data;
    if value.is_none() {
        return Err("proto value is empty".to_string());
    }
    match value.unwrap() {
        proto::value::Data::ArrayNd(arrayND) => match arrayND.flattened {
            Some(flattened) => match flattened.data {
                Some(data) => match data {
                    proto::array1_d::Data::Bool(array) => {
                        let shape: Vec<usize> = arrayND.shape.iter().map(|x| *x as usize).collect();
                        Ok(NodeEvaluation::Bool(Array::from_shape_vec(shape, array.data).unwrap().into_dyn()))
                    }
                    proto::array1_d::Data::I64(array) => {
                        let shape: Vec<usize> = arrayND.shape.iter().map(|x| *x as usize).collect();
                        Ok(NodeEvaluation::I64(Array::from_shape_vec(shape, array.data).unwrap().into_dyn()))
                    }
                    proto::array1_d::Data::F64(array) => {
                        let shape: Vec<usize> = arrayND.shape.iter().map(|x| *x as usize).collect();
                        Ok(NodeEvaluation::F64(Array::from_shape_vec(shape, array.data).unwrap().into_dyn()))
                    }
                    proto::array1_d::Data::String(array) => {
                        let shape: Vec<usize> = arrayND.shape.iter().map(|x| *x as usize).collect();
                        Ok(NodeEvaluation::Str(Array::from_shape_vec(shape, array.data).unwrap().into_dyn()))
                    }
                    _ => return Err("unsupported proto array variant encountered".to_string())
                },
                None => return Err("proto array is empty".to_string())
            },
            None => return Err("proto array is empty".to_string())
        },
        proto::value::Data::HashmapString(hash_map) => {
            let mut evaluation: HashMap<String, NodeEvaluation> = HashMap::new();
            for (node_id, value) in &hash_map.data {
                let parsed_result = parse_proto_value(value);
                if let Ok(parsed) = parsed_result {
                    evaluation.insert(node_id.to_owned(), parsed);
                } else {
                    return parsed_result;
                }
            }
            Ok(NodeEvaluation::HashmapString(evaluation))
        }
        proto::value::Data::JaggedArray2D(jagged) => Ok(NodeEvaluation::Vec(jagged.data.iter()
            .map(|categories: &proto::jagged_array2_d::OptionalArray1D| match &categories.data {
                Some(data) => match data {
                    proto::jagged_array2_d::optional_array1_d::Data::Option(vector) => match vector.data.to_owned() {
                        Some(data) => match data {
                            proto::array1_d::Data::Bool(data) =>
                                NodeEvaluation::Bool(Array::from(data.data).into_dyn()),
                            proto::array1_d::Data::I64(data) =>
                                NodeEvaluation::I64(Array::from(data.data).into_dyn()),
                            proto::array1_d::Data::F64(data) =>
                                NodeEvaluation::F64(Array::from(data.data).into_dyn()),
                            proto::array1_d::Data::String(data) =>
                                NodeEvaluation::Str(Array::from(data.data).into_dyn()),
                        },
                        None => panic!("proto array is empty")
                    }
                },
                None => panic!("proto array is empty")
            }).collect::<Vec<NodeEvaluation>>())),
//        proto::array_nd::Data::Bytes(x) =>
//            NodeEvaluation::Bytes(bytes::Bytes::from(x)),
        _ => Err("unsupported proto value variant encountered".to_string())
    }
}

pub fn serialize_proto_value(evaluation: &NodeEvaluation) -> Result<proto::Value, String> {
    match evaluation {
//        NodeEvaluation::Bytes(x) => proto::Value {
//            datatype: proto::DataType::Bytes as i32,
//            data: Some(proto::value::Data::Bytes(prost::encoding::bytes::encode(x)))
//        },
        NodeEvaluation::Bool(x) => Ok(proto::Value {
            data: Some(proto::value::Data::ArrayNd(proto::ArrayNd {
                flattened: Some(proto::Array1D {
                    data: Some(proto::array1_d::Data::Bool(proto::Array1Dbool {
                        data: x.iter().map(|s| *s).collect(),
                    }))
                }),
                order: (1..x.ndim()).map(|x| { x as u64 }).collect(),
                shape: x.shape().iter().map(|y| { *y as u64 }).collect(),
            }))
        }),
        NodeEvaluation::I64(x) => Ok(proto::Value {
            data: Some(proto::value::Data::ArrayNd(proto::ArrayNd {
                flattened: Some(proto::Array1D {
                    data: Some(proto::array1_d::Data::I64(proto::Array1Di64 {
                        data: x.iter().map(|s| *s).collect()
                    }))
                }),
                order: (1..x.ndim()).map(|x| { x as u64 }).collect(),
                shape: x.shape().iter().map(|y| { *y as u64 }).collect(),
            }))
        }),
        NodeEvaluation::F64(x) => Ok(proto::Value {
            data: Some(proto::value::Data::ArrayNd(proto::ArrayNd {
                flattened: Some(proto::Array1D {
                    data: Some(proto::array1_d::Data::F64(proto::Array1Df64 {
                        data: x.iter().map(|s| *s).collect()
                    }))
                }),
                order: (1..x.ndim()).map(|x| { x as u64 }).collect(),
                shape: x.shape().iter().map(|y| { *y as u64 }).collect(),
            }))
        }),
        NodeEvaluation::Str(x) => Ok(proto::Value {
            data: Some(proto::value::Data::ArrayNd(proto::ArrayNd {
                flattened: Some(proto::Array1D {
                    data: Some(proto::array1_d::Data::String(proto::Array1Dstr {
                        data: x.iter().map(|s| s.to_owned()).collect()
                    }))
                }),
                order: (1..x.ndim()).map(|x| { x as u64 }).collect(),
                shape: x.shape().iter().map(|y| { *y as u64 }).collect(),
            }))
        }),
        NodeEvaluation::HashmapString(x) => {
            let mut evaluation_serialized: HashMap<String, proto::Value> = HashMap::new();
            for (node_id, node_eval) in x {
                if let Ok(array_serialized) = serialize_proto_value(node_eval) {
                    evaluation_serialized.insert(node_id.to_owned(), array_serialized);
                }
            }

            return Ok(proto::Value {
                data: Some(proto::value::Data::HashmapString(proto::HashmapString {
                    data: evaluation_serialized
                }))
            });
        }
        NodeEvaluation::Vec(x) => Ok(proto::Value {
            data: Some(proto::value::Data::JaggedArray2D(proto::JaggedArray2D {
                data: x.iter()
                    .map(|column| proto::jagged_array2_d::OptionalArray1D {
                        data: Some(proto::jagged_array2_d::optional_array1_d::Data::Option(match column {
                            NodeEvaluation::Bool(x) => proto::Array1D {
                                data: Some(proto::array1_d::Data::Bool(proto::Array1Dbool {
                                    data: x.to_owned().into_dimensionality::<Ix1>().unwrap().to_vec()
                                }))
                            },
                            NodeEvaluation::I64(x) => proto::Array1D {
                                data: Some(proto::array1_d::Data::I64(proto::Array1Di64 {
                                    data: x.to_owned().into_dimensionality::<Ix1>().unwrap().to_vec()
                                }))
                            },
                            NodeEvaluation::F64(x) => proto::Array1D {
                                data: Some(proto::array1_d::Data::F64(proto::Array1Df64 {
                                    data: x.to_owned().into_dimensionality::<Ix1>().unwrap().to_vec()
                                }))
                            },
                            NodeEvaluation::Str(x) => proto::Array1D {
                                data: Some(proto::array1_d::Data::String(proto::Array1Dstr {
                                    data: x.to_owned().into_dimensionality::<Ix1>().unwrap().to_vec()
                                }))
                            },
                            _ => panic!("only vectors are implemented for jagged matrices".to_string())
                        }))
                    }).collect()
            }))
        }),
        _ => Err("Unsupported evaluation type. Could not serialize data to protobuf.".to_string())
    }
}
