use std::collections::HashMap;
use crate::hashmap;
use crate::proto;
use crate::utilities;

use crate::components;
use crate::components::Component;


#[derive(Clone, Debug)]
pub struct Constraint {
    pub nullity: Vec<bool>,
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
pub struct NatureContinuous {
    pub categories: Option<ConstraintVector>
}

#[derive(Clone, Debug)]
pub struct NatureCategorical {
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
        proto::Constraint {}
    }
    pub fn from_proto(other: &proto::Constraint) -> Constraint {
        Constraint {
            nullity: vec![true],
            nature: None,
            releasable: false,
            num_records: None,
        }
    }
}

pub type GraphConstraint = HashMap<u32, Constraint>;
pub type NodeConstraints = HashMap<String, Constraint>;

pub fn get_constraints(component: &proto::Component, graph_constraints: &GraphConstraint) -> NodeConstraints {
    let mut constraints = NodeConstraints::new();
    component.arguments.iter().for_each(|(field_id, field)| {
        let constraint: Constraint = graph_constraints.get(&field).unwrap().clone();
        constraints.insert(field_id.to_owned(), constraint);
    });
    constraints
}

pub fn propagate_constraints(
    analysis: &proto::Analysis,
    release: &proto::Release,
) -> Result<GraphConstraint, String> {
    let mut graph: HashMap<u32, proto::Component> = analysis.graph.to_owned();
    let traversal: Vec<u32> = utilities::graph::get_traversal(analysis)?;

    let mut graph_constraint = GraphConstraint::new();
    traversal.iter().for_each(|node_id| {
        let component: proto::Component = graph.get(node_id).unwrap().to_owned();
        let input_constraints = get_constraints(&component, &graph_constraint);
        let constraint = component.value.unwrap().propagate_constraint(&input_constraints);
        graph_constraint.insert(node_id.clone(), constraint);
    });
    Ok(graph_constraint)
}