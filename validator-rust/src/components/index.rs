use crate::errors::*;

use crate::base::{Array, Value, ValueProperties, ArrayProperties, Nature, NatureContinuous, NatureCategorical, Vector1DNull, Jagged, IndexKey, NodeProperties, DataType};

use crate::{proto, base, Warnable};
use crate::components::{Component, Named, Expandable};

use std::ops::Deref;
use ndarray::ArrayD;
use ndarray::prelude::*;
use crate::utilities::{get_common_value, get_literal};
use indexmap::map::IndexMap;
use std::collections::HashMap;
use itertools::Itertools;

impl Component for proto::Index {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: &IndexMap<base::IndexKey, Value>,
        properties: &base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.clone();

        let mut dimensionality = None;

        let properties = match data_property {
            ValueProperties::Indexmap(data_property) => {

                match data_property.variant {
                    proto::indexmap_properties::Variant::Dataframe => {

                        if let Some(column_names) = public_arguments.get::<IndexKey>(&"names".into()) {
                            let column_names = column_names.array()?;
                            dimensionality = Some(column_names.shape().len() as i64 + 1);
                            match column_names.to_owned() {
                                Array::F64(_) => return Err("floats are not valid indexes".into()),
                                Array::I64(names) => to_name_vec(&names)?.into_iter()
                                    .map(|v| data_property.children.get::<IndexKey>(&v.into()).cloned())
                                    .collect::<Option<Vec<ValueProperties>>>(),
                                Array::Str(names) => to_name_vec(&names)?.into_iter()
                                    .map(|v| data_property.children.get::<IndexKey>(&v.into()).cloned())
                                    .collect::<Option<Vec<ValueProperties>>>(),
                                Array::Bool(names) => to_name_vec(&names)?.into_iter()
                                    .map(|v| data_property.children.get::<IndexKey>(&v.into()).cloned())
                                    .collect::<Option<Vec<ValueProperties>>>(),
                            }
                                .ok_or_else(|| Error::from("columns: unknown column in index"))

                        } else if let Some(indices) = public_arguments.get::<IndexKey>(&"indices".into()) {
                            let indices = indices.array()?.i64()?;
                            dimensionality = Some(indices.shape().len() as i64 + 1);
                            to_name_vec(indices)?.into_iter()
                                .map(|idx| data_property.children.get_index(idx as usize).map(|v| v.1.clone()))
                                .collect::<Option<Vec<ValueProperties>>>()
                                .ok_or_else(|| Error::from("index does not exist"))

                        } else if let Some(mask) = public_arguments.get::<IndexKey>(&"mask".into()) {
                            let mask = mask.array()?.bool()?;
                            if mask.shape().len() != 1 {
                                return Err("mask: must be 1-dimensional".into())
                            }
                            dimensionality = Some(2);
                            let mask = to_name_vec(mask)?;
                            if mask.len() != data_property.children.len() {
                                return Err("mask: must be same length as the number of columns")?
                            }
                            Ok(data_property.children.into_iter().zip(mask)
                                .filter(|(_, m)| *m).map(|(v, _)| v.1)
                                .collect::<Vec<ValueProperties>>())
                        } else {
                            return Err("one of names, indices or mask must be supplied".into())
                        }
                    }
                    proto::indexmap_properties::Variant::Partition => {
                        let names = public_arguments.get::<IndexKey>(&"names".into())
                            .ok_or_else(|| Error::from("names: missing"))?.deref().to_owned().array()?.clone();

                        let partition_key = IndexKey::new(names)?;
                        let mut part_properties = data_property.children.get::<IndexKey>(&partition_key)
                            .ok_or_else(|| format!("unknown partition index: {:?}", partition_key))?.clone();

                        fn set_group_index(part_properties: &mut ArrayProperties, key: IndexKey) {
                            let last_idx = part_properties.group_id.len() - 1;
                            part_properties.group_id.get_mut(last_idx)
                                .map(|v| v.index = Some(key));
                        }

                        match &mut part_properties {
                            ValueProperties::Array(part_properties) =>
                                set_group_index(part_properties, partition_key),
                            ValueProperties::Indexmap(part_properties) =>
                                part_properties.children.values_mut()
                                    .map(|mut v| match &mut v {
                                        ValueProperties::Array(v) => Ok(set_group_index(v, partition_key.clone())),
                                        _ => Err("dataframe columns must be arrays".into())
                                    })
                                    .collect::<Result<()>>()?,
                            _ => return Err("data: partition members must be either a dataframe or array".into())
                        }

                        return Ok(Warnable::new(part_properties))
                    }
                }
            },
            ValueProperties::Array(data_property) => {
                if !data_property.releasable {
                    data_property.assert_is_not_aggregated()?;
                }

                if let Some(indices) = public_arguments.get::<IndexKey>(&"indices".into()) {
                    let indices = indices.array()?.i64()?;
                    dimensionality = Some(indices.shape().len() as i64 + 1);

                    to_name_vec(indices)?.into_iter()
                        .map(|idx| select_properties(&data_property, &(idx as usize)))
                        .collect::<Result<Vec<ValueProperties>>>()

                } else if let Some(mask) = public_arguments.get::<IndexKey>(&"mask".into()) {
                    let mask = mask.array()?.bool()?;
                    if mask.shape().len() != 1 {
                        return Err("mask: must be 1-dimensional".into())
                    }
                    dimensionality = Some(2);
                    let mask = to_name_vec(mask)?;
                    if mask.len() != data_property.num_columns()? as usize {
                        return Err("mask: must be same length as the number of columns")?
                    }
                    mask.into_iter().enumerate().filter(|(_, mask)| *mask)
                        .map(|(idx, _)| select_properties(&data_property, &idx))
                        .collect::<Result<Vec<ValueProperties>>>()
                } else {
                    return Err("either indices or mask must be supplied".into())
                }
            },
            ValueProperties::Jagged(_) => Err("indexing is not supported on vectors".into()),
            ValueProperties::Function(_) => Err("indexing is not suppported on functions".into())
        }?;

        stack_properties(&properties, dimensionality).map(Warnable::new)
    }
}

impl Expandable for proto::Index {
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        properties: &NodeProperties,
        component_id: &u32,
        maximum_id: &u32
    ) -> Result<proto::ComponentExpansion> {
        let mut current_id = *maximum_id;
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();
        let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();

        let data_property: ValueProperties = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.clone();

        if let Ok(indexmap) = data_property.indexmap() {
            if indexmap.variant == proto::indexmap_properties::Variant::Partition {
                current_id += 1;
                let id_is_partition = current_id;
                let (patch_node, release) = get_literal(true.into(), &component.submission)?;
                computation_graph.insert(id_is_partition.clone(), patch_node);
                releases.insert(id_is_partition.clone(), release);

                let mut component = component.clone();
                component.insert_argument(&"is_partition".into(), id_is_partition);
                computation_graph.insert(*component_id, component);
            }
        }

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases,
            traversal: Vec::new(),
            warnings: vec![]
        })
    }
}

impl Named for proto::Index {
    fn get_names(
        &self,
        public_arguments: &IndexMap<base::IndexKey, Value>,
        argument_variables: &IndexMap<base::IndexKey, Vec<String>>,
        _release: Option<&Value>
    ) -> Result<Vec<String>> {
        let input_names = argument_variables.get::<IndexKey>(&"data".into()).ok_or("data: missing")?;
        Ok(match public_arguments.get::<IndexKey>(&"columns".into())
            .ok_or_else(|| Error::from("columns: missing"))?.to_owned()
            .array()? {
            Array::Str(names) =>
                names.iter().cloned().collect::<Vec<String>>(),
            Array::I64(indices) => indices.iter()
                .map(|idx| input_names.get(*idx as usize).cloned())
                .collect::<Option<Vec<String>>>()
                .ok_or_else(|| "attempted to retrieve an out-of-bounds name")?,
            Array::Bool(mask) => mask.iter()
                .zip(input_names.iter())
                .filter(|(mask, _)| **mask)
                .map(|(_, name)| name.clone())
                .collect::<Vec<String>>(),
            _ => return Err("column names may not be floats".into())
        })
    }
}

pub fn to_name_vec<T: Clone>(columns: &ArrayD<T>) -> Result<Vec<T>> {
    match columns.ndim() {
        0 => Ok(vec![columns.first()
            .ok_or_else(|| Error::from("At least one column name must be supplied"))?.clone()]),
        1 => match columns.clone().into_dimensionality::<Ix1>() {
            Ok(columns) => Ok(columns.to_vec()),
            Err(_) => Err("column names must be 1-dimensional".into())
        },
        _ => Err("dimensionality of column names must be less than 2".into())
    }
}

pub fn mask_columns(column_names: &[IndexKey], mask: &[bool]) -> Result<Vec<IndexKey>> {
    if mask.len() != column_names.len() {
        return Err("boolean mask must be the same length as the column names".into());
    }
    Ok(column_names.iter().zip(mask)
        .filter(|(_, mask)| **mask)
        .map(|(name, _)| name.to_owned())
        .collect::<Vec<IndexKey>>())
}

fn take<T: Clone>(vector: &[T], index: &usize) -> Result<T> {
    match vector.get(*index) {
        Some(value) => Ok(value.clone()),
        None => Err("property column index is out of bounds".into())
    }
}

fn select_properties(properties: &ArrayProperties, index: &usize) -> Result<ValueProperties> {
    let mut properties = properties.clone();
    properties.c_stability = vec![take(&properties.c_stability, index)?];
    properties.num_columns = Some(1);
    if let Some(nature) = &properties.nature {
        properties.nature = Some(match nature {
            Nature::Continuous(continuous) => Nature::Continuous(NatureContinuous {
                lower: match &continuous.lower {
                    Vector1DNull::F64(lower) => Vector1DNull::F64(vec![take(lower, index)?]),
                    Vector1DNull::I64(lower) => Vector1DNull::I64(vec![take(lower, index)?]),
                    _ => return Err("lower must be numeric".into())
                },
                upper: match &continuous.upper {
                    Vector1DNull::F64(upper) => Vector1DNull::F64(vec![take(upper, index)?]),
                    Vector1DNull::I64(upper) => Vector1DNull::I64(vec![take(upper, index)?]),
                    _ => return Err("upper must be numeric".into())
                },
            }),
            Nature::Categorical(categorical) => Nature::Categorical(NatureCategorical {
                categories: match &categorical.categories {
                    Jagged::F64(cats) => Jagged::F64(vec![take(&cats, index)?]),
                    Jagged::I64(cats) => Jagged::I64(vec![take(&cats, index)?]),
                    Jagged::Bool(cats) => Jagged::Bool(vec![take(&cats, index)?]),
                    Jagged::Str(cats) => Jagged::Str(vec![take(&cats, index)?]),
                }
            })
        })
    }
    Ok(ValueProperties::Array(properties))
}

fn stack_properties(all_properties: &Vec<ValueProperties>, dimensionality: Option<i64>) -> Result<ValueProperties> {
    let all_properties = all_properties.into_iter()
        .map(|property| Ok(property.array()?.clone()))
        .collect::<Result<Vec<ArrayProperties>>>()?;

    let num_records = get_common_value(&all_properties.iter()
        .map(|prop| prop.num_records).collect()).unwrap_or(None);
    let dataset_id = get_common_value(&all_properties.iter()
        .map(|prop| prop.dataset_id).collect()).unwrap_or(None);

    if num_records.is_none() && dataset_id.is_none() {
        return Err("dataset may not be conformable".into())
    }

    if all_properties.iter().any(|prop| prop.aggregator.is_some()) {
        return Err("indexing is not currently supported on aggregated data".into())
    }

    let data_type = get_common_value(&all_properties.iter().map(|prop| prop.data_type.clone()).collect())
        .ok_or_else(|| Error::from("dataset must have homogeneous type"))?;

    let group_id = get_common_value(&all_properties.iter()
        .map(|v| v.group_id.clone()).collect())
        .ok_or_else(|| "group_id: must be homogeneous")?;

    // TODO: preserve nature when indexing
    let natures = all_properties.iter()
        .map(|prop| prop.nature.as_ref())
        .collect::<Vec<Option<&Nature>>>();

    let nature = get_common_continuous_nature(&natures, data_type.to_owned())
        .or_else(|| get_common_categorical_nature(&natures));

    Ok(ValueProperties::Array(ArrayProperties {
        num_records,
        num_columns: all_properties.iter()
            .map(|prop| prop.num_columns)
            .fold(Some(0), |total, num| match (total, num) {
                (Some(total), Some(num)) => Some(total + num),
                _ => None
            }),
        nullity: get_common_value(&all_properties.iter().map(|prop| prop.nullity).collect()).unwrap_or(true),
        releasable: get_common_value(&all_properties.iter().map(|prop| prop.releasable).collect()).unwrap_or(true),
        c_stability: all_properties.iter().flat_map(|prop| prop.c_stability.clone()).collect(),
        aggregator: None,
        nature,
        data_type,
        dataset_id: all_properties[0].dataset_id,
        // this is a library-wide assumption - that datasets have more than zero rows
        is_not_empty: all_properties.iter().all(|prop| prop.is_not_empty),
        dimensionality,
        group_id
    }))
}

fn get_common_continuous_nature(natures: &Vec<Option<&Nature>>, data_type: DataType) -> Option<Nature> {
    let lower: Vector1DNull = natures.into_iter().map(|nature| match nature {
        Some(Nature::Continuous(nature)) => Some(nature.lower.clone()),
        Some(Nature::Categorical(_)) => None,
        _ => Some(match data_type {
            DataType::F64 => Vector1DNull::F64(vec![None]),
            DataType::I64 => Vector1DNull::I64(vec![None]),
            _ => return None
        })
    }).collect::<Option<Vec<Vector1DNull>>>()?.into_iter()
        .map(Ok).fold1(concat_vector1d_null)?.ok()?;

    let upper: Vector1DNull = natures.into_iter().map(|nature| match nature {
        Some(Nature::Continuous(nature)) => Some(nature.upper.clone()),
        Some(Nature::Categorical(_)) => None,
        None => Some(match data_type {
            DataType::F64 => Vector1DNull::F64(vec![None]),
            DataType::I64 => Vector1DNull::I64(vec![None]),
            _ => return None
        })
    }).collect::<Option<Vec<Vector1DNull>>>()?.into_iter()
        .map(Ok).fold1(concat_vector1d_null)?.ok()?;

    Some(Nature::Continuous(NatureContinuous {
        lower, upper
    }))
}

fn get_common_categorical_nature(natures: &Vec<Option<&Nature>>) -> Option<Nature> {
    let categories = natures.into_iter().map(|nature| match nature {
        Some(Nature::Categorical(nature)) => Some(nature.categories.clone()),
        Some(Nature::Continuous(_)) => None,
        None => None
    }).collect::<Option<Vec<Jagged>>>()?.into_iter()
        .map(Ok).fold1(concat_jagged)?.ok()?;

    Some(Nature::Categorical(NatureCategorical {
        categories
    }))
}

fn concat_vector1d_null(a: Result<Vector1DNull>, b: Result<Vector1DNull>) -> Result<Vector1DNull> {
    Ok(match (a?, b?) {
        (Vector1DNull::F64(a), Vector1DNull::F64(b)) =>
            Vector1DNull::F64([&a[..], &b[..]].concat()),
        (Vector1DNull::I64(a), Vector1DNull::I64(b)) =>
            Vector1DNull::I64([&a[..], &b[..]].concat()),
        (Vector1DNull::Bool(a), Vector1DNull::Bool(b)) =>
            Vector1DNull::Bool([&a[..], &b[..]].concat()),
        (Vector1DNull::Str(a), Vector1DNull::Str(b)) =>
            Vector1DNull::Str([&a[..], &b[..]].concat()),
        _ => return Err("attempt to concatenate non-homogenously typed vectors".into())
    })
}

fn concat_jagged(a: Result<Jagged>, b: Result<Jagged>) -> Result<Jagged> {
    Ok(match (a?, b?) {
        (Jagged::F64(a), Jagged::F64(b)) =>
            Jagged::F64([&a[..], &b[..]].concat()),
        (Jagged::I64(a), Jagged::I64(b)) =>
            Jagged::I64([&a[..], &b[..]].concat()),
        (Jagged::Bool(a), Jagged::Bool(b)) =>
            Jagged::Bool([&a[..], &b[..]].concat()),
        (Jagged::Str(a), Jagged::Str(b)) =>
            Jagged::Str([&a[..], &b[..]].concat()),
        _ => return Err("attempt to concatenate non-homogenously typed vectors".into())
    })
}
