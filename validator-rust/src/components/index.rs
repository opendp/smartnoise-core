use crate::errors::*;

use crate::base::{Array, Value, ValueProperties, ArrayProperties, Nature, NatureContinuous, NatureCategorical, Vector1DNull, Jagged, IndexKey};

use crate::{proto, base, Warnable};
use crate::components::{Component, Named};

use std::ops::Deref;
use ndarray::ArrayD;
use ndarray::prelude::*;
use crate::utilities::get_common_value;
use indexmap::map::IndexMap;

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
                                    .map(|v| data_property.properties.get::<IndexKey>(&v.into()).cloned())
                                    .collect::<Option<Vec<ValueProperties>>>(),
                                Array::Str(names) => to_name_vec(&names)?.into_iter()
                                    .map(|v| data_property.properties.get::<IndexKey>(&v.into()).cloned())
                                    .collect::<Option<Vec<ValueProperties>>>(),
                                Array::Bool(names) => to_name_vec(&names)?.into_iter()
                                    .map(|v| data_property.properties.get::<IndexKey>(&v.into()).cloned())
                                    .collect::<Option<Vec<ValueProperties>>>(),
                            }
                                .ok_or_else(|| Error::from("columns: unknown column in index"))

                        } else if let Some(indices) = public_arguments.get::<IndexKey>(&"indices".into()) {
                            dimensionality = Some(2);
                            to_name_vec(indices.to_owned().array()?.i64()?)?.into_iter()
                                .map(|idx| data_property.properties.get_index(idx as usize).map(|v| v.1.clone()))
                                .collect::<Option<Vec<ValueProperties>>>()
                                .ok_or_else(|| Error::from("index does not exist"))

                        } else if let Some(mask) = public_arguments.get::<IndexKey>(&"mask".into()) {
                            dimensionality = Some(2);
                            let mask = to_name_vec(mask.to_owned().array()?.bool()?)?;
                            if mask.len() != data_property.properties.len() {
                                return Err("mask: must be same length as the number of columns")?
                            }
                            Ok(data_property.properties.into_iter().zip(mask)
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
                        let mut part_properties = data_property.properties.get::<IndexKey>(&partition_key)
                            .ok_or_else(|| "unknown partition index")?.array()?.clone();

                        let last_idx = part_properties.group_id.len() - 1;
                        part_properties.group_id.get_mut(last_idx)
                            .map(|v| v.index = Some(partition_key));
                        return Ok(Warnable::new(ValueProperties::Array(part_properties)))
                    }
                }
            },
            ValueProperties::Array(data_property) => {
                if !data_property.releasable {
                    data_property.assert_is_not_aggregated()?;
                }
                dimensionality = Some(2);

                if let Some(indices) = public_arguments.get::<IndexKey>(&"indices".into()) {
                    to_name_vec(indices.to_owned().array()?.i64()?)?.into_iter()
                        .map(|idx| select_properties(&data_property, &(idx as usize)))
                        .collect::<Result<Vec<ValueProperties>>>()

                } else if let Some(mask) = public_arguments.get::<IndexKey>(&"mask".into()) {
                    let mask = to_name_vec(mask.to_owned().array()?.bool()?)?;
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

    let group_id = get_common_value(&all_properties.iter()
        .map(|v| v.group_id.clone()).collect())
        .ok_or_else(|| "group_id: must be homogeneous")?;

    // TODO: preserve nature when indexing

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
        nature: None,
        data_type: get_common_value(&all_properties.iter().map(|prop| prop.data_type.clone()).collect())
            .ok_or_else(|| Error::from("dataset must have homogeneous type"))?,
        dataset_id: all_properties[0].dataset_id,
        // this is a library-wide assumption - that datasets have more than zero rows
        is_not_empty: true,
        dimensionality,
        group_id
    }))
}