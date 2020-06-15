use crate::errors::*;

use crate::base::{ArrayProperties, ValueProperties, NatureContinuous, Nature, Vector1DNull, Jagged, NatureCategorical, DataType};
use crate::utilities::get_common_value;
use itertools::Itertools;

fn take<T: Clone>(vector: &[T], index: usize) -> Result<T> {
    match vector.get(index) {
        Some(value) => Ok(value.clone()),
        None => Err("property column index is out of bounds".into())
    }
}

pub fn select_properties(properties: &ArrayProperties, index: usize) -> Result<ValueProperties> {
    let mut properties = properties.clone();
    properties.c_stability = vec![take(&properties.c_stability, index)?];
    properties.num_columns = Some(1);
    properties.dimensionality = Some(1);
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

pub fn stack_properties(all_properties: &[ValueProperties], dimensionality: Option<i64>) -> Result<ValueProperties> {
    let all_properties = all_properties.iter()
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

fn get_common_continuous_nature(natures: &[Option<&Nature>], data_type: DataType) -> Option<Nature> {
    let lower: Vector1DNull = natures.iter().map(|nature| match nature {
        Some(Nature::Continuous(nature)) => Some(nature.lower.clone()),
        Some(Nature::Categorical(_)) => None,
        _ => Some(match data_type {
            DataType::F64 => Vector1DNull::F64(vec![None]),
            DataType::I64 => Vector1DNull::I64(vec![None]),
            _ => return None
        })
    }).collect::<Option<Vec<Vector1DNull>>>()?.into_iter()
        .map(Ok).fold1(concat_vector1d_null)?.ok()?;

    let upper: Vector1DNull = natures.iter().map(|nature| match nature {
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

fn get_common_categorical_nature(natures: &[Option<&Nature>]) -> Option<Nature> {
    let categories = natures.iter().map(|nature| match nature {
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
