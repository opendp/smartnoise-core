use indexmap::map::IndexMap;

use crate::{base, Float, Warnable};
use crate::base::{DataType, IndexKey, Jagged, Nature, NatureCategorical, NatureContinuous, Value, ValueProperties, Vector1DNull, ArrayProperties};
use crate::components::{Component, Expandable};
use crate::errors::*;
use crate::proto;
use crate::utilities::{get_literal, prepend, standardize_categorical_argument};
use crate::utilities::inference::infer_property;

impl Component for proto::Resize {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32,
    ) -> Result<Warnable<ValueProperties>> {

        let mut data_property: ArrayProperties = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        if properties.contains_key::<IndexKey>(&"number_rows".into())
            && properties.contains_key::<IndexKey>(&"minimum_rows".into()) {
            return Err("only one of number_rows and minimum_rows may be set".into())
        }

        if let Some(num_columns) = public_arguments.get::<IndexKey>(&"number_columns".into()) {
            if data_property.num_columns.is_some() {
                return Err("cannot resize number of columns when number of columns is known".into())
            }

            let num_columns = num_columns.ref_array()?.first_int()
                .map_err(prepend("number_columns:"))? as i64;
            if num_columns < 1 {
                return Err("number_columns must be greater than zero".into());
            }
            data_property.num_columns = Some(num_columns);
            data_property.nature = None;
            data_property.dimensionality = Some(2);
        }

        if let Some(num_records) = public_arguments.get::<IndexKey>(&"number_rows".into()) {
            let num_records = num_records.ref_array()?.first_int()
                .map_err(prepend("number_rows:"))?;
            if num_records < 1 {
                return Err("number_rows: must be greater than zero".into());
            }

            data_property.num_records = Some(num_records as i64);
            data_property.is_not_empty = num_records > 0;
        }

        if let Some(minimum_rows) = public_arguments.get::<IndexKey>(&"minimum_rows".into()) {
            if minimum_rows.ref_array()?.first_int()? > 0 {
                data_property.is_not_empty = true;
            } else {
                return Err("minimum_rows must be greater than zero".into())
            }
        }

        let num_columns = data_property.num_columns()?;

        if let Some(&categories) = public_arguments.get::<IndexKey>(&"categories".into()) {
            if data_property.data_type != categories.ref_jagged()?.data_type() {
                return Err("data's atomic type must match categories' atomic type".into());
            }
            data_property.nature = match data_property.nature {
                Some(Nature::Categorical(NatureCategorical { categories: prior })) => Some(Nature::Categorical(NatureCategorical {
                    categories: match (prior, categories.clone().jagged()?) {
                        (Jagged::Int(prior), Jagged::Int(categories)) =>
                            standardize_categorical_argument(prior, num_columns)?.into_iter()
                                .zip(standardize_categorical_argument(categories, num_columns)?.into_iter())
                                .map(|(l, r)| [l, r].concat())
                                .collect::<Vec<_>>().into(),
                        (Jagged::Bool(prior), Jagged::Bool(categories)) =>
                            standardize_categorical_argument(prior, num_columns)?.into_iter()
                                .zip(standardize_categorical_argument(categories, num_columns)?.into_iter())
                                .map(|(l, r)| [l, r].concat())
                                .collect::<Vec<_>>().into(),
                        (Jagged::Str(prior), Jagged::Str(categories)) =>
                            Jagged::Str(standardize_categorical_argument(prior, num_columns)?.into_iter()
                                .zip(standardize_categorical_argument(categories, num_columns)?.into_iter())
                                .map(|(l, r)| [l, r].concat())
                                .collect::<Vec<_>>()),
                        _ => return Err("categories may not be float".into())
                    }.deduplicate()?
                })),
                _ => None
            };
            return Ok(ValueProperties::Array(data_property).into())
        }

        match data_property.data_type {
            DataType::Float => {

                // 1. check public arguments (constant n)
                let impute_lower = match public_arguments.get::<IndexKey>(&"lower".into()) {
                    Some(lower) => lower.ref_array()?.clone().vec_float(Some(num_columns))
                        .map_err(prepend("lower:"))?,

                    // 2. then private arguments (for example from another clamped column)
                    None => match properties.get::<IndexKey>(&"lower".into()) {
                        Some(lower) => lower.array()?.lower_float()
                            .map_err(prepend("lower:"))?,

                        // 3. then data properties (propagated from prior clamping/min/max)
                        None => data_property
                            .lower_float().map_err(prepend("min:"))?
                    }
                };

                // 1. check public arguments (constant n)
                let impute_upper = match public_arguments.get::<IndexKey>(&"upper".into()) {
                    Some(upper) => upper.ref_array()?.clone().vec_float(Some(num_columns))
                        .map_err(prepend("upper:"))?,

                    // 2. then private arguments (for example from another clamped column)
                    None => match properties.get::<IndexKey>(&"upper".into()) {
                        Some(upper) => upper.array()?.upper_float()
                            .map_err(prepend("upper:"))?,

                        // 3. then data properties (propagated from prior clamping/min/max)
                        None => data_property
                            .upper_float().map_err(prepend("upper:"))?
                    }
                };

                if !impute_lower.iter().zip(impute_upper.clone()).all(|(low, high)| *low < high) {
                    return Err("lower is greater than upper".into());
                }

                // the actual data bound (if it exists) may be wider than the imputation parameters
                let impute_lower = match data_property.lower_float_option() {
                    Ok(data_lower) => impute_lower.iter().zip(data_lower)
                        .map(|(impute_lower, optional_data_lower)| match optional_data_lower {
                            Some(data_lower) => Some(impute_lower.min(data_lower)),
                            // since there was no prior bound, nothing is known about the min
                            None => None
                        }).collect(),
                    Err(_) => (0..num_columns).map(|_| None).collect()
                };

                let impute_upper = match data_property.upper_float_option() {
                    Ok(data_upper) => impute_upper.iter().zip(data_upper)
                        .map(|(impute_upper, optional_data_upper)| match optional_data_upper {
                            Some(data_upper) => Some(impute_upper.max(data_upper)),
                            // since there was no prior bound, nothing is known about the max
                            None => None
                        }).collect(),
                    Err(_) => (0..num_columns).map(|_| None).collect()
                };

                // impute may only ever widen prior existing bounds
                data_property.nature = Some(Nature::Continuous(NatureContinuous {
                    lower: Vector1DNull::Float(impute_lower),
                    upper: Vector1DNull::Float(impute_upper),
                }));
            }

            DataType::Int => {

                // 1. check public arguments (constant n)
                let impute_lower = match public_arguments.get::<IndexKey>(&"lower".into()) {
                    Some(lower) => lower.ref_array()?.clone().vec_int(Some(num_columns))
                        .map_err(prepend("lower:"))?,

                    // 2. then private arguments (for example from another clamped column)
                    None => match properties.get::<IndexKey>(&"lower".into()) {
                        Some(lower) => lower.array()?.lower_int()
                            .map_err(prepend("lower:"))?,

                        // 3. then data properties (propagated from prior clamping/lower/upper)
                        None => data_property
                            .lower_int().map_err(prepend("lower:"))?
                    }
                };

                // 1. check public arguments (constant n)
                let impute_upper = match public_arguments.get::<IndexKey>(&"upper".into()) {
                    Some(upper) => upper.ref_array()?.clone().vec_int(Some(num_columns))
                        .map_err(prepend("upper:"))?,

                    // 2. then private arguments (for example from another clamped column)
                    None => match properties.get::<IndexKey>(&"upper".into()) {
                        Some(upper) => upper.array()?.upper_int()
                            .map_err(prepend("upper:"))?,

                        // 3. then data properties (propagated from prior clamping/lower/upper)
                        None => data_property
                            .upper_int().map_err(prepend("upper:"))?
                    }
                };

                if !impute_lower.iter().zip(impute_upper.clone()).all(|(low, high)| *low < high) {
                    return Err("lower is greater than upper".into());
                }

                // the actual data bound (if it exists) may be wider than the imputation parameters
                let impute_lower = match data_property.lower_int_option() {
                    Ok(data_lower) => impute_lower.into_iter().zip(data_lower.into_iter())
                        .map(|(impute_lower, optional_data_lower)| match optional_data_lower {
                            Some(data_lower) => Some(impute_lower.min(data_lower)),
                            // since there was no prior bound, nothing is known about the min
                            None => None
                        }).collect(),
                    Err(_) => (0..num_columns).map(|_| None).collect()
                };

                let impute_upper = match data_property.upper_int_option() {
                    Ok(data_upper) => impute_upper.into_iter().zip(data_upper.into_iter())
                        .map(|(impute_upper, optional_data_upper)| match optional_data_upper {
                            Some(data_upper) => Some(impute_upper.max(data_upper)),
                            // since there was no prior bound, nothing is known about the max
                            None => None
                        }).collect(),
                    Err(_) => (0..num_columns).map(|_| None).collect()
                };

                // impute may only ever widen prior existing bounds
                data_property.nature = Some(Nature::Continuous(NatureContinuous {
                    lower: Vector1DNull::Int(impute_lower),
                    upper: Vector1DNull::Int(impute_upper),
                }));
            }
            _ => return Err("bounds for imputation must be numeric".into())
        }

        let sample_proportion: Option<Float> = public_arguments.get(&IndexKey::from("sample_proportion"))
            .and_then(|v| v.ref_array().ok()?.first_float().ok());
        if let Some(sample_proportion) = sample_proportion {
            if sample_proportion <= 0. {
                return Err("sample_proportion must be positive".into())
            }
        }
        data_property.c_stability = data_property.c_stability * sample_proportion.unwrap_or(1.).ceil() as u32;
        data_property.sample_proportion = match (data_property.sample_proportion, sample_proportion) {
            (Some(_), Some(_)) => return Err(Error::from("multiple samplings is not currently supported")),
            (Some(prior_prop), None) => Some(prior_prop),
            (None, Some(new_prop)) => Some(new_prop / new_prop.ceil()),
            (None, None) => None
        };

        if data_property.sample_proportion.is_some() {
            data_property.naturally_ordered = false;
        }

        Ok(ValueProperties::Array(data_property).into())
    }
}

impl Expandable for proto::Resize {
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        _public_arguments: &IndexMap<IndexKey, &Value>,
        properties: &base::NodeProperties,
        component_id: u32,
        mut maximum_id: u32,
    ) -> Result<base::ComponentExpansion> {

        let mut expansion = base::ComponentExpansion::default();

        if properties.contains_key::<IndexKey>(&"categories".into()) {
            return Ok(expansion)
        }

        let has_lower = properties.contains_key::<IndexKey>(&"lower".into());
        let has_upper = properties.contains_key::<IndexKey>(&"upper".into());

        let data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let mut component = component.clone();

        // numeric resizing
        if has_lower || has_upper || matches!(data_property.nature, Some(Nature::Continuous(_))) {
            if !has_lower {
                maximum_id += 1;
                let id_lower = maximum_id;
                let (patch_node, release) = get_literal(
                    Value::Array(data_property.lower()?), component.submission)?;
                expansion.computation_graph.insert(id_lower, patch_node);
                expansion.properties.insert(id_lower, infer_property(&release.value, None, id_lower)?);
                expansion.releases.insert(id_lower, release);
                component.insert_argument(&"lower".into(), id_lower);
            }
            if !has_upper {
                maximum_id += 1;
                let id_upper = maximum_id;
                let (patch_node, release) = get_literal(
                    Value::Array(data_property.upper()?), component.submission)?;
                expansion.computation_graph.insert(id_upper, patch_node);
                expansion.properties.insert(id_upper, infer_property(&release.value, None, id_upper)?);
                expansion.releases.insert(id_upper, release);
                component.insert_argument(&"upper".into(), id_upper);
            }
        }
        // categorical resizing
        else if matches!(data_property.nature, Some(Nature::Categorical(_))) {
            maximum_id += 1;
            let id_categories = maximum_id;
            let (patch_node, release) = get_literal(
                Value::Jagged(data_property.categories()?), component.submission)?;
            expansion.computation_graph.insert(id_categories, patch_node);
            expansion.properties.insert(id_categories, infer_property(&release.value, None, id_categories)?);
            expansion.releases.insert(id_categories, release);
            component.insert_argument(&"categories".into(), id_categories);
        }
        // unknown clamping procedure
        else {
            return Err("lower/upper/categorical arguments must be provided, or lower/upper/categorical properties must be known on data".into())
        }

        expansion.computation_graph.insert(component_id, component);

        Ok(expansion)
    }
}



#[cfg(test)]
pub mod test_resize {
    use crate::base::test_data;

    pub mod utilities {
        use crate::base::Value;
        use crate::bindings::Analysis;
        use crate::components::impute::test_impute;

        pub fn analysis_f64_cont(value: Value, number_rows: Value, lower: Option<Value>, upper: Option<Value>) -> (Analysis, u32) {

            let (mut analysis, imputed) = test_impute::utilities::analysis_f64_cont(
                value,  None, None);

            let lower = analysis.literal().value(match lower {
                Some(lower) => lower, None => 0.0.into()
            }).value_public(true).build();
            let upper = analysis.literal().value(match upper {
                Some(upper) => upper, None => 10.0.into()
            }).value_public(true).build();
            let number_rows = analysis.literal()
                .value(number_rows).value_public(true)
                .build();

            let resized = analysis.resize(imputed)
                .number_rows(number_rows).upper(upper).lower(lower)
                .build();

            (analysis, resized)
        }

        pub fn analysis_i64_cont(value: Value, number_rows: Value, lower: Option<Value>, upper: Option<Value>) -> (Analysis, u32) {
            let (mut analysis, imputed) = test_impute::utilities::analysis_i64_cont(
                value, None, None);

            let lower = analysis.literal().value(match lower {
                Some(lower) => lower, None => 0.into()
            }).value_public(true).build();
            let upper = analysis.literal().value(match upper {
                Some(upper) => upper, None => 10.into()
            }).value_public(true).build();
            let number_rows = analysis.literal()
                .value(number_rows).value_public(true)
                .build();

            let resized = analysis.resize(imputed)
                .number_rows(number_rows).upper(upper).lower(lower)
                .build();

            (analysis, resized)
        }

        pub fn analysis_i64_cat(value: Value, number_rows: Value, categories: Value) -> (Analysis, u32) {
            let (mut analysis, imputed) = test_impute::utilities::analysis_i64_cat(
                value, categories.clone(), None);

            let categories = analysis.literal()
                .value(categories).value_public(true)
                .build();
            let number_rows = analysis.literal()
                .value(number_rows).value_public(true)
                .build();

            let resized = analysis.resize(imputed)
                .number_rows(number_rows)
                .categories(categories)
                .build();

            (analysis, resized)
        }

        pub fn analysis_string_cat(value: Value, number_rows: Value, categories: Option<Value>) -> (Analysis, u32) {
            let (mut analysis, imputed) = test_impute::utilities::analysis_string_cat(
                value, None, None);

            let categories = analysis.literal().value(match categories {
                Some(categories) => categories,
                None => Value::Jagged(vec![vec!["a", "b", "c", "d"].into_iter().map(String::from).collect::<Vec<String>>()].into())
            }).value_public(true).build();
            let number_rows = analysis.literal()
                .value(number_rows).value_public(true)
                .build();

            let resized = analysis.resize(imputed)
                .categories(categories)
                .number_rows(number_rows)
                .build();
            (analysis, resized)
        }

        pub fn analysis_bool_cat(value: Value, number_rows: Value) -> (Analysis, u32) {
            let (mut analysis, imputed) = test_impute::utilities::analysis_bool_cat(value);
            let categories = analysis.literal()
                .value(Value::Jagged(vec![vec![false, true]].into()))
                .value_public(true).build();
            let number_rows = analysis.literal()
                .value(number_rows).value_public(true)
                .build();

            let resized = analysis.resize(imputed)
                .categories(categories)
                .number_rows(number_rows)
                .build();
            (analysis, resized)
        }
    }

    macro_rules! test_f64 {
        ( $( $variant:ident; $number_rows:expr; $lower:expr; $upper:expr, )*) => {
            $(
                #[test]
                fn $variant() {
                    let (analysis, resized) = utilities::analysis_f64_cont(
                        test_data::$variant(), $number_rows, $lower, $upper);
                    analysis.properties(resized).unwrap();
                }
            )*
        }
    }

    test_f64!(
        array1d_f64_0; 10.into(); None; None,
        array1d_f64_10_uniform; 10.into(); None; None,
    );

    macro_rules! test_i64 {
        ( $( $variant:ident; $number_rows:expr; $lower:expr; $upper:expr; $categories:expr; $null_values:expr, )*) => {
            $(
                #[test]
                fn $variant() {
                    use crate::base::Value;

                    let (analysis, resized) = utilities::analysis_i64_cat(
                        test_data::$variant(),
                        $number_rows, $categories);
                    analysis.properties(resized).unwrap();

                    let (analysis, resized) = utilities::analysis_i64_cont(
                        test_data::$variant(), $number_rows, $lower, $upper);
                    analysis.properties(resized).unwrap();
                }
            )*
        }
    }

    test_i64!(
        array1d_i64_0; 10.into(); None; None; Value::Jagged(vec![vec![1]].into()); None,
        array1d_i64_10_uniform; 10.into(); Some(0.into()); Some(10.into()); Value::Jagged(vec![(0..10).collect::<Vec<i64>>()].into()); Some((-1).into()),
    );

    macro_rules! test_string {
        ( $( $variant:ident; $number_rows:expr; $categories:expr; $null_values:expr, )*) => {
            $(
                #[test]
                fn $variant() {
                    let (analysis, resized) = utilities::analysis_string_cat(
                        test_data::$variant(),
                        $number_rows, $categories);
                    analysis.properties(resized).unwrap();
                }
            )*
        }
    }

    test_string!(
        array1d_string_0; 10.into(); None; None,
        array1d_string_10_uniform; 10.into(); None; None,
    );

    macro_rules! test_bool {
        ( $( $variant:ident; $number_rows:expr, )*) => {
            $(
                #[test]
                fn $variant() {
                    let (analysis, resized) = utilities::analysis_bool_cat(
                        test_data::$variant(), $number_rows);
                    analysis.properties(resized).unwrap();
                }
            )*
        }
    }

    test_bool!(
        array1d_bool_0; 10.into(),
        array1d_bool_10_uniform; 10.into(),
    );
}
