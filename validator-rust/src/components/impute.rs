use indexmap::map::IndexMap;

use crate::{base, Warnable};
use crate::base::{Array, DataType, IndexKey, Nature, NatureContinuous, Value, ValueProperties, Vector1DNull, NatureCategorical, Jagged};
use crate::components::{Component, Expandable};
use crate::errors::*;
use crate::proto;
use crate::utilities::{get_argument, get_literal, prepend, standardize_categorical_argument, standardize_null_candidates_argument};
use crate::utilities::inference::infer_property;

impl Component for proto::Impute {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: base::NodeProperties,
        _node_id: u32,
    ) -> Result<Warnable<ValueProperties>> {
        let mut data_property = properties.get::<base::IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        // integers may not be null
        if data_property.data_type == DataType::Int {
            if data_property.nullity {
                return Err("data: integers may not contain nullity".into())
            }
            return Ok(ValueProperties::Array(data_property).into())
        }

        if data_property.data_type == DataType::Unknown {
            return Err("data: data_type must be known".into())
        }

        let num_columns = data_property.num_columns
            .ok_or("data: number of columns missing")?;

        if let Some(&categories) = public_arguments.get::<IndexKey>(&"categories".into()) {
            if data_property.data_type != categories.ref_jagged()?.data_type() {
                return Err("categories and data must be homogeneously typed".into())
            }

            let null_values = get_argument(&public_arguments, "null_values")?.clone().jagged()?;

            if null_values.data_type() != data_property.data_type {
                return Err("null_values and data must be homogeneously typed".into())
            }

            data_property.nature = match data_property.nature {
                Some(Nature::Categorical(NatureCategorical { categories: prior })) => Some(Nature::Categorical(NatureCategorical {
                    categories: match (prior, categories.clone().jagged()?, null_values) {
                        (Jagged::Int(prior), Jagged::Int(categories), Jagged::Int(nulls)) =>
                            standardize_categorical_argument(prior, num_columns)?.into_iter()
                                .zip(standardize_categorical_argument(categories, num_columns)?.into_iter())
                                .zip(standardize_null_candidates_argument(nulls, num_columns)?.into_iter())
                                .map(|((prior, cands), nulls)| prior.into_iter()
                                    .filter(|p| !nulls.contains(p)).chain(cands).collect::<Vec<_>>())
                                .collect::<Vec<_>>().into(),
                        (Jagged::Bool(prior), Jagged::Bool(categories), Jagged::Bool(nulls)) =>
                            standardize_categorical_argument(prior, num_columns)?.into_iter()
                                .zip(standardize_categorical_argument(categories, num_columns)?.into_iter())
                                .zip(standardize_null_candidates_argument(nulls, num_columns)?.into_iter())
                                .map(|((prior, cands), nulls)| prior.into_iter()
                                    .filter(|p| !nulls.contains(p)).chain(cands).collect::<Vec<_>>())
                                .collect::<Vec<_>>().into(),
                        (Jagged::Str(prior), Jagged::Str(categories), Jagged::Str(nulls)) =>
                            standardize_categorical_argument(prior, num_columns)?.into_iter()
                                .zip(standardize_categorical_argument(categories, num_columns)?.into_iter())
                                .zip(standardize_null_candidates_argument(nulls, num_columns)?.into_iter())
                                .map(|((prior, cands), nulls)| prior.into_iter()
                                    .filter(|p| !nulls.contains(p)).chain(cands).collect::<Vec<_>>())
                                .collect::<Vec<_>>().into(),
                        _ => return Err("categories may not be float".into())
                    }
                })),
                _ => None
            };
            return Ok(ValueProperties::Array(data_property).into())
        }

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
                    .lower_float().map_err(prepend("lower:"))?
            }
        };

        // 1. check public arguments (constant n)
        let impute_upper = match public_arguments.get::<IndexKey>(&"upper".into()) {
            Some(max) => max.ref_array()?.clone().vec_float(Some(num_columns))
                .map_err(prepend("upper:"))?,

            // 2. then private arguments (for example from another clamped column)
            None => match properties.get::<IndexKey>(&"upper".into()) {
                Some(min) => min.array()?.upper_float()
                    .map_err(prepend("max:"))?,

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
                .map(|(impute_max, optional_data_max)| match optional_data_max {
                    Some(data_max) => Some(impute_max.max(data_max)),
                    // since there was no prior bound, nothing is known about the max
                    None => None
                }).collect(),
            Err(_) => (0..num_columns).map(|_| None).collect()
        };

        data_property.nullity = false;

        // impute may only ever widen prior existing bounds
        data_property.nature = Some(Nature::Continuous(NatureContinuous {
            lower: Vector1DNull::Float(impute_lower),
            upper: Vector1DNull::Float(impute_upper),
        }));

        Ok(ValueProperties::Array(data_property).into())
    }
}

impl Expandable for proto::Impute {
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        _public_arguments: &IndexMap<IndexKey, &Value>,
        properties: &base::NodeProperties,
        component_id: u32,
        mut maximum_id: u32,
    ) -> Result<base::ComponentExpansion> {
        let mut component = component.clone();

        let mut expansion = base::ComponentExpansion::default();

        if !properties.contains_key::<base::IndexKey>(&"categories".into()) {
            if !properties.contains_key::<IndexKey>(&"lower".into()) {
                maximum_id += 1;
                let id_lower = maximum_id;
                let value = Value::Array(Array::Float(
                    ndarray::Array::from(properties.get::<IndexKey>(&"data".into())
                        .unwrap().to_owned().array()?.lower_float()?).into_dyn()));
                let (patch_node, release) = get_literal(value, component.submission)?;
                expansion.computation_graph.insert(id_lower, patch_node);
                expansion.properties.insert(id_lower, infer_property(&release.value, None)?);
                expansion.releases.insert(id_lower, release);
                component.insert_argument(&"lower".into(), id_lower);
            }

            if !properties.contains_key::<IndexKey>(&"upper".into()) {
                maximum_id += 1;
                let id_upper = maximum_id;
                let value = Value::Array(Array::Float(
                    ndarray::Array::from(properties.get::<IndexKey>(&"data".into())
                        .unwrap().to_owned().array()?.upper_float()?).into_dyn()));
                let (patch_node, release) = get_literal(value, component.submission)?;
                expansion.computation_graph.insert(id_upper, patch_node);
                expansion.properties.insert(id_upper, infer_property(&release.value, None)?);
                expansion.releases.insert(id_upper, release);
                component.insert_argument(&"upper".into(), id_upper);
            }
        }

        expansion.computation_graph.insert(component_id, component);

        Ok(expansion)
    }
}


#[cfg(test)]
pub mod test_impute {
    use crate::base::test_data;

    pub mod utilities {
        use crate::base::Value;
        use crate::bindings::Analysis;
        use crate::components::clamp::test_clamp;

        pub fn analysis_f64_cont(value: Value, lower: Option<Value>, upper: Option<Value>) -> (Analysis, u32) {
            let (mut analysis, clamped) = test_clamp::utilities::analysis_f64_cont(
                value, lower.clone(), upper.clone());

            let lower = analysis.literal()
                .value(lower.unwrap_or_else(|| 0.0.into()))
                .value_public(true).build();

            let upper = analysis.literal()
                .value(upper.unwrap_or_else(|| 10.0.into()))
                .value_public(true).build();

            let imputed = analysis.impute(clamped)
                .lower(lower).upper(upper)
                .build();

            (analysis, imputed)
        }

        pub fn analysis_i64_cont(value: Value, lower: Option<Value>, upper: Option<Value>) -> (Analysis, u32) {
            let (mut analysis, clamped) = test_clamp::utilities::analysis_i64_cont(
                value, lower.clone(), upper.clone());

            let lower = analysis.literal().value(match lower {
                Some(lower) => lower, None => 0.into()
            }).value_public(true).build();
            let upper = analysis.literal().value(match upper {
                Some(upper) => upper, None => 10.into()
            }).value_public(true).build();

            let imputed = analysis.impute(clamped)
                .lower(lower).upper(upper)
                .build();

            (analysis, imputed)
        }

        pub fn analysis_i64_cat(value: Value, categories: Value, null_values: Option<Value>) -> (Analysis, u32) {
            let (mut analysis, clamped) = test_clamp::utilities::analysis_i64_cat(
                value, categories.clone(), None);

            let categories = analysis.literal()
                .value(categories).value_public(true)
                .build();

            let null_values = analysis.literal()
                .value(match null_values {
                    Some(null_values) => null_values,
                    None => (-1).into()
                }).value_public(true)
                .build();

            let imputed = analysis.impute(clamped)
                .categories(categories)
                .null_values(null_values)
                .build();

            (analysis, imputed)
        }

        pub fn analysis_string_cat(value: Value, categories: Option<Value>, null_values: Option<Value>) -> (Analysis, u32) {
            let (mut analysis, clamped) = test_clamp::utilities::analysis_string_cat(
                value, categories.clone(), None);

            let categories = analysis.literal().value(match categories {
                Some(categories) => categories,
                None => Value::Jagged(vec![vec!["a", "b", "c", "d"].into_iter().map(String::from).collect::<Vec<String>>()].into())
            }).value_public(true).build();

            let null_values = analysis.literal().value(match null_values {
                Some(null_values) => null_values,
                None => Value::Jagged(vec![vec!["z".to_string()]].into())
            }).value_public(true).build();

            let imputed = analysis.impute(clamped)
                .categories(categories)
                .null_values(null_values)
                .build();
            (analysis, imputed)
        }

        pub fn analysis_bool_cat(value: Value) -> (Analysis, u32) {
            let (mut analysis, clamped) = test_clamp::utilities::analysis_bool_cat(value);
            let categories = analysis.literal()
                .value(Value::Jagged(vec![vec![false, true]].into()))
                .value_public(true).build();

            let null_values = analysis.literal()
                .value(Value::Jagged(vec![vec![false]].into()))
                .value_public(true).build();

            let imputed = analysis.impute(clamped)
                .categories(categories)
                .null_values(null_values)
                .build();
            (analysis, imputed)
        }
    }

    macro_rules! test_f64 {
        ( $( $variant:ident; $lower:expr; $upper:expr, )*) => {
            $(
                #[test]
                fn $variant() {
                    let (analysis, imputed) = utilities::analysis_f64_cont(
                        test_data::$variant(), $lower, $upper);
                    analysis.properties(imputed).unwrap();
                }
            )*
        }
    }

    test_f64!(
        array1d_f64_0; Some(0.0.into()); Some(10.0.into()),
        array1d_f64_10_uniform; Some(0.0.into()); Some(10.0.into()),
    );

    macro_rules! test_i64 {
        ( $( $variant:ident; $lower:expr; $upper:expr; $categories:expr; $null_values:expr, )*) => {
            $(
                #[test]
                fn $variant() {
                    use crate::base::Value;

                    let (analysis, imputed) = utilities::analysis_i64_cat(
                        test_data::$variant(),
                        $categories, $null_values);
                    analysis.properties(imputed).unwrap();

                    let (analysis, imputed) = utilities::analysis_i64_cont(
                        test_data::$variant(), $lower, $upper);
                    analysis.properties(imputed).unwrap();
                }
            )*
        }
    }

    test_i64!(
        array1d_i64_0; None; None; Value::Jagged(vec![vec![1]].into()); None,
        array1d_i64_10_uniform; Some(0.into()); Some(10.into()); Value::Jagged(vec![(0..10).collect::<Vec<i64>>()].into()); Some((-1).into()),
    );

    macro_rules! test_string {
        ( $( $variant:ident; $categories:expr; $null_values:expr, )*) => {
            $(
                #[test]
                fn $variant() {
                    let (analysis, imputed) = utilities::analysis_string_cat(
                        test_data::$variant(),
                        $categories, $null_values);
                    analysis.properties(imputed).unwrap();
                }
            )*
        }
    }

    test_string!(
        array1d_string_0; None; None,
        array1d_string_10_uniform; None; None,
    );

    macro_rules! test_bool {
        ( $( $variant:ident, )*) => {
            $(
                #[test]
                fn $variant() {
                    let (analysis, imputed) = utilities::analysis_bool_cat(
                        test_data::$variant());
                    analysis.properties(imputed).unwrap();
                }
            )*
        }
    }

    test_bool!(
        array1d_bool_0,
        array1d_bool_10_uniform,
    );
}
