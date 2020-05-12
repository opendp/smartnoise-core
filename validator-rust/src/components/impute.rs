use crate::errors::*;


use std::collections::HashMap;

use crate::{base};
use crate::proto;
use crate::components::{Component, Expandable};

use ndarray;
use crate::base::{Vector1DNull, Nature, NatureContinuous, Value, Array, ValueProperties, DataType};
use crate::utilities::{prepend, get_literal};


impl Component for proto::Impute {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
        _node_id: u32
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        // integers may not be null
        if data_property.data_type == DataType::I64 {
            if data_property.nullity {
                return Err("impossible state: integers contain nullity".into())
            }
            return Ok(data_property.into())
        }

        if data_property.data_type == DataType::Unknown {
            return Err("data_type must be known".into())
        }

        if let Some(categories) = public_arguments.get("categories") {
            if data_property.data_type != categories.jagged()?.data_type() {
                return Err("categories and data must be homogeneously typed".into())
            }

            let null_values = public_arguments.get("null_values")
                .ok_or_else(|| Error::from("null_values: missing, must be public"))?.jagged()?;

            if null_values.data_type() != data_property.data_type {
                return Err("null_values and data must be homogeneously typed".into())
            }

            // TODO: propagation of categories through imputation and resize
            data_property.nature = None;
            return Ok(data_property.into());
        }

        let num_columns = data_property.num_columns
            .ok_or("data: number of columns missing")?;
        // 1. check public arguments (constant n)
        let impute_lower = match public_arguments.get("lower") {
            Some(min) => min.array()?.clone().vec_f64(Some(num_columns))
                .map_err(prepend("lower:"))?,

            // 2. then private arguments (for example from another clamped column)
            None => match properties.get("lower") {
                Some(min) => min.array()?.lower_f64()
                    .map_err(prepend("lower:"))?,

                // 3. then data properties (propagated from prior clamping/min/max)
                None => data_property
                    .lower_f64().map_err(prepend("lower:"))?
            }
        };

        // 1. check public arguments (constant n)
        let impute_upper = match public_arguments.get("upper") {
            Some(max) => max.array()?.clone().vec_f64(Some(num_columns))
                .map_err(prepend("upper:"))?,

            // 2. then private arguments (for example from another clamped column)
            None => match properties.get("upper") {
                Some(min) => min.array()?.upper_f64()
                    .map_err(prepend("max:"))?,

                // 3. then data properties (propagated from prior clamping/min/max)
                None => data_property
                    .upper_f64().map_err(prepend("upper:"))?
            }
        };

        if !impute_lower.iter().zip(impute_upper.clone()).all(|(low, high)| *low < high) {
            return Err("lower is greater than upper".into());
        }

        // the actual data bound (if it exists) may be wider than the imputation parameters
        let impute_lower = match data_property.lower_f64_option() {
            Ok(data_lower) => impute_lower.iter().zip(data_lower)
                .map(|(impute_lower, optional_data_lower)| match optional_data_lower {
                    Some(data_lower) => Some(impute_lower.min(data_lower)),
                    // since there was no prior bound, nothing is known about the min
                    None => None
                }).collect(),
            Err(_) => (0..num_columns).map(|_| None).collect()
        };

        let impute_upper = match data_property.upper_f64_option() {
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
            lower: Vector1DNull::F64(impute_lower),
            upper: Vector1DNull::F64(impute_upper),
        }));

        Ok(data_property.into())
    }
}

impl Expandable for proto::Impute {
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        properties: &base::NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut current_id = *maximum_id;
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();
        let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();

        let mut component = component.clone();

        if !properties.contains_key("categories") {
            if !properties.contains_key("lower") {
                current_id += 1;
                let id_lower = current_id;
                let value = Value::Array(Array::F64(
                    ndarray::Array::from(properties.get("data").unwrap().to_owned().array()?.lower_f64()?).into_dyn()));
                let (patch_node, release) = get_literal(value, &component.submission)?;
                computation_graph.insert(id_lower.clone(), patch_node);
                releases.insert(id_lower.clone(), release);
                component.arguments.insert("lower".to_string(), id_lower);
            }

            if !properties.contains_key("upper") {
                current_id += 1;
                let id_upper = current_id;
                let value = Value::Array(Array::F64(
                    ndarray::Array::from(properties.get("data").unwrap().to_owned().array()?.upper_f64()?).into_dyn()));
                let (patch_node, release) = get_literal(value, &component.submission)?;
                computation_graph.insert(id_upper.clone(), patch_node);
                releases.insert(id_upper.clone(), release);
                component.arguments.insert("upper".to_string(), id_upper);
            }
        }

        computation_graph.insert(component_id.clone(), component);

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases,
            traversal: Vec::new()
        })
    }
}


#[cfg(test)]
pub mod test_impute {
    use crate::base::test_data;

    pub mod utilities {
        use crate::components::clamp::test_clamp;
        use crate::bindings::Analysis;
        use crate::base::Value;

        pub fn analysis_f64_cont(value: Value, lower: Option<Value>, upper: Option<Value>) -> (Analysis, u32) {
            let (mut analysis, clamped) = test_clamp::utilities::analysis_f64_cont(
                value, lower.clone(), upper.clone());

            let lower = analysis.literal().value(match lower {
                Some(lower) => lower, None => 0.0.into()
            }).value_public(true).build();
            let upper = analysis.literal().value(match upper {
                Some(upper) => upper, None => 10.0.into()
            }).value_public(true).build();

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
