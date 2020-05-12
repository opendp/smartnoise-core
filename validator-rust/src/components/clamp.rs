use crate::errors::*;

use std::collections::HashMap;
use crate::base::{Nature, Vector1DNull, Array, ValueProperties, NatureCategorical, Jagged, DataType};

use crate::{proto, base};
use crate::utilities::{prepend, get_literal, standardize_null_target_argument};
use crate::components::{Component, Expandable};

use ndarray;
use crate::base::{Value, NatureContinuous};


impl Component for proto::Clamp {
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

        let num_columns = data_property.num_columns
            .ok_or("data: number of data columns missing")?;

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        // handle categorical clamping
        if let Some(categories) = public_arguments.get("categories") {
            let null = public_arguments.get("null_value")
                .ok_or_else(|| Error::from("null value must be defined when clamping by categories"))?
                .array()?;

            let mut categories = categories.jagged()?.clone();
            match (&mut categories, null) {
                (Jagged::F64(jagged), Array::F64(null)) => {
                    let null_target = standardize_null_target_argument(&null, &num_columns)?;
                    jagged.iter_mut().zip(null_target.into_iter())
                        .for_each(|(cats, null)| cats.push(null))
                },
                (Jagged::I64(jagged), Array::I64(null)) => {
                    let null_target = standardize_null_target_argument(&null, &num_columns)?;
                    jagged.iter_mut().zip(null_target.into_iter())
                        .for_each(|(cats, null)| cats.push(null))
                },
                (Jagged::Str(jagged), Array::Str(null)) => {
                    let null_target = standardize_null_target_argument(&null, &num_columns)?;
                    jagged.iter_mut().zip(null_target.into_iter())
                        .for_each(|(cats, null)| cats.push(null))
                },
                (Jagged::Bool(jagged), Array::Bool(null)) => {
                    let null_target = standardize_null_target_argument(&null, &num_columns)?;
                    jagged.iter_mut().zip(null_target.into_iter())
                        .for_each(|(cats, null)| cats.push(null))
                },
                _ => return Err("categories and null_value must be homogeneously typed".into())
            };
            categories = categories.standardize(&num_columns)?;
            data_property.nature = Some(Nature::Categorical(NatureCategorical { categories }));

            return Ok(data_property.into());
        }

        // else handle numerical clamping
        match data_property.data_type {
            DataType::F64 => {

                // 1. check public arguments (constant n)
                let mut clamp_lower = match public_arguments.get("lower") {
                    Some(min) => min.clone().array()?.clone().vec_f64(Some(num_columns))
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
                let mut clamp_upper = match public_arguments.get("upper") {
                    Some(upper) => upper.array()?.clone().vec_f64(Some(num_columns))
                        .map_err(prepend("upper:"))?,

                    // 2. then private arguments (for example from another clamped column)
                    None => match properties.get("upper") {
                        Some(upper) => upper.array()?.upper_f64()
                            .map_err(prepend("upper:"))?,

                        // 3. then data properties (propagated from prior clamping/min/max)
                        None => data_property
                            .upper_f64().map_err(prepend("upper:"))?
                    }
                };

                if !clamp_lower.iter().zip(clamp_upper.clone()).all(|(low, high)| *low < high) {
                    return Err("lower is greater than upper".into());
                }

                // the actual data bound (if it exists) may be tighter than the clamping parameters
                if let Ok(data_minimum) = data_property.lower_f64_option() {
                    clamp_lower = clamp_lower.into_iter().zip(data_minimum)
                        // match on if the actual bound exists for each column, and remain conservative if not
                        .map(|(clamp_lower, optional_data_lower)| match optional_data_lower {
                            Some(data_lower) => clamp_lower.max(data_lower), // tighter data bound is only applied here
                            None => clamp_lower
                        }).collect()
                }
                if let Ok(data_upper) = data_property.upper_f64_option() {
                    clamp_upper = clamp_upper.into_iter().zip(data_upper)
                        .map(|(clamp_upper, optional_data_upper)| match optional_data_upper {
                            Some(data_upper) => clamp_upper.min(data_upper),
                            None => clamp_upper
                        }).collect()
                }

                // save revised bounds
                data_property.nature = Some(Nature::Continuous(NatureContinuous {
                    lower: Vector1DNull::F64(clamp_lower.into_iter().map(Some).collect()),
                    upper: Vector1DNull::F64(clamp_upper.into_iter().map(Some).collect()),
                }));

            },

            DataType::I64 => {
                // 1. check public arguments (constant n)
                let mut clamp_lower = match public_arguments.get("lower") {
                    Some(lower) => lower.clone().array()?.clone().vec_i64(Some(num_columns))
                        .map_err(prepend("lower:"))?,

                    // 2. then private arguments (for example from another clamped column)
                    None => match properties.get("lower") {
                        Some(lower) => lower.array()?.lower_i64()
                            .map_err(prepend("lower:"))?,

                        // 3. then data properties (propagated from prior clamping/lower/upper)
                        None => data_property
                            .lower_i64().map_err(prepend("lower:"))?
                    }
                };

                // 1. check public arguments (constant n)
                let mut clamp_upper = match public_arguments.get("upper") {
                    Some(upper) => upper.array()?.clone().vec_i64(Some(num_columns))
                        .map_err(prepend("upper:"))?,

                    // 2. then private arguments (for example from another clamped column)
                    None => match properties.get("upper") {
                        Some(upper) => upper.array()?.upper_i64()
                            .map_err(prepend("upper:"))?,

                        // 3. then data properties (propagated from prior clamping/min/max)
                        None => data_property
                            .upper_i64().map_err(prepend("upper:"))?
                    }
                };

                if !clamp_lower.iter().zip(clamp_upper.clone()).all(|(low, high)| *low < high) {
                    return Err("lower is greater than upper".into());
                }

                // the actual data bound (if it exists) may be tighter than the clamping parameters
                if let Ok(data_lower) = data_property.lower_i64_option() {
                    clamp_lower = clamp_lower.into_iter().zip(data_lower)
                        // match on if the actual bound exists for each column, and remain conservative if not
                        .map(|(clamp_lower, optional_data_lower)| match optional_data_lower {
                            Some(data_lower) => clamp_lower.max(data_lower), // tighter data bound is only applied here
                            None => clamp_lower
                        }).collect()
                }
                if let Ok(data_upper) = data_property.upper_i64_option() {
                    clamp_upper = clamp_upper.into_iter().zip(data_upper)
                        .map(|(clamp_upper, optional_data_upper)| match optional_data_upper {
                            Some(data_upper) => clamp_upper.min(data_upper),
                            None => clamp_upper
                        }).collect()
                }

                // save revised bounds
                data_property.nature = Some(Nature::Continuous(NatureContinuous {
                    lower: Vector1DNull::I64(clamp_lower.into_iter().map(Some).collect()),
                    upper: Vector1DNull::I64(clamp_upper.into_iter().map(Some).collect()),
                }));

            },
            _ => return Err("numeric clamping requires numeric data".into())
        }

        Ok(data_property.into())
    }

}


impl Expandable for proto::Clamp {
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
        let has_categorical = properties.contains_key("categories");

        if !has_categorical && !properties.contains_key("lower") {
            current_id += 1;
            let id_lower = current_id.to_owned();
            let value = Value::Array(Array::F64(
                ndarray::Array::from(properties.get("data").unwrap().to_owned().array()?.lower_f64()?).into_dyn()));
            let (patch_node, release) = get_literal(value, &component.batch)?;
            computation_graph.insert(id_lower.clone(), patch_node);
            releases.insert(id_lower.clone(), release);
            component.arguments.insert("lower".to_string(), id_lower);
        }

        if !has_categorical && !properties.contains_key("upper") {
            current_id += 1;
            let id_upper = current_id.to_owned();
            let value = Value::Array(Array::F64(
                ndarray::Array::from(properties.get("data").unwrap().to_owned().array()?.upper_f64()?).into_dyn()));
            let (patch_node, release) = get_literal(value, &component.batch)?;
            computation_graph.insert(id_upper.clone(), patch_node);
            releases.insert(id_upper.clone(), release);
            component.arguments.insert("upper".to_string(), id_upper);
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
pub mod test_clamp {
    use crate::base::test_data;

    pub mod utilities {
        use crate::components::cast::test_cast;
        use crate::bindings::Analysis;
        use crate::base::Value;

        pub fn analysis_f64_cont(value: Value, lower: Option<Value>, upper: Option<Value>) -> (Analysis, u32) {
            let (mut analysis, casted) = test_cast::utilities::analysis_f64(value);

            let lower = analysis.literal().value(match lower {
                Some(lower) => lower, None => 0.0.into()
            }).value_public(true).build();
            let upper = analysis.literal().value(match upper {
                Some(upper) => upper, None => 10.0.into()
            }).value_public(true).build();

            let clamped = analysis.clamp(casted)
                .lower(lower).upper(upper)
                .build();

            (analysis, clamped)
        }

        pub fn analysis_i64_cont(value: Value, lower: Option<Value>, upper: Option<Value>) -> (Analysis, u32) {
            let (mut analysis, casted) = test_cast::utilities::analysis_i64(value, lower.clone(), upper.clone());

            let lower = analysis.literal().value(match lower {
                Some(lower) => lower, None => 0.into()
            }).value_public(true).build();
            let upper = analysis.literal().value(match upper {
                Some(upper) => upper, None => 10.into()
            }).value_public(true).build();

            let clamped = analysis.clamp(casted)
                .lower(lower).upper(upper)
                .build();

            (analysis, clamped)
        }

        pub fn analysis_i64_cat(value: Value, categories: Value, null_value: Option<Value>) -> (Analysis, u32) {
            let lower: Value = i64::min_value().into();
            let upper: Value = i64::max_value().into();
            let (mut analysis, casted) = test_cast::utilities::analysis_i64(value, Some(lower), Some(upper));

            let categories = analysis.literal()
                .value(categories).value_public(true)
                .build();

            let null_value = analysis.literal()
                .value(match null_value {
                    Some(null_value) => null_value,
                    None => (-1).into()
                }).value_public(true)
                .build();

            let clamped = analysis.clamp(casted)
                .categories(categories)
                .null_value(null_value)
                .build();

            (analysis, clamped)
        }

        pub fn analysis_string_cat(value: Value, categories: Option<Value>, null_value: Option<Value>) -> (Analysis, u32) {
            let (mut analysis, casted) = test_cast::utilities::analysis_string(value);

            let categories = analysis.literal().value(match categories {
                Some(categories) => categories,
                None => Value::Jagged(vec![vec!["a", "b", "c", "d"].into_iter().map(String::from).collect::<Vec<String>>()].into())
            }).value_public(true).build();

            let null_value = analysis.literal().value(match null_value {
                Some(null_value) => null_value,
                None => "e".to_string().into()
            }).value_public(true).build();

            let clamped = analysis.clamp(casted)
                .categories(categories)
                .null_value(null_value)
                .build();
            (analysis, clamped)
        }

        pub fn analysis_bool_cat(value: Value) -> (Analysis, u32) {
            let (mut analysis, casted) = test_cast::utilities::analysis_bool(value, true.into());
            let categories = analysis.literal()
                .value(Value::Jagged(vec![vec![false, true]].into()))
                .value_public(true).build();

            let null_value = analysis.literal()
                .value(false.into())
                .value_public(true).build();

            let clamped = analysis.clamp(casted)
                .categories(categories)
                .null_value(null_value)
                .build();
            (analysis, clamped)
        }
    }

    macro_rules! test_f64 {
        ( $( $variant:ident; $lower:expr; $upper:expr, )*) => {
            $(
                #[test]
                fn $variant() {
                    let (analysis, clamped) = utilities::analysis_f64_cont(
                        test_data::$variant(), $lower, $upper);
                    analysis.properties(clamped).unwrap();
                }
            )*
        }
    }

    test_f64!(
        array1d_f64_0; Some(0.0.into()); Some(10.0.into()),
        array1d_f64_10_uniform; Some(0.0.into()); Some(10.0.into()),
    );

    macro_rules! test_i64 {
        ( $( $variant:ident; $lower:expr; $upper:expr; $categories:expr; $null_value:expr, )*) => {
            $(
                #[test]
                fn $variant() {
                    use crate::base::Value;

                    let (analysis, clamped) = utilities::analysis_i64_cat(
                        test_data::$variant(),
                        $categories, $null_value);
                    analysis.properties(clamped).unwrap();

                    let (analysis, clamped) = utilities::analysis_i64_cont(
                        test_data::$variant(), $lower, $upper);
                    analysis.properties(clamped).unwrap();
                }
            )*
        }
    }

    test_i64!(
        array1d_i64_0; None; None; Value::Jagged(vec![vec![1]].into()); None,
        array1d_i64_10_uniform; Some(0.into()); Some(10.into()); Value::Jagged(vec![(0..10).collect::<Vec<i64>>()].into()); Some((-1).into()),
    );

    macro_rules! test_string {
        ( $( $variant:ident; $categories:expr; $null_value:expr, )*) => {
            $(
                #[test]
                fn $variant() {
                    let (analysis, clamped) = utilities::analysis_string_cat(
                        test_data::$variant(),
                        $categories, $null_value);
                    analysis.properties(clamped).unwrap();
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
                    let (analysis, clamped) = utilities::analysis_bool_cat(
                        test_data::$variant());
                    analysis.properties(clamped).unwrap();
                }
            )*
        }
    }

    test_bool!(
        array1d_bool_0,
        array1d_bool_10_uniform,
    );
}
