use crate::errors::*;

use std::collections::HashMap;


use crate::{proto, base};
use crate::hashmap;
use crate::components::{Component, Expandable};

use crate::base::{Value, NodeProperties, ValueProperties, DataType, Nature, NatureCategorical, Jagged, Vector1DNull, NatureContinuous, Array};
use crate::utilities::prepend;
use itertools::Itertools;

impl Component for proto::Cast {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
        _node_id: u32
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or_else(|| Error::from("data: missing"))?.array()
            .map_err(prepend("data:"))?.clone();

        data_property.assert_is_not_aggregated()?;
        let prior_datatype = data_property.data_type.clone();

        data_property.data_type = match self.atomic_type.to_lowercase().as_str() {
            "float" => DataType::F64,
            "real" => DataType::F64,
            "int" => DataType::I64,
            "integer" => DataType::I64,
            "bool" => DataType::Bool,
            "string" => DataType::Str,
            "str" => DataType::Str,
            _ => bail!("data type is not recognized. Must be one of \"float\", \"int\", \"bool\" or \"string\"")
        };

        match data_property.data_type {
            DataType::Unknown => unreachable!(),
            DataType::Bool => {
                // true label must be defined
                let true_label = public_arguments.get("true_label")
                    .ok_or_else(|| Error::from("true_label: missing, must be public"))?.array()?.clone();

                data_property.nature = match data_property.nature {
                    Some(nature) => match nature {
                        Nature::Categorical(cat_nature) => Some(Nature::Categorical(NatureCategorical {
                            categories: match (cat_nature.categories, true_label) {
                                (Jagged::I64(cats), Array::I64(true_label)) => Jagged::Bool(cats.iter()
                                    .map(|cats| cats.into_iter().map(|v| Some(v) == true_label.first())
                                        .unique().collect::<Vec<_>>())
                                    .collect::<Vec<Vec<_>>>()),
                                (Jagged::F64(cats), Array::F64(true_label)) => Jagged::Bool(cats.iter()
                                    .map(|cats| cats.into_iter().map(|v| Some(v) == true_label.first())
                                        .unique().collect::<Vec<_>>())
                                    .collect::<Vec<Vec<_>>>()),
                                (Jagged::Bool(cats), Array::Bool(true_label)) => Jagged::Bool(cats.iter()
                                    .map(|cats| cats.into_iter().map(|v| Some(v) == true_label.first())
                                        .unique().collect::<Vec<_>>())
                                    .collect::<Vec<Vec<_>>>()),
                                (Jagged::Str(cats), Array::Str(true_label)) => Jagged::Bool(cats.iter()
                                    .map(|cats| cats.into_iter().map(|v| Some(v) == true_label.first())
                                        .unique().collect::<Vec<_>>())
                                    .collect::<Vec<Vec<_>>>()),
                                _ => return Err("type of true label must match the data type".into())
                            }
                        })),
                        Nature::Continuous(_) => None
                    },
                    None => None
                };

                data_property.nature = data_property.num_columns
                    .map(|num_columns| Nature::Categorical(NatureCategorical {
                        categories: Jagged::Bool((0..num_columns).map(|_| vec![true, false]).collect())
                    }));

                data_property.nullity = false;
            },
            DataType::I64 => {
                // lower must be defined, for imputation of values that won't cast
                public_arguments.get("lower")
                    .ok_or_else(|| Error::from("lower: missing, must be public"))?.first_i64()
                    .map_err(prepend("type:"))?;
                // max must be defined
                public_arguments.get("upper")
                    .ok_or_else(|| Error::from("upper: missing, must be public"))?.first_i64()
                    .map_err(prepend("type:"))?;

                data_property.nature = None;
                data_property.nature = match data_property.nature {
                    Some(nature) => match nature.clone() {
                        Nature::Categorical(cat_nature) => match cat_nature.categories {
                            // properties are lost because floats cannot be categorical
                            Jagged::F64(_) => None,
                            Jagged::I64(_) => Some(nature.clone()),
                            Jagged::Bool(cats) =>
                                Some(Nature::Categorical(NatureCategorical {
                                    categories: Jagged::I64(cats.into_iter()
                                        .map(|cats| cats.into_iter()
                                            .map(|v| if v { 1 } else { 0 })
                                            .unique().collect::<Vec<i64>>())
                                        .collect())
                                })),

                            // properties are lost because of potential imputation
                            Jagged::Str(_) => None
                        },
                        Nature::Continuous(bounds) => match (bounds.lower.clone(), bounds.upper.clone()) {
                            (Vector1DNull::F64(lower), Vector1DNull::F64(upper)) =>
                                Some(Nature::Continuous(NatureContinuous {
                                    lower: Vector1DNull::I64(lower.into_iter()
                                        .map(|v| v.map(|v| v.round() as i64))
                                        .collect()),
                                    upper: Vector1DNull::I64(upper.into_iter()
                                        .map(|v| v.map(|v| v.round() as i64))
                                        .collect())
                                })),
                            (Vector1DNull::I64(_), Vector1DNull::I64(_)) =>
                                Some(Nature::Continuous(NatureContinuous { lower: bounds.lower, upper: bounds.upper })),
                            _ => None
                        }
                    },
                    None => None
                };
                data_property.nullity = false;
            },
            DataType::Str => {
                data_property.nullity = false;
                data_property.nature = match data_property.nature {
                    Some(nature) => match nature {
                        Nature::Categorical(nature) => match nature.categories {
                            Jagged::F64(_) => None,
                            Jagged::Bool(jagged) =>
                                Some(Nature::Categorical(NatureCategorical {
                                    categories: Jagged::Str(jagged.into_iter()
                                        .map(|cats| cats.into_iter()
                                            .map(|v| v.to_string())
                                            .unique().collect())
                                        .collect::<Vec<Vec<String>>>())
                                })),
                            Jagged::I64(jagged) =>
                                Some(Nature::Categorical(NatureCategorical {
                                    categories: Jagged::Str(jagged.into_iter()
                                        .map(|cats| cats.into_iter()
                                            .map(|v| v.to_string())
                                            .unique().collect())
                                        .collect::<Vec<Vec<String>>>())
                                })),
                            Jagged::Str(jagged) => Some(Nature::Categorical(NatureCategorical {
                                categories: Jagged::Str(jagged.clone())
                            }))
                        },
                        _ => None
                    },
                    None => None
                }
            },
            DataType::F64 => {
                data_property.nature = None;
                data_property.nullity = match prior_datatype {
                    DataType::F64 => data_property.nullity,
                    DataType::Bool => false,
                    _ => true
                }
            }
        };

        Ok(data_property.into())
    }

}

macro_rules! make_expandable {
    ($variant:ident, $var_type:expr) => {
        impl Expandable for proto::$variant {
            fn expand_component(
                &self,
                _privacy_definition: &Option<proto::PrivacyDefinition>,
                component: &proto::Component,
                _properties: &base::NodeProperties,
                component_id: &u32,
                _maximum_id: &u32,
            ) -> Result<proto::ComponentExpansion> {
                Ok(proto::ComponentExpansion {
                    computation_graph: hashmap![component_id.clone() => proto::Component {
                        arguments: component.arguments.clone(),
                        variant: Some(proto::component::Variant::Cast(proto::Cast {
                            atomic_type: $var_type
                        })),
                        omit: false,
                        submission: component.submission,
                    }],
                    properties: HashMap::new(),
                    releases: HashMap::new(),
                    // add the component_id, to force the node to be re-evaluated and the Cast to be expanded
                    traversal: vec![*component_id]
                })
            }
        }
    }
}

make_expandable!(ToBool, "bool".to_string());
make_expandable!(ToFloat, "float".to_string());
make_expandable!(ToInt, "int".to_string());
make_expandable!(ToString, "string".to_string());


#[cfg(test)]
pub mod test_cast {
    use crate::base::test_data;

    pub mod utilities {
        use crate::components::literal::test_literal;
        use crate::bindings::Analysis;
        use crate::base::Value;

        pub fn analysis_f64(value: Value) -> (Analysis, u32) {
            let (mut analysis, literal) = test_literal::analysis_literal(value, true);
            let cast = analysis.to_float(literal).build();
            (analysis, cast)
        }

        pub fn analysis_i64(value: Value, lower: Option<Value>, upper: Option<Value>) -> (Analysis, u32) {
            let (mut analysis, literal) = test_literal::analysis_literal(value, true);
            let lower = analysis.literal().value(match lower {
                Some(lower) => lower, None => 0.into()
            }).value_public(true).build();
            let upper = analysis.literal().value(match upper {
                Some(upper) => upper, None => 10.into()
            }).value_public(true).build();
            let cast = analysis.to_int(literal, lower, upper).build();
            (analysis, cast)
        }

        pub fn analysis_string(value: Value) -> (Analysis, u32) {
            let (mut analysis, literal) = test_literal::analysis_literal(value, true);
            let cast = analysis.to_string(literal).build();
            (analysis, cast)
        }

        pub fn analysis_bool(value: Value, true_label: Value) -> (Analysis, u32) {
            let (mut analysis, literal) = test_literal::analysis_literal(value, true);
            let true_label = analysis.literal().value(true_label).value_public(true).build();
            let cast = analysis.to_bool(literal, true_label).build();
            (analysis, cast)
        }
    }

    macro_rules! test_propagation {
        ( $( $variant:ident: $true_label:expr, )*) => {
            $(
                #[test]
                fn $variant() {
                    let (analysis, cast) = utilities::analysis_f64(test_data::$variant());
                    analysis.properties(cast).unwrap();

                    let (analysis, cast) = utilities::analysis_i64(test_data::$variant(), None, None);
                    analysis.properties(cast).unwrap();

                    let (analysis, cast) = utilities::analysis_bool(test_data::$variant(), $true_label);
                    analysis.properties(cast).unwrap();

                    let (analysis, cast) = utilities::analysis_string(test_data::$variant());
                    analysis.properties(cast).unwrap();
                }
            )*
        }
    }

    test_propagation!(
        array1d_f64_0: 1.0.into(),
        array1d_i64_0: 1.into(),
        array1d_string_0: "a".to_string().into(),
        array1d_bool_0: true.into(),
        array1d_f64_10_uniform: 1.0.into(),
        array1d_i64_10_uniform: 0.into(),
        array1d_string_10_uniform: "a".to_string().into(),
        array1d_bool_10_uniform: true.into(),
    );
}
