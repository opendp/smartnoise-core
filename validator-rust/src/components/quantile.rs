use indexmap::map::IndexMap;
use ndarray::prelude::*;

use crate::{base, Float, proto, Warnable};
use crate::base::{
    AggregatorProperties, DataType, IndexKey, JaggedProperties,
    NodeProperties, SensitivitySpace, Value, ValueProperties,
    ArrayProperties, Nature, NatureContinuous, Vector1DNull
};
use crate::components::{Component, Expandable, Sensitivity};
use crate::errors::*;
use crate::utilities::prepend;

impl Component for proto::Quantile {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: IndexMap<base::IndexKey, &Value>,
        mut properties: base::NodeProperties,
        node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let mut data_property: ArrayProperties = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        if data_property.data_type != DataType::Float && data_property.data_type != DataType::Int {
            return Err("data: atomic type must be numeric".into());
        }

        let has_bounds = public_arguments.contains_key(&IndexKey::from("lower"))
            && public_arguments.contains_key(&IndexKey::from("upper"));

        Ok(match properties.remove::<IndexKey>(&"candidates".into()) {
            Some(candidates_property) => {
                let candidates_property: &ArrayProperties = candidates_property.array()
                    .map_err(prepend("candidates:"))?;

                if data_property.data_type != candidates_property.data_type {
                    return Err("data_type of data must match data_type of candidates".into())
                }

                if data_property.num_columns()? != candidates_property.num_columns()? {
                    return Err("candidates is not column-conformable with the data".into())
                }

                // upper bound for n * max(a, 1 - a) - |(1 - a) * #z - a * (n - #z)|
                //               = n * max(a, 1 - a) - |#z - an|
                //              <= n * max(a, 1 - a) (because |#z - an| minimized when #z = an)
                let utility_upper_bound = candidates_property.num_records
                    .map(|n| n as f64 * self.alpha.max(1. - self.alpha));

                // bounds make the quantile nan-robust
                if !has_bounds {
                    data_property.assert_non_null()?;
                    data_property.assert_is_not_empty()?;
                }

                ValueProperties::Array(ArrayProperties {
                    num_records: candidates_property.num_records,
                    num_columns: data_property.num_columns,
                    nullity: candidates_property.nullity,
                    releasable: data_property.releasable && candidates_property.releasable,
                    c_stability: data_property.c_stability.clone(),
                    aggregator: Some(AggregatorProperties::new(
                        proto::component::Variant::Quantile(self.clone()),
                        properties, data_property.num_columns()?)),
                    nature: Some(Nature::Continuous(NatureContinuous {
                        lower: Vector1DNull::Float((0..data_property.num_columns()?)
                            .map(|_| Some(0.)).collect()),
                        upper: Vector1DNull::Float((0..data_property.num_columns()?)
                            .map(|_| utility_upper_bound).collect())
                    })),
                    data_type: DataType::Float,
                    dataset_id: None,
                    node_id: node_id as i64,
                    is_not_empty: true,
                    dimensionality: candidates_property.dimensionality,
                    group_id: data_property.group_id,
                    naturally_ordered: data_property.naturally_ordered,
                    sample_proportion: None
                }).into()
            },
            None => {
                if has_bounds { return Err("bounds are only useful when evaluating candidates".into()) }

                data_property.assert_is_not_empty()?;
                // save a snapshot of the state when aggregating
                data_property.aggregator = Some(AggregatorProperties::new(
                    proto::component::Variant::Quantile(self.clone()),
                    properties,
                    data_property.num_columns()?));

                data_property.num_records = Some(1);

                ValueProperties::Array(data_property).into()
            }
        })
    }
}

impl Sensitivity for proto::Quantile {
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &SensitivitySpace,
    ) -> Result<Value> {
        let data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        data_property.assert_is_not_aggregated()?;

        match sensitivity_type {
            SensitivitySpace::KNorm(_k) => {
                let lower = data_property.lower_float()?;
                let upper = data_property.upper_float()?;

                let row_sensitivity = lower.iter()
                    .zip(upper.iter())
                    .map(|(min, max)| max - min)
                    .collect::<Vec<Float>>();

                let mut array_sensitivity = Array::from(row_sensitivity).into_dyn();
                array_sensitivity.insert_axis_inplace(Axis(0));

                Ok(array_sensitivity.into())
            }

            // SensitivitySpace::Exponential(implementation) if implementation == "standard" => {
            SensitivitySpace::Exponential => {
                data_property.assert_non_null()?;

                let neighboring_type = Neighboring::from_i32(privacy_definition.neighboring)
                    .ok_or_else(|| Error::from("neighboring definition must be either \"AddRemove\" or \"Substitute\""))?;
                use proto::privacy_definition::Neighboring;
                let cell_sensitivity = match neighboring_type {
                    Neighboring::AddRemove => self.alpha.max(1. - self.alpha),
                    Neighboring::Substitute => 1.
                } as Float;

                let row_sensitivity = (0..data_property.num_columns()?)
                    .map(|_| cell_sensitivity)
                    .collect::<Vec<Float>>();

                let array_sensitivity = Array::from(row_sensitivity).into_dyn();
                // array_sensitivity.insert_axis_inplace(Axis(0));

                Ok(array_sensitivity.into())
            }

            // this implementation is robust to nans, but only supports alpha == .5
            // SensitivitySpace::Exponential(implementation) if implementation == "NaN-robust" => {
            //
            //     if self.alpha != 0.5 {
            //         return Err(Error::from("alpha must be 0.5 to use the NaN-robust quantile"))
            //     }
            //
            //     let neighboring_type = Neighboring::from_i32(privacy_definition.neighboring)
            //         .ok_or_else(|| Error::from("neighboring definition must be either \"AddRemove\" or \"Substitute\""))?;
            //     use proto::privacy_definition::Neighboring;
            //     let cell_sensitivity = match neighboring_type {
            //         Neighboring::AddRemove => self.alpha.max(1. - self.alpha),
            //         Neighboring::Substitute => 1.
            //     } as Float;
            //
            //     let row_sensitivity = (0..data_property.num_columns()?)
            //         .map(|_| cell_sensitivity)
            //         .collect::<Vec<Float>>();
            //
            //     let array_sensitivity = Array::from(row_sensitivity).into_dyn();
            //     // array_sensitivity.insert_axis_inplace(Axis(0));
            //
            //     Ok(array_sensitivity.into())
            // }
            _ => Err("Quantile sensitivity is not implemented for the specified sensitivity space".into())
        }
    }
}


macro_rules! make_quantile {
    ($variant:ident, $alpha:expr, $interpolation:expr) => {

        impl Expandable for proto::$variant {
            fn expand_component(
                &self,
                _privacy_definition: &Option<proto::PrivacyDefinition>,
                component: &proto::Component,
                _public_arguments: &IndexMap<IndexKey, &Value>,
                _properties: &base::NodeProperties,
                component_id: u32,
                _maximum_id: u32,
            ) -> Result<base::ComponentExpansion> {
                let mut expansion = base::ComponentExpansion::default();

                expansion.computation_graph.insert(component_id, proto::Component {
                    arguments: component.arguments.clone(),
                    variant: Some(proto::component::Variant::Quantile(proto::Quantile {
                        alpha: $alpha,
                        interpolation: $interpolation
                    })),
                    omit: component.omit,
                    submission: component.submission,
                });
                expansion.traversal.push(component_id);

                Ok(expansion)
            }
        }
    }
}

make_quantile!(Minimum, 0.0, "lower".to_string());
make_quantile!(Median, 0.5, "midpoint".to_string());
make_quantile!(Maximum, 1.0, "upper".to_string());
