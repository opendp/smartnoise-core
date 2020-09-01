use crate::errors::*;

use crate::{proto, Warnable, base, Float};

use crate::components::{Component, Sensitivity, Expandable};
use crate::base::{
    IndexKey, Value, NodeProperties, AggregatorProperties, SensitivitySpace,
    ValueProperties, DataType, NatureContinuous, Nature, Vector1DNull, Jagged,
    DataframeProperties
};
use crate::utilities::{prepend, get_literal};
use ndarray::{arr1, Array};
use indexmap::map::IndexMap;
use crate::utilities::inference::infer_property;
use crate::base::ArrayProperties;


impl Component for proto::Histogram {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        properties: NodeProperties,
        node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        let mut data_property: ArrayProperties = properties.get::<base::IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        // this check is already guaranteed by the state space, but still included for safety
        if data_property.data_type == DataType::Unknown {
            return Err("data_type must be known".into())
        }

        let num_columns = data_property.num_columns()?;

        // save a snapshot of the state when aggregating
        data_property.aggregator = Some(AggregatorProperties {
            component: proto::component::Variant::Histogram(self.clone()),
            properties,
            lipschitz_constants: ndarray::Array::from_shape_vec(
                vec![1, num_columns as usize],
                (0..num_columns).map(|_| 1.).collect())?.into_dyn().into(),
            censor_rows: false
        });
        data_property.dataset_id = Some(node_id as i64);

        let mut count_property = data_property.clone();

        count_property.nature = Some(Nature::Continuous(NatureContinuous {
            lower: Vector1DNull::Int((0..num_columns).map(|_| Some(0)).collect()),
            upper: Vector1DNull::Int((0..num_columns).map(|_| None).collect()),
        }));
        count_property.data_type = DataType::Int;

        if let Ok(categories) = data_property.categories() {
            // properties specific to categorical histograms

            if categories.num_columns() != 1 {
                return Err("categories must contain one column".into())
            }
            count_property.num_records = Some(categories.num_records()[0] as i64);

            Ok(ValueProperties::Array(count_property).into())

        } else {
            // properties specific to stability histograms
            if num_columns > 1 {
                return Err("only one data column is allowed on stability-histograms".into())
            }

            data_property.num_records = None;
            count_property.num_records = None;

            data_property.dimensionality = Some(1);
            count_property.num_records = Some(1);

            data_property.dataset_id = Some(node_id as i64);
            count_property.dataset_id = Some(node_id as i64);

            if let Some(aggregator) = &mut data_property.aggregator {
                aggregator.censor_rows = true;
            }
            if let Some(aggregator) = &mut count_property.aggregator {
                aggregator.censor_rows = true;
            }

            Ok(ValueProperties::Dataframe(DataframeProperties {
                children: indexmap![
                    "counts".into() => count_property.into(),
                    "categories".into() => data_property.into()
                ]
            }).into())
        }

    }
}


impl Expandable for proto::Histogram {
    /// If min and max are not supplied, but are known statically, then add them automatically
    /// Add nodes for clamp or digitize if categories or edges are passed
    fn expand_component(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        component: &proto::Component,
        _public_arguments: &IndexMap<IndexKey, &Value>,
        properties: &NodeProperties,
        component_id: u32,
        mut maximum_id: u32,
    ) -> Result<base::ComponentExpansion> {

        let mut expansion = base::ComponentExpansion::default();

        let data_id = component.arguments().get::<IndexKey>(&"data".into())
            .ok_or_else(|| Error::from("data is a required argument to Histogram"))?.to_owned();

        let mut component = component.clone();

        match (
            component.arguments().get::<IndexKey>(&"edges".into()),
            component.arguments().get::<IndexKey>(&"categories".into())) {

            (Some(edges_id), None) => {
                // digitize
                let mut arguments = indexmap![
                    IndexKey::from("data") => data_id,
                    "edges".into() => *edges_id
                ];

                let prior_arguments = component.arguments();
                prior_arguments.get::<IndexKey>(&"null_value".into())
                    .map(|v| arguments.insert("null_value".into(), *v));
                prior_arguments.get::<IndexKey>(&"inclusive_left".into())
                    .map(|v| arguments.insert("inclusive_left".into(), *v));

                maximum_id += 1;
                let id_digitize = maximum_id;
                expansion.computation_graph.insert(id_digitize, proto::Component {
                    arguments: Some(proto::ArgumentNodeIds::new(arguments)),
                    variant: Some(proto::component::Variant::Digitize(proto::Digitize {})),
                    omit: true,
                    submission: component.submission,
                });
                component.arguments = Some(proto::ArgumentNodeIds::new(indexmap!["data".into() => id_digitize]));
                expansion.traversal.push(id_digitize);
            }

            (None, Some(categories_id)) => {
                // clamp
                let prior_arguments = component.arguments();
                let null_id = prior_arguments.get::<IndexKey>(&"null_value".into())
                    .ok_or_else(|| Error::from("null_value is a required argument to Histogram when categories are not known"))?;
                maximum_id += 1;
                let id_clamp = maximum_id;
                expansion.computation_graph.insert(id_clamp, proto::Component {
                    arguments: Some(proto::ArgumentNodeIds::new(indexmap![
                        "data".into() => data_id,
                        "categories".into() => *categories_id,
                        "null_value".into() => *null_id
                    ])),
                    variant: Some(proto::component::Variant::Clamp(proto::Clamp {})),
                    omit: true,
                    submission: component.submission,
                });
                component.arguments = Some(proto::ArgumentNodeIds::new(indexmap!["data".into() => id_clamp]));
                expansion.traversal.push(id_clamp);
            }

            (None, None) => {
                let data_property = properties.get::<IndexKey>(&"data".into())
                    .ok_or("data: missing")?.array()
                    .map_err(prepend("data:"))?.clone();

                if let Ok(categories) = data_property.categories() {
                    maximum_id += 1;
                    let id_categories = maximum_id;
                    let value = match categories {
                        Jagged::Int(jagged) => arr1(&jagged[0]).into_dyn().into(),
                        Jagged::Float(jagged) => arr1(&jagged[0]).into_dyn().into(),
                        Jagged::Bool(jagged) => arr1(&jagged[0]).into_dyn().into(),
                        Jagged::Str(jagged) => arr1(&jagged[0]).into_dyn().into(),
                    };
                    let (patch_node, categories_release) = get_literal(value, component.submission)?;
                    expansion.computation_graph.insert(id_categories, patch_node);
                    expansion.properties.insert(id_categories, infer_property(&categories_release.value, None)?);
                    expansion.releases.insert(id_categories, categories_release);
                    component.insert_argument(&"categories".into(), id_categories);
                }
            }

            (Some(_), Some(_)) => return Err("either edges or categories must be supplied".into())
        }

        expansion.computation_graph.insert(component_id, component);

        Ok(expansion)
    }
}


impl Sensitivity for proto::Histogram {
    /// Histogram sensitivities [are backed by the the proofs here](https://github.com/opendifferentialprivacy/whitenoise-core/blob/955703e3d80405d175c8f4642597ccdf2c00332a/whitepapers/sensitivities/counts/counts.pdf).
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &SensitivitySpace
    ) -> Result<Value> {
        let data_property = properties.get::<base::IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        data_property.assert_is_not_aggregated()?;



        match sensitivity_type {
            SensitivitySpace::KNorm(k) => {

                use proto::privacy_definition::Neighboring;
                use proto::privacy_definition::Neighboring::{Substitute, AddRemove};
                let neighboring_type = Neighboring::from_i32(privacy_definition.neighboring)
                    .ok_or_else(|| Error::from("neighboring definition must be either \"AddRemove\" or \"Substitute\""))?;

                // when categories are defined, a disjoint group by query is performed
                let categories_length = data_property.categories().ok().map(|v| v.num_records()[0]);

                let num_records = data_property.num_records;

                // SENSITIVITY DERIVATIONS
                let sensitivity: Float = match (neighboring_type, categories_length, num_records) {
                    // one category, known N. Applies to any neighboring type.
                    (_, Some(1), Some(_)) => 0.,

                    // one category, unknown N. The sensitivity here is really zero-- artificially raised
                    (Substitute, Some(1), None) => 1.,
                    // two categories, known N. Knowing N determines the second category
                    (Substitute, Some(2), Some(_)) => 1.,

                    // one category, unknown N
                    (AddRemove, Some(1), None) => 1.,
                    // two categories, known N
                    (AddRemove, Some(2), Some(_)) => 1.,

                    // over two categories, N either known or unknown. Record may switch from one bin to another.
                    (Substitute, _, _) => match k {
                        1 => 2.,
                        2 => 2.0_f64.sqrt(),
                        _ =>  return Err("KNorm sensitivity is only supported in L1 and L2 spaces".into())
                    } ,
                    // over two categories, N either known or unknown. Only one bin may be edited.
                    (AddRemove, _, _) => 1.,
                };

                let num_columns = data_property.num_columns()?;

                Ok(Array::from_shape_vec(
                    vec![1, num_columns as usize],
                    (0..num_columns)
                        .map(|_| sensitivity)
                        .collect::<Vec<Float>>())?.into())
            },
            _ => Err("Histogram sensitivity is only implemented for KNorm".into())
        }
    }
}
