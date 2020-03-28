use crate::errors::*;

use std::collections::HashMap;

use crate::{proto};

use crate::components::{Component, Aggregator, Expandable};
use crate::base::{Value, NodeProperties, AggregatorProperties, SensitivitySpace, ValueProperties, DataType, NatureContinuous, Nature, Vector1DNull, Jagged};
use crate::utilities::{prepend, get_literal};
use ndarray::{arr1, Array};


impl Component for proto::Histogram {
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        let categories = data_property.categories()?;

        if categories.num_columns() != 1 {
            return Err("categories must contain one column".into())
        }
        data_property.num_records = Some(categories.lengths()?[0] as i64);

        // save a snapshot of the state when aggregating
        data_property.aggregator = Some(AggregatorProperties {
            component: proto::component::Variant::from(self.clone()),
            properties: properties.clone()
        });

        let data_num_columns = data_property.num_columns()?;
        data_property.nature = Some(Nature::Continuous(NatureContinuous {
            min: Vector1DNull::I64((0..data_num_columns).map(|_| Some(0)).collect()),
            max: Vector1DNull::I64((0..data_num_columns).map(|_| None).collect()),
        }));
        data_property.data_type = DataType::I64;

        Ok(data_property.into())
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}


impl Expandable for proto::Histogram {
    /// If min and max are not supplied, but are known statically, then add them automatically
    fn expand_component(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        component: &proto::Component,
        properties: &NodeProperties,
        component_id: &u32,
        maximum_id: &u32,
    ) -> Result<proto::ComponentExpansion> {
        let mut current_id = *maximum_id;
        let mut computation_graph: HashMap<u32, proto::Component> = HashMap::new();
        let mut releases: HashMap<u32, proto::ReleaseNode> = HashMap::new();

        let mut component = component.clone();

        current_id += 1;
        let id_categories = current_id;
        let categories = properties.get("data").ok_or("data: missing")?.array()?.categories()?;
        let value = match categories {
            Jagged::I64(jagged) => arr1(jagged[0].as_ref().unwrap()).into_dyn().into(),
            Jagged::F64(jagged) => arr1(jagged[0].as_ref().unwrap()).into_dyn().into(),
            Jagged::Bool(jagged) => arr1(jagged[0].as_ref().unwrap()).into_dyn().into(),
            Jagged::Str(jagged) => arr1(jagged[0].as_ref().unwrap()).into_dyn().into(),
        };
        let (patch_node, categories_release) = get_literal(&value, &component.batch)?;
        computation_graph.insert(id_categories.clone(), patch_node);
        releases.insert(id_categories.clone(), categories_release);
        component.arguments.insert("categories".to_string(), id_categories);

        computation_graph.insert(component_id.clone(), component);

        Ok(proto::ComponentExpansion {
            computation_graph,
            properties: HashMap::new(),
            releases,
            traversal: Vec::new()
        })
    }
}


impl Aggregator for proto::Histogram {
    fn compute_sensitivity(
        &self,
        privacy_definition: &proto::PrivacyDefinition,
        properties: &NodeProperties,
        sensitivity_type: &SensitivitySpace
    ) -> Result<Value> {
        let data_property = properties.get("data")
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?.clone();

        data_property.assert_is_not_aggregated()?;

        match sensitivity_type {
            SensitivitySpace::KNorm(_k) => {
                // k has no effect on the sensitivity, and is ignored

                use proto::privacy_definition::Neighboring;
                use proto::privacy_definition::Neighboring::{Substitute, AddRemove};
                let neighboring_type = Neighboring::from_i32(privacy_definition.neighboring)
                    .ok_or_else(|| Error::from("neighboring definition must be either \"AddRemove\" or \"Substitute\""))?;

                // when categories are defined, a disjoint group by query is performed
                let categories_length = data_property.categories()?.lengths()?[0];

                let num_records = data_property.num_records;

                // SENSITIVITY DERIVATIONS
                let sensitivity: f64 = match (neighboring_type, categories_length, num_records) {
                    // one category, known N. Applies to any neighboring type.
                    (_, 1, Some(_)) => 0.,

                    // one category, unknown N. The sensitivity here is really zero-- artificially raised
                    (Substitute, 1, None) => 1.,
                    // two categories, known N. Knowing N determines the second category
                    (Substitute, 2, Some(_)) => 1.,

                    // one category, unknown N
                    (AddRemove, 1, None) => 1.,
                    // two categories, known N
                    (AddRemove, 2, Some(_)) => 1.,

                    // over two categories, N either known or unknown. Record may switch from one bin to another.
                    (Substitute, _, _) => 2.,
                    // over two categories, N either known or unknown. Only one bin may be edited.
                    (AddRemove, _, _) => 1.,
                };

                let num_columns = data_property.num_columns()?;
                let num_records = categories_length;
                Ok(Array::from_shape_vec(
                    vec![num_records as usize, num_columns as usize],
                    (0..(num_records * num_columns)).map(|_| sensitivity).collect())?.into())
            },
            _ => Err("Histogram sensitivity is only implemented for KNorm".into())
        }
    }
}
