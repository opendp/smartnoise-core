use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component};
use crate::base::{Value, ValueProperties, IndexmapProperties};
use crate::utilities::get_common_value;


impl Component for proto::Map {
    fn propagate_property(
        &self,
        privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
        node_id: u32,
    ) -> Result<ValueProperties> {
        let (props_partitioned, props_singular): (HashMap<String, ValueProperties>, HashMap<String, ValueProperties>) = properties.clone().into_iter()
            .partition(|(_, props)| props.indexmap()
                .and_then(IndexmapProperties::assert_is_partition).is_ok());

        let first_partition = props_partitioned.values().next()
            .ok_or_else(|| "there must be at least one partitioned argument to map")?.clone();

        let num_partitions = first_partition.indexmap()?.properties.keys_length();

        let mapped_properties = first_partition.indexmap()?.from_values((0..num_partitions).map(|idx| {
            let mut partition_props = props_partitioned.iter()
                .map(|(name, map_props)| Ok((
                    name.clone(),
                    map_props.indexmap()?.properties.values()[idx].clone()
                )))
                .collect::<Result<HashMap<String, ValueProperties>>>()?;

            partition_props.extend(props_singular.clone());

            self.component.as_ref().ok_or_else(|| "component must be defined")?
                .variant.as_ref().ok_or_else(|| "variant must be defined")?
                .propagate_property(
                    privacy_definition,
                    public_arguments,
                    &partition_props,
                    node_id,
                )
        })
            .collect::<Result<Vec<ValueProperties>>>()?);

        Ok(IndexmapProperties {
            num_records: None,
            disjoint: props_partitioned.values().all(|v| v.indexmap().map(|v| v.disjoint).unwrap_or(false)),
            properties: mapped_properties,
            dataset_id: get_common_value(&props_partitioned.values()
                .map(|v| v.indexmap().map(|v| v.dataset_id)
                    .unwrap_or(None)).collect())
                .ok_or_else(|| "dataset_id must be shared among all partitions")?,
            variant: proto::indexmap_properties::Variant::Partition,
        }.into())
    }
}

// impl Expandable for proto::Map {
//     fn expand_component(
//         &self,
//         privacy_definition: &Option<PrivacyDefinition>,
//         component: &proto::Component,
//         properties: &NodeProperties,
//         component_id: &u32,
//         maximum_id: &u32
//     ) -> Result<ComponentExpansion> {
//         let component = self.component.as_ref().ok_or_else(|| "component must be defined")?;
//
//         component
//             .variant.as_ref().ok_or_else(|| "component variant must be defined")?
//             .expand_component(
//                 privacy_definition,
//                 component,
//                 properties, // properties need to be the Array variant
//                 component_id,
//                 maximum_id
//             )
//     }
// }