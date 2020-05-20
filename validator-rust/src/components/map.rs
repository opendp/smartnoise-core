use crate::errors::*;


use std::collections::HashMap;

use crate::{proto, base, Warnable};

use crate::components::{Component};
use crate::base::{Value, ValueProperties, IndexmapProperties};
use crate::utilities::get_common_value;


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