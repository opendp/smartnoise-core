use crate::errors::*;


use std::collections::HashMap;

use crate::{base};
use crate::proto;

use crate::components::{Component};




use crate::base::{Properties, Value, NodeProperties};
use std::ops::Deref;


impl Component for proto::Cast {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<Properties> {
        let mut data_property = properties.get("data")
            .ok_or::<Error>("data is a required argument for Cast".into())?.clone();

        let _datatype = public_arguments.get("type")
            .ok_or::<Error>("data type is a required argument for Cast".into())?.deref().to_owned().get_first_str()?;

        // clear continuous properties if casting to categorical-only raw type
//        match &datatype {
//            dt if dt == &"STRING".to_string() => {
//                if let Nature::Continuous(nature) = data_property.nature.clone() {
//                    data_property.nature = None
//                }
//            },
//            dt if dt == &"BOOL".to_string() => {
//                if let Nature::Continuous(nature) = data_property.nature.clone() {
//                    data_property.nature = None
//                }
//            },
//        }

        data_property.nature = None;
        data_property.nullity = true;

        Ok(data_property)
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}
