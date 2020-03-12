use crate::errors::*;

use std::collections::HashMap;

use crate::{proto, base};

use crate::components::{Component};
use crate::base::{Hashmap, Value, NodeProperties, ValueProperties, HashmapProperties};

impl Component for proto::Materialize {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        _public_arguments: &HashMap<String, Value>,
        _properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        Ok(HashmapProperties {
            num_records: None,
            disjoint: false,
            value_properties: Hashmap::<ValueProperties>::Str(HashMap::new())
        }.into())
//        Ok(Properties {
//            nullity: true,
//            releasable: false,
//            nature: None,
//            c_stability: vec![],
//            num_columns: None,
//            num_records: vec![],
//            aggregator: None
//        })
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}
