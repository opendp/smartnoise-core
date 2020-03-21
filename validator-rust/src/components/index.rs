use crate::errors::*;

use std::collections::HashMap;
use crate::base::{ArrayND, Value, NodeProperties, ValueProperties, prepend, Hashmap};

use crate::{proto, base};
use crate::components::Component;

use std::ops::Deref;

// TODO: this could use additional checks to prevent out of bounds


impl Component for proto::Index {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &base::NodeProperties,
    ) -> Result<ValueProperties> {
        let data_property = properties.get("data")
            .ok_or("data: missing")?.get_hashmap()
            .map_err(prepend("data:"))?.clone();

        let columns = public_arguments.get("columns")
            .ok_or::<Error>("columns: missing".into())?.deref().to_owned().get_arraynd()?.clone();

        match (data_property.value_properties, columns) {
            (Hashmap::Str(value_properties), ArrayND::Str(column_names)) => {
                let all_properties = column_names.iter()
                    .map(|v| value_properties.get(v))
                    .collect::<Option<Vec<&ValueProperties>>>()
                    .ok_or::<Error>("columns: unknown column in index".into())?;

                if all_properties.len() != 1 {
                    return Err("columns: only one column may be selected at this time".into())
                }
                Ok(all_properties.first().unwrap().clone().clone())
            },
            (Hashmap::I64(value_properties), ArrayND::I64(column_indices)) => {
                let all_properties = column_indices.iter()
                    .map(|v| value_properties.get(v))
                    .collect::<Option<Vec<&ValueProperties>>>()
                    .ok_or::<Error>("columns: unknown column in index".into())?;

                if all_properties.len() != 1 {
                    return Err("columns: only one column may be selected at this time".into())
                }
                Ok(all_properties.first().unwrap().clone().clone())
            }
            _ => return Err("columns must be strings or integers".into())
        }
    }

    fn get_names(
        &self,
        _properties: &NodeProperties,
    ) -> Result<Vec<String>> {
        Err("get_names not implemented".into())
    }
}
