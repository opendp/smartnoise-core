use crate::errors::*;

use std::collections::HashMap;


use crate::proto;

use crate::components::{Component};

use crate::base::{Value, NodeProperties, ValueProperties, DataType, Nature, NatureCategorical, Jagged};
use crate::utilities::prepend;

impl Component for proto::Cast {
    // modify min, max, n, categories, is_public, non-null, etc. based on the arguments and component
    fn propagate_property(
        &self,
        _privacy_definition: &proto::PrivacyDefinition,
        public_arguments: &HashMap<String, Value>,
        properties: &NodeProperties,
    ) -> Result<ValueProperties> {
        let mut data_property = properties.get("data")
            .ok_or_else(|| Error::from("data: missing"))?.array()
            .map_err(prepend("data:"))?.clone();

        let datatype = public_arguments.get("type")
            .ok_or_else(|| Error::from("type: missing, must be public"))?.first_string()
            .map_err(prepend("type:"))?;

        let prior_datatype = data_property.data_type.clone();

        data_property.data_type = match datatype.to_lowercase().as_str() {
            "float" => DataType::F64,
            "real" => DataType::F64,
            "int" => DataType::I64,
            "integer" => DataType::I64,
            "bool" => DataType::Bool,
            "string" => DataType::Str,
            "str" => DataType::Str,
            _ => bail!("data type is not recognized. Must be one of \"float\", \"int\", \"bool\" or \"string\"")
        };

        let num_columns = data_property.num_columns()?;

        // TODO: It is possible to preserve significantly more properties here
        match data_property.data_type {
            DataType::Bool => {
                // true label must be defined
                public_arguments.get("true_label")
                    .ok_or_else(|| Error::from("true_label: missing, must be public"))?.array()?;

                data_property.nature = Some(Nature::Categorical(NatureCategorical {
                    categories: Jagged::Bool((0..num_columns).map(|_| Some(vec![true, false])).collect())
                }));
                data_property.nullity = false;
            },
            DataType::I64 => {
                // min must be defined, for imputation of values that won't cast
                public_arguments.get("min")
                    .ok_or_else(|| Error::from("min: missing, must be public"))?.first_i64()
                    .map_err(prepend("type:"))?;
                // max must be defined
                public_arguments.get("max")
                    .ok_or_else(|| Error::from("max: missing, must be public"))?.first_i64()
                    .map_err(prepend("type:"))?;
                data_property.nature = None;
                data_property.nullity = false;
            },
            DataType::Str => {
                data_property.nullity = false;
                if prior_datatype != data_property.data_type {
                    data_property.nature = None;
                }
            },
            DataType::F64 => {
                data_property.nature = None;
                data_property.nullity = true;
            }
        };

        Ok(data_property.into())
    }

}
