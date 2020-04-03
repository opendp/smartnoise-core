use crate::errors::*;

use std::collections::HashMap;


use crate::{proto, base};
use crate::hashmap;
use crate::components::{Component, Expandable};

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

        let prior_datatype = data_property.data_type.clone();

        data_property.data_type = match self.r#type.to_lowercase().as_str() {
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

macro_rules! make_expandable {
    ($variant:ident, $var_type:expr) => {
        impl Expandable for proto::$variant {
            fn expand_component(
                &self,
                _privacy_definition: &proto::PrivacyDefinition,
                component: &proto::Component,
                _properties: &base::NodeProperties,
                component_id: &u32,
                _maximum_id: &u32,
            ) -> Result<proto::ComponentExpansion> {
                Ok(proto::ComponentExpansion {
                    computation_graph: hashmap![component_id.clone() => proto::Component {
                        arguments: component.arguments.clone(),
                        variant: Some(proto::component::Variant::from(proto::Cast {
                            r#type: $var_type
                        })),
                        omit: false,
                        batch: component.batch,
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
