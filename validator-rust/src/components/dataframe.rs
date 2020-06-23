use crate::errors::*;

use crate::{proto, base, Warnable};

use crate::components::{Component, Named};
use crate::base::{Value, ValueProperties, IndexmapProperties, IndexKey};
use crate::utilities::{prepend, get_argument};
use indexmap::map::IndexMap;
use crate::utilities::properties::select_properties;

impl Component for proto::Dataframe {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: &IndexMap<base::IndexKey, &Value>,
        properties: &base::NodeProperties,
        _node_id: u32
    ) -> Result<Warnable<ValueProperties>> {

        let data_property = properties.get::<IndexKey>(&"data".into())
            .ok_or("data: missing")?.array()
            .map_err(prepend("data:"))?;

        if !data_property.releasable {
            data_property.assert_is_not_aggregated()?;
        }

        let column_names = self.get_names(
            public_arguments, &IndexMap::new(), None)?;

        if column_names.len() != data_property.num_columns()? as usize {
            return Err("Column names must be the same length as the number of columns.".into())
        }

        Ok(ValueProperties::Indexmap(IndexmapProperties {
            variant: proto::indexmap_properties::Variant::Dataframe,
            children: column_names.into_iter().enumerate()
                .map(|(idx, name)| Ok((
                    name,
                    select_properties(data_property, idx)?
                )))
                .collect::<Result<IndexMap<IndexKey, ValueProperties>>>()?,
        }).into())
    }
}

impl Named for proto::Dataframe {
    fn get_names(
        &self,
        public_arguments: &IndexMap<base::IndexKey, &Value>,
        _argument_variables: &IndexMap<base::IndexKey, Vec<IndexKey>>,
        _release: Option<&Value>
    ) -> Result<Vec<IndexKey>> {
        Ok(match get_argument(public_arguments, "names")?.array()? {
            base::Array::Str(names) =>
                names.iter().map(|v| IndexKey::from(v.to_string())).collect(),
            base::Array::Bool(names) =>
                names.iter().map(|v| IndexKey::from(*v)).collect(),
            base::Array::Int(names) =>
                names.iter().map(|v| IndexKey::from(*v)).collect(),
            _ => return Err("floats may not be used for column names".into())
        })
    }
}
