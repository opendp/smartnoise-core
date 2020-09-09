use crate::errors::*;

use crate::{proto, base, Warnable};

use crate::components::{Component, Named};
use crate::base::{Value, ValueProperties, ArrayProperties, DataType, IndexKey, DataframeProperties};
use indexmap::map::IndexMap;

impl Component for proto::Materialize {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: IndexMap<base::IndexKey, &Value>,
        _properties: base::NodeProperties,
        node_id: u32
    ) -> Result<Warnable<ValueProperties>> {

        let column_names = self.get_names(public_arguments, IndexMap::new(), None)?;

        Ok(ValueProperties::Dataframe(DataframeProperties {
            children: column_names.into_iter()
                .map(|name| (name, ValueProperties::Array(ArrayProperties {
                    num_records: None,
                    num_columns: Some(1),
                    nullity: true,
                    releasable: self.public,
                    c_stability: vec![1.],
                    aggregator: None,
                    nature: None,
                    data_type: DataType::Str,
                    dataset_id: Some(node_id as i64),
                    // this is a library-wide assumption - that datasets initially have more than zero rows
                    is_not_empty: true,
                    dimensionality: Some(1),
                    group_id: vec![],
                    naturally_ordered: true
                }))).collect(),
        }).into())
    }
}

impl Named for proto::Materialize {
    fn get_names(
        &self,
        public_arguments: IndexMap<base::IndexKey, &Value>,
        _argument_variables: IndexMap<base::IndexKey, Vec<IndexKey>>,
        _release: Option<&Value>
    ) -> Result<Vec<IndexKey>> {

        let column_names = public_arguments.get::<base::IndexKey>(&"column_names".into())
            .and_then(|column_names| column_names.ref_array().ok());
        let num_columns = public_arguments.get::<base::IndexKey>(&"num_columns".into())
            .and_then(|num_columns| num_columns.ref_array().ok()?.first_int().ok());

        // standardize to vec of column names
        Ok(match (column_names, num_columns) {
            (Some(column_names), None) => match column_names {
                base::Array::Int(keys) => {
                    if keys.ndim() > 1 {
                        return Err("column_names: dimensionality may not be greater than one".into())
                    }
                    keys.iter().copied().map(IndexKey::from).collect()
                },
                base::Array::Bool(keys) => {
                    if keys.ndim() > 1 {
                        return Err("column_names: dimensionality may not be greater than one".into())
                    }
                    keys.into_iter().copied().map(IndexKey::from).collect()
                },
                base::Array::Str(keys) => {
                    if keys.ndim() > 1 {
                        return Err("column_names: dimensionality may not be greater than one".into())
                    }
                    keys.iter().map(|v| v.as_str().into()).collect()
                },
                _ => return Err("names: unhashable type".into())
            },
            (None, Some(num_columns)) => (0..num_columns).map(|idx| idx.into()).collect(),
            _ => return Err("either column_names or num_columns must be specified".into())
        })
    }
}
