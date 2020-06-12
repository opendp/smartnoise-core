use crate::errors::*;

use crate::{proto, base, Warnable};

use crate::components::{Component, Named};
use crate::base::{Value, ValueProperties, IndexmapProperties, ArrayProperties, DataType};
use ndarray::prelude::*;
use indexmap::map::IndexMap;

impl Component for proto::Materialize {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        public_arguments: &IndexMap<base::IndexKey, Value>,
        _properties: &base::NodeProperties,
        node_id: u32
    ) -> Result<Warnable<ValueProperties>> {

        let column_names = self.get_names(public_arguments, &IndexMap::new(), None)?;

        Ok(ValueProperties::Indexmap(IndexmapProperties {
            children: column_names.into_iter()
                .map(|name| (name.into(), ValueProperties::Array(ArrayProperties {
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
                    group_id: vec![]
                }))).collect(),
            variant: proto::indexmap_properties::Variant::Dataframe
        }).into())
    }
}

impl Named for proto::Materialize {
    fn get_names(
        &self,
        public_arguments: &IndexMap<base::IndexKey, Value>,
        _argument_variables: &IndexMap<base::IndexKey, Vec<String>>,
        _release: Option<&Value>
    ) -> Result<Vec<String>> {

        let column_names = public_arguments.get::<base::IndexKey>(&"column_names".into())
            .and_then(|column_names| column_names.array().ok()?.string().ok()).cloned();
        let num_columns = public_arguments.get::<base::IndexKey>(&"num_columns".into())
            .and_then(|num_columns| num_columns.array().ok()?.first_i64().ok());

        // standardize to vec of column names
        Ok(match (column_names, num_columns) {
            (Some(column_names), None) => column_names.into_dimensionality::<Ix1>()?.to_vec(),
            (None, Some(num_columns)) => (0..num_columns).map(|idx| idx.to_string()).collect(),
            _ => return Err("either column_names or num_columns must be specified".into())
        })
    }
}
