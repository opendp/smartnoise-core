use whitenoise_validator::errors::*;
use whitenoise_validator::components::Named;

use crate::NodeArguments;
use whitenoise_validator::base::{Value, ReleaseNode, IndexKey};
use indexmap::IndexMap;
use crate::components::Evaluable;

use whitenoise_validator::{proto};

impl Evaluable for proto::Materialize {
    fn evaluate(&self, _privacy_definition: &Option<proto::PrivacyDefinition>, arguments: NodeArguments) -> Result<ReleaseNode> {

        let column_names = self.get_names(
            arguments.iter().map(|(k, v)| (k.clone(), v)).collect(),
            IndexMap::new(), None)?;

        // num columns is sufficient shared information to build the dataframes
        let num_columns = column_names.len();

        let mut response = (0..num_columns)
            .map(|_| Vec::new())
            .collect::<Vec<Vec<String>>>();

        let mut reader = match csv::ReaderBuilder::new()
            .has_headers(self.skip_row)
            .from_path(self.file_path.clone()) {
            Ok(reader) => reader,
            Err(_) => return Err("provided file path could not be found".into())
        };

        // parse from csv into response
        reader.deserialize().try_for_each(|result: std::result::Result<Vec<String>, _>| {

            // parse each record into the whitenoise internal format
            match result {
                Ok(record) => record.into_iter().enumerate()
                    .filter(|(idx, _)| idx < &num_columns)
                    .for_each(|(idx, value)| response[idx].push(value)),
                Err(e) => return Err(format!("{:?}", e).into())
            };
            Ok::<_, Error>(())
        })?;

        let num_nonempty_columns = response.iter()
            .filter(|col| !col.is_empty()).count();

        if 0 < num_nonempty_columns && num_nonempty_columns < num_columns {
            (num_nonempty_columns..num_columns).for_each(|idx|
                response[idx] = (0..response[0].len()).map(|_| "".to_string()).collect::<Vec<String>>())
        }

        Ok(ReleaseNode::new(Value::Dataframe(column_names.into_iter()
            .zip(response.into_iter())
            .map(|(key, value): (IndexKey, Vec<String>)|
                (key, ndarray::Array::from(value).into_dyn().into()))
            .collect::<IndexMap<IndexKey, Value>>())))
    }
}
