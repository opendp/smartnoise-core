use whitenoise_validator::errors::*;

use ndarray::prelude::*;
use crate::NodeArguments;
use whitenoise_validator::base::{Value, Hashmap, ReleaseNode};
use indexmap::IndexMap;
use crate::components::Evaluable;
use ndarray;
use whitenoise_validator::proto;

impl Evaluable for proto::Materialize {
    fn evaluate(&self, arguments: &NodeArguments) -> Result<ReleaseNode> {
        let column_names = arguments.get("column_names")
            .and_then(|column_names| column_names.array().ok()?.string().ok()).cloned();

        let num_columns = arguments.get("num_columns")
            .and_then(|num_columns| num_columns.first_i64().ok());

        // num columns is sufficient shared information to build the dataframes
        let num_columns = match (column_names.clone(), num_columns) {
            (Some(column_names), None) => match column_names.into_dimensionality::<Ix1>() {
                Ok(column_names) => column_names,
                Err(_) => return Err("column names must be one-dimensional".into())
            }.to_vec().len(),
            (None, Some(num_columns)) => num_columns as usize,
            _ => return Err("either column_names or num_columns must be provided".into())
        };

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
        reader.deserialize().map(|result: std::result::Result<Vec<String>, _>| {

            // parse each record into the whitenoise internal format
            match result {
                Ok(record) => record.into_iter().enumerate()
                    .filter(|(idx, _)| idx < &num_columns)
                    .for_each(|(idx, value)| response[idx].push(value)),
                Err(e) => return Err(format!("{:?}", e).into())
            };
            Ok(())
        }).collect::<Result<()>>()?;

        let num_nonempty_columns = response.iter()
            .filter(|col| col.len() > 0).count();

        if 0 < num_nonempty_columns && num_nonempty_columns < num_columns {
            (num_nonempty_columns..num_columns).for_each(|idx|
                response[idx] = (0..response[0].len()).map(|_| "".to_string()).collect::<Vec<String>>())
        }

        match column_names {
            Some(column_names) => {
                let column_names = column_names.into_dimensionality::<Ix1>()?.to_vec();
                // convert hashmap of vecs into arrays
                Ok(ReleaseNode::new(Value::Hashmap(Hashmap::Str(response.into_iter().enumerate()
                    .map(|(k, v): (usize, Vec<String>)|
                        (column_names[k].clone(), ndarray::Array::from(v).into_dyn().into()))
                    .collect::<IndexMap<String, Value>>()))))
            }
            None => {

                // convert hashmap of vecs into arrays
                Ok(ReleaseNode::new(Value::Hashmap(Hashmap::I64(response.into_iter().enumerate()
                    .map(|(k, v): (usize, Vec<String>)|
                        (k as i64, ndarray::Array::from(v).into_dyn().into()))
                    .collect::<IndexMap<i64, Value>>()))))
            }
        }
    }
}
