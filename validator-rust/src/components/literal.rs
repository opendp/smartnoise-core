
use crate::errors::*;
use crate::components::{Named, Component};
use crate::utilities::array::get_ith_column;
use ndarray::ArrayD;
use crate::{proto, base, Warnable, Float, Integer};
use crate::base::{Value, Array, ValueProperties, ArrayProperties, DataType, IndexKey};
use indexmap::map::IndexMap;

impl Component for proto::Literal {
    fn propagate_property(
        &self,
        _privacy_definition: &Option<proto::PrivacyDefinition>,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        _properties: base::NodeProperties,
        node_id: u32
    ) -> Result<Warnable<ValueProperties>> {
        Ok(ValueProperties::Array(ArrayProperties {
            num_records: None,
            num_columns: None,
            nullity: true,
            releasable: false,
            c_stability: vec![],
            aggregator: None,
            nature: None,
            data_type: DataType::Unknown,
            dataset_id: Some(node_id as i64),
            // this is a library-wide assumption - that datasets initially have more than zero rows
            is_not_empty: true,
            dimensionality: None,
            group_id: vec![],
            naturally_ordered: true,
            sample_proportion: vec![]
        }).into())
    }
}


impl Named for proto::Literal {
    fn get_names(
        &self,
        _public_arguments: IndexMap<base::IndexKey, &Value>,
        _argument_variables: IndexMap<base::IndexKey, Vec<IndexKey>>,
        release: Option<&Value>
    ) -> Result<Vec<IndexKey>> {

        // annoying mini-trait to work around the generic from
        trait ToIndexKey { fn to_index_key(self) -> IndexKey; }
        impl ToIndexKey for Float {
            fn to_index_key(self) -> IndexKey {
                self.to_string().into()
            }
        }
        macro_rules! make_convertable {
            ($var_type:ty) => {
                impl ToIndexKey for $var_type {
                    fn to_index_key(self) -> IndexKey {
                        self.into()
                    }
                }
            }
        }
        make_convertable!(Integer);
        make_convertable!(bool);
        make_convertable!(String);

        fn array_to_names<T: ToString + Clone + Default + ToIndexKey>(array: &ArrayD<T>, num_columns: usize) -> Result<Vec<IndexKey>> {
            (0..num_columns)
                .map(|index| {
                    let array = get_ith_column(array, index)?;
                    match array.ndim() {
                        0 => match array.first() {
                            Some(value) => Ok(value.clone().to_index_key()),
                            None => Err("array may not be empty".into())
                        },
                        1 => Ok("[Literal Column]".into()),
                        _ => Err("array has too great of a dimension".into())
                    }
                })
                .collect()
        }

        match release {
            Some(release) => match release {
                Value::Jagged(jagged) => Ok((0..jagged.num_columns()).map(|_| "[Literal vector]".into()).collect()),
                Value::Array(value) => match value {
                    Array::Float(array) => array_to_names(array, value.num_columns()?),
                    Array::Int(array) => array_to_names(array, value.num_columns()?),
                    Array::Str(array) => array_to_names(array, value.num_columns()?),
                    Array::Bool(array) => array_to_names(array, value.num_columns()?),
                },
                _ => Err("names are only supported for arrays and jagged arrays".into()),  // (other types are not necessary)
            },
            None => Err("Literals must always be accompanied by a release".into())
        }
    }
}



#[cfg(test)]
pub mod test_literal {
    use crate::base::Value;
    use crate::bindings::Analysis;
    use crate::base::test_data::array1d_f64_10_uniform;

    pub fn analysis_literal(value: Value, public: bool) -> (Analysis, u32) {
        let mut analysis = Analysis::new();
        let literal = analysis.literal()
            .value(value).value_public(public)
            .build();
        (analysis, literal)
    }

    #[test]
    fn test_literal() {
        let (analysis, literal) = analysis_literal(array1d_f64_10_uniform(), true);

        analysis.properties(literal).unwrap();
    }
}