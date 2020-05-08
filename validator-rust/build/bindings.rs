extern crate heck;

use crate::ComponentJSON;
use std::path::PathBuf;
use std::fs;
use std::fs::File;
use std::io::Write;
use self::heck::CamelCase;


pub fn build_bindings(
    components: &Vec<ComponentJSON>,
    output_path_impls: PathBuf,
    output_path_builders: PathBuf
) {

    let mut bindings_analysis = Vec::new();
    let mut bindings_builders = Vec::new();

    components.iter().for_each(|component| {
        if component.id == "Map" {
            return
        }

        // GENERATE ANALYSIS BINDINGS
        let positional_args = component.arguments.iter()
            .filter(|(_name, arg)| arg.default_rust.is_none())
            .map(|(name, _meta)| format!("{}: u32", name))
            .collect::<Vec<String>>();
        let positional_opts = component.options.iter()
            .filter_map(|(name, opt)|
                Some((name, if opt.default_rust.is_some() {return None} else {opt.type_rust.as_ref().unwrap()})))
            .map(|(name, opt_type)| format!("{}: {}", name, opt_type))
            .collect::<Vec<String>>();
        let signature = &[vec!["&mut self".to_string()], positional_args, positional_opts.clone()]
            .iter().flatten().cloned().collect::<Vec<String>>().join(", ");

        let argument_insertion = component.arguments.iter()
            .filter(|(_name, arg)| arg.default_rust.is_none())
            .map(|(name, _meta)|
                format!("arguments.insert(String::from(\"{name}\"), {name});", name=name))
            .collect::<Vec<String>>().join("\n        ");

        let option_insertion = component.options.iter()
            .map(|(name, meta)| {
                if meta.default_rust.is_some() {
                    format!("{}: {}", name, meta.default_rust.as_ref().unwrap())
                } else {
                    format!("{}", name)
                }
            })
            .collect::<Vec<String>>().join(",\n                ");

        bindings_analysis.push(format!(r#"
impl Analysis {{
    pub fn {name}({signature}) -> builders::{id}Builder {{
        #[allow(unused_mut)]
        let mut arguments = HashMap::new();
        {argument_insertion}
        let component = proto::Component {{
            variant: Some(proto::component::Variant::{variant}(proto::{id} {{
                {option_insertion}
            }})),
            omit: false,
            batch: self.submission_count,
            arguments,
        }};

        self.component_count += 1;
        self.components.insert(self.component_count, component);
        builders::{id}Builder {{
            id: self.component_count,
            component: self.components.get_mut(&self.component_count).unwrap(),
            release: &mut self.release,
        }}
    }}
}}
"#,
            name=component.name,
            variant=component.name.to_camel_case(),
            id=component.id.to_camel_case(),
            signature=signature,
            argument_insertion=argument_insertion,
            option_insertion=option_insertion
        ));

        // GENERATE BUILDER BINDINGS
        let arg_builders = component.arguments.keys()
            .map(|name| {
                format!(r#"
    /// set the id of the {name} argument from a previous component
    pub fn {name}(self, id: u32) -> Self {{
        self.component.arguments.insert(String::from("{name}"), id);
        self
    }}"#, name=name)
            })
            .collect::<Vec<String>>().join("\n");

        let option_builders = component.options.iter()
            .map(|(name, arg)| {
                format!(r#"
    /// set the field attribute "{name}" directly
    pub fn {name}(self, value: {rust_type}) -> Self {{
        if let Some(proto::component::Variant::{variant}(ref mut variant)) = self.component.variant {{
            variant.{name} = value;
        }}
        self
    }}"#,
                name=name,
                variant=component.name.to_camel_case(),
                rust_type=arg.type_rust.as_ref().unwrap())
            })
            .collect::<Vec<String>>().join("\n");

        bindings_builders.push(format!(r#"
/// Builder interface for [{id}](../../proto/struct.{id}.html)
pub struct {id}Builder<'a> {{
    pub id: u32,
    pub component: &'a mut proto::Component,
    pub release: &'a mut Release
}}

impl<'a> {id}Builder<'a> {{
    {arg_builders}
    {option_builders}
    pub fn value(self, value: Value) -> Self {{
        self.release.insert(self.id.clone(), ReleaseNode::new(value));
        self
    }}

    pub fn enter(self) -> u32 {{
        self.id
    }}
}}
"#,
           id=component.id.to_camel_case(),
           arg_builders=arg_builders,
           option_builders=option_builders
        ));

    });

    let bindings_builders_text = format!(r#"
use crate::proto;
use crate::base::{{Release, Value, ReleaseNode}};

{}"#, bindings_builders.join("\n"));

    let bindings_analysis_text = bindings_analysis.join("\n");

    {
        fs::remove_file(output_path_impls.clone()).ok();
        let mut file = File::create(output_path_impls).unwrap();
        file.write(bindings_analysis_text.as_bytes())
            .expect("Unable to write bindings impls file.");
        file.flush().unwrap();
    }

    {
        fs::remove_file(output_path_builders.clone()).ok();
        let mut file = File::create(output_path_builders).unwrap();
        file.write(bindings_builders_text.as_bytes())
            .expect("Unable to write bindings builders file.");
        file.flush().unwrap();
    }
}