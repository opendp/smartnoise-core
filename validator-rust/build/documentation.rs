use std::collections::BTreeSet;
use std::fs;
use std::fs::File;
use std::io::Write;
use std::iter::FromIterator;
use crate::ComponentJSON;
use std::path::PathBuf;
use heck::CamelCase;

pub fn build_documentation(components: &[ComponentJSON], output_path: PathBuf) {
    let component_docs_text_header = r#"/// All of the components available in the library are listed below.
/// The components may be strung together in arbitrary directed graphs (called analyses), and only verifiably DP analyses and data are released.
///
/// | Component ID | Bindings Name | Inputs |
/// |--------------|---------------|--------|  "#.to_string();
    let component_docs_text_table = components.iter()
        .map(|component| {
            let mut inputs = BTreeSet::from_iter(&mut component.arguments.keys());
            inputs.append(&mut BTreeSet::from_iter(&mut component.options.keys()));
            let inputs = inputs.iter()
                .map(|v| format!("`{}`", v))
                .collect::<Vec<String>>().join(", ");

            format!("/// | [{id}](../../proto/struct.{id_link}.html) | {name} | {inputs} |  ",
                    id = component.id,
                    id_link = component.id.to_camel_case(),
                    name = component.name,
                    inputs = inputs)
        })
        .collect::<Vec<String>>().join("\n");

    let component_docs = format!("{}\n{}\npub mod components {{}}", component_docs_text_header, component_docs_text_table);

    {
        // fs::create_dir_all("src/docs/").ok();
        fs::remove_file(output_path.clone()).ok();
        let mut file = File::create(output_path).unwrap();
        file.write_all(component_docs.as_bytes())
            .expect("Unable to write components.rs doc file.");
        file.flush().unwrap();
    }
}