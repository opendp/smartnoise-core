mod lib;

fn main() {
    println!("Hello, world!");

    let mut analysis = lib::burdock::Analysis::default();
    analysis.definition = Some(lib::burdock::PrivacyDefinition::default());

    let mut buf2 = Vec::new();
    let encoded = prost::Message::encode(&analysis, &mut buf2);
}
