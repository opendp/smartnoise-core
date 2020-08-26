This directory contains trait implementations for each component. 
- [mod.rs](mod.rs) provides the trait definitions and top-level implementation for the proto::component::Variant enum.
- Each component is a variant of the proto::component::Variant enum and traits are implemented for each variant.
- [transforms.rs](transforms.rs) contains a collection of traits for row-wise transformations.
- The primary purpose of the dp_*.rs files is to expand into smaller components.
