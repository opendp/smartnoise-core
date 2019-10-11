//use std::io::Cursor;
//
//use prost::Message;

// Include the `items` module, which is generated from items.proto.
pub mod burdock {
    include!(concat!(env!("OUT_DIR"), "/burdock.rs"));
}
