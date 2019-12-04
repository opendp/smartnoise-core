
1. install Rust

2. turn on nightly channel

        rustup override set nightly

2. run the command (every code change)
        
        cargo +nightly build --release

NOTE: Do not modify api.h. The file is automatically generated.