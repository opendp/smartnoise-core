# Script notes

## Building a crate

**IMPORTANT NOTE**: This is usually done in conjunction with building a PyPI version. Please look at the [whitenoise-core-python repository](https://github.com/opendifferentialprivacy/whitenoise-core-python), under scripts/README.md) 

The steps below describe how to build releases for crates.io


Notes:
  - All example commands run from the Terminal and start within the `whitenoise-core` directory--the top of this repository.
  - You will also need to have the `whitenoise-core-python` repository. Please see the "**IMPORTANT NOTE**" above.
  - To publish to crates.io, make sure you have credentials for:
      - https://crates.io/crates/whitenoise_runtime
      - https://crates.io/crates/whitenoise_validator

---

1. Edit the file `scripts/update_version.toml`, updating:
    - version numbers
    - paths to this repository as well as the `whitenoise-core-python` repository
    ```
    # vim or your editor of choice
    vim scripts/update_version.toml
    ```
1. Run the `update_version.py` script. 
    ```
    python scripts/update_version.py 
    ```
    This command runs quickly and may generate no output. If it fails, follow the steps in the **IMPORTANT NOTE** above.
1. Test publish the crate
    ```
    cd validator-rust
    cargo publish --dry-run
    ```
1. If the previous step runs without error, publish the crate
    ```
    cd validator-rust
    cargo publish --no-verify
    ```