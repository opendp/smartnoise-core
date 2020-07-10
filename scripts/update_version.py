import tomlkit
import os

self_dir = os.path.dirname(__file__)
config_path = os.path.join(self_dir, "update_version_config.toml")

with open(config_path, 'r') as config_file:
    config = tomlkit.loads(config_file.read())

VERSION = ".".join([str(config['version'][sem]) for sem in ['major', 'minor', 'patch']])

if "core" in config['paths']:

    def update_validator(manifest):
        manifest['package']['version'] = VERSION
        return manifest

    def update_runtime(manifest):
        manifest['package']['version'] = VERSION
        manifest['dependencies']['whitenoise_validator']['version'] = VERSION
        return manifest

    def update_ffi(manifest):
        manifest['package']['version'] = VERSION
        manifest['dependencies']['whitenoise_validator']['version'] = VERSION
        manifest['dependencies']['whitenoise_runtime']['version'] = VERSION
        return manifest

    crates = {
        "validator-rust": update_validator,
        "runtime-rust": update_runtime,
        "ffi-rust": update_ffi
    }

    # update version references in all three crates
    for project in crates:
        manifest_path = os.path.join(config['paths']['core'], project, "Cargo.toml")
        with open(manifest_path, 'r') as manifest_file:
            manifest = tomlkit.loads(manifest_file.read())

        manifest = crates[project](manifest)

        with open(manifest_path, 'w') as runtime_toml_file:
            runtime_toml_file.write(tomlkit.dumps(manifest))



if "python" in config['paths']:
    # update version number in setup.cfg
    import configparser
    setup_path = os.path.join(config['paths']['python'], "setup.cfg")
    setup = configparser.ConfigParser(comment_prefixes='/', allow_no_value=True)
    setup.read(setup_path)
    setup['metadata']['version'] = VERSION

    with open(setup_path, 'w') as setup_file:
        setup.write(setup_file)

    # update version number in documentation
    doc_builder_path = os.path.join(config['paths']['python'], "scripts", "build_docs.sh")
    with open(doc_builder_path, "r") as doc_builder_file:
        lines = doc_builder_file.readlines()
    lines[next(i for i, l in enumerate(lines) if l.startswith("WN_VERSION="))] = f"WN_VERSION={VERSION}"
    with open(doc_builder_path, "w") as doc_builder_file:
        doc_builder_file.writelines(lines)

if "R" in config['paths']:
    # update DESCRIPTION file
    description_path = os.path.join(config['paths']['R'], "DESCRIPTION")
    with open(description_path, 'r') as description_file:
        lines = description_file.readlines()
    lines[next(i for i, l in enumerate(lines) if l.startswith("Version: "))] = f"Version: {VERSION}\n"
    with open(description_path, "w") as doc_builder_file:
        doc_builder_file.writelines(lines)
