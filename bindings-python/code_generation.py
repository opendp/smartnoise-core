import json
import os

if os.name != 'nt':
    # auto-update the protos
    import subprocess

    # protoc must be installed and on path
    package_dir = os.path.join(os.getcwd(), 'yarrow')
    subprocess.call(f"protoc --python_out={package_dir} *.proto", shell=True, cwd=os.path.abspath('../prototypes/'))
    subprocess.call(f"sed -i -E 's/^import.*_pb2/from . \\0/' *.py", shell=True, cwd=package_dir)

if os.name == 'nt' and not os.path.exists('yarrow/api_pb2.py'):
    print('make sure to run protoc to generate python proto bindings, and fix package imports to be relative to yarrow')

components_dir = os.path.abspath("../prototypes/components")

generated_code = "from . import Component\n\n"
for file_name in os.listdir(components_dir):
    component_path = os.path.join(components_dir, file_name)
    with open(component_path, 'r') as component_schema_file:
        component_schema = json.load(component_schema_file)
        signature_arguments = ", ".join(
            list(component_schema['arguments'].keys()) + list(component_schema['options'].keys()))

        component_arguments = "{\n            "\
                              + ",\n            ".join([f"'{name}': Component.of({name})"
                                                      for name in component_schema['arguments']
                                                      if name != "**kwargs"]) \
                              + "\n        }"
        if "**kwargs" in component_schema['arguments']:
            component_arguments = f"{{**kwargs, **{component_arguments}}}"

        component_options = "{\n            " \
                            + ",\n            ".join([f"'{name}': {name}"
                                                    for name in component_schema['options']
                                                    if name != "**kwargs"]) \
                            + "\n        }"
        if "**kwargs" in component_schema['options']:
            component_options = f"{{**kwargs, **{component_options}}}"

        generated_code += f"""
def {component_schema['name']}({signature_arguments}):
    \"\"\"{component_schema.get("description", component_schema["name"] + " step")}\"\"\"
    return Component(
        "{component_schema['id']}", 
        arguments={component_arguments}, 
        options={component_options})

"""

with open(os.path.join('.', 'yarrow', 'components.py'), 'w') as generated_file:
    generated_file.write(generated_code)
