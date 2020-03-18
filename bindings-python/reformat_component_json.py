import json
import os


components_dir = '/home/shoe/PSI/whitenoise/prototypes/components'

for component_name in os.listdir(components_dir):
    component_path = os.path.join(components_dir, component_name)

    with open(component_path, 'r') as component_file:
        component = json.load(component_file)

    def remove_stuff(argument):
        if 'constraints' in argument:
            del argument['constraints']
        if 'of' in argument:
            del argument['of']


    for argument in component['arguments']:
        remove_stuff(component['arguments'][argument])

    remove_stuff(component['return'])

    with open(component_path, 'w') as component_file:
        json.dump(component, component_file, indent=2)


