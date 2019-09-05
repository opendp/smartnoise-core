## Bindings-Python

1. build
    Edit the CMakeLists.txt to set the python version the shared object is built against.   
    Open a python interpreter in ./libs and run `import differential_privacy` to access bound methods
2. package
    TODO: attach .so files to distutils.setup via package_data (which includes the .so in the MANIFEST.ini)
