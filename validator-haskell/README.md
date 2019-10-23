# validator-haskell2

## Setup

1. Install stack, a haskell project management tool

2. run the stack setup script from the `validator-haskell` directory:  

        stack setup
        

## Interactive debugging

1. run the glasgow haskell compiler interactively with the stack project's context:  

        stack ghci
        
2. load the Validator library

        :l Validator

    Functions in the Validator module are now available in the interpreter

3. To reload modules after updating code, run:

        :r

This is a useful, brief, opinionated, explanatory article:
https://www.vacationlabs.com/haskell/environment-setup.html#


## Release
To build the project to a shared, dynamically-linked library, ideally run:

        stack build
   
Hpack is used to generate a .cabal build file, and cabal >=2.0 supports building foreign libraries (but hpack does not).  
Instructions for building a foreign library are passed to the generated cabal file via the verbatim argument in the hpack package.yaml.  

Alternatively, you can build a shared library directly from ghc.   

        ghc --make -dynamic -shared src/Validator.hs csrc/DPValidatorWrapper.c -o Validator.so -lHSrts-ghc8.0.2
    
You must modify the HSrts-ghc version to be included in the shared object based on the `ghc --version`. 
Invoking ghc directly may incorrectly handle dependencies or code generation.

When you compile a Haskell module to a shared library, package dependencies are linked dynamically (the package dependencies are not bundled with the library).
Linking them statically means recompiling the dependencies as static libraries. 
Trying to recompile something like the ghc runtime statically isn't really reasonable. 
So using haskell as a validator would require distribution of a set of probably ~10 libraries per OS?

Rust/C++ don't have runtimes, and their dependencies are statically linked, so you get clean distributable libraries.

More information here, and in the linked article:  
https://github.com/haskell/cabal/issues/1688
