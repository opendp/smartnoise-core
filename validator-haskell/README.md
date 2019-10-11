1. Install the Haskell platform, (cabal, ghc)


    sudo apt-get install haskell-platform    
    cabal install Cabal cabal-install 

2. Download the package list


    cabal update
        
3. Install validator dependencies (listed in .cabal file)


    cabal install --only-dependencies

4. Build protobuf


    hprotoc -u --prefix=Text -d protoc-gen-haskell -I google-proto-files/ google/protobuf/plugin.proto 

    



## Build a shared library on Linux:


    . build.sh
       
## Build Haskell package:

    
    cabal build