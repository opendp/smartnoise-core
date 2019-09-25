## R Install
1. The RProtoBuf package has extra dependencies in the protobuf toolchain
    <!-- Documentation from https://github.com/eddelbuettel/rprotobuf -->
    On Ubuntu run:
    
    
    sudo apt-get install protobuf-compiler libprotobuf-dev libprotoc-dev

2. Install R packages


    Rscript -e 'install.packages(c("RProtoBuf", "Rcpp"))' 

