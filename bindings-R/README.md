## R Install
1. RProtoBuf requires the protobuf libraries to be installed
    <!-- Documentation from https://github.com/eddelbuettel/rprotobuf -->
    On Ubuntu use:
    
    <!-- Running this installs protobuf 3.0.0, which is not supported -->
    <!-- sudo apt-get install protobuf-compiler libprotobuf-dev libprotoc-dev -->
        https://askubuntu.com/a/1072684

2. Install R packages


    Rscript -e 'install.packages(c("RProtoBuf", "Rcpp"))' 

