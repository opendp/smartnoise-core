echo "> Copying runtime package"
rm -r src/runtime-rust
cp -fR ../runtime-rust/ src/runtime-rust
cp -fR ../prototypes/ src/prototypes

echo "> Building and installing package"
(cd .. && R -e "devtools::install('bindings-R')")