# (re)installs dependencies using conan package manager

for project in validator-c++ tests runtime-eigen;
do
  cd $project/
  mkdir -p build && cd build && conan install .. && cd ..
  cd ..
done
