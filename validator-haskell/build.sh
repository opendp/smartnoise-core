ghc -dynamic -shared -fPIC -optc '-DMODULE=Validator' src/Validator.hs src/module_init.c -o libdifferential_privacy.so
rm  src/*.hi src/*.h src/*.o

cabal build --ghc-option="-dynamic -shared -fPIC -optc '-DMODULE=Validator' -o libdifferential_privacy.so"