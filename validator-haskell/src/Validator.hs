{-# LANGUAGE ForeignFunctionInterface #-}
module Validator where
 
import Foreign.C.Types
import Foreign.C.String

increment :: CInt -> IO CInt
f1 x = do
    return (x + 1)

foreign export ccall
    increment :: CInt -> IO CInt
