{-# LANGUAGE ForeignFunctionInterface, ViewPatterns, OverloadedStrings #-}
module Validator (foo) where

import           Proto.Types
import           Proto.Analysis
import           Proto.Analysis_Fields
import           Data.ProtoLens (defMessage, showMessage, encodeMessage, decodeMessage)
import           Lens.Micro
import           Data.ProtoLens.Labels ()
import           Lens.Micro.Extras (view)
import qualified Data.ByteString as B
import qualified Data.ByteString.Unsafe as B
import qualified Data.ByteString.Lazy as L
import           Foreign.C
import           Foreign.Ptr
import           Foreign.Storable
import           Foreign.Marshal.Alloc
import           Foreign.Marshal.Utils
import           System.IO.Error
import           Control.Monad
import           Data.Maybe

definitionExample :: Proto.Types.PrivacyDefinition
definitionExample =
  defMessage


componentExample :: Proto.Analysis.Component
componentExample = defMessage
    & laplace .~ laplaceComponent
  where
    laplaceComponent :: Proto.Analysis.Laplace
    laplaceComponent = defMessage
      & epsilon .~ 0.1


catchAlloc :: IO (Ptr a) -> IO (Ptr a)
catchAlloc = (`catchIOError` const (return nullPtr))

--fromSingleBuffer :: IO (CString, Int) -> L.ByteString

toSingleBuffer :: L.ByteString -> IO (Maybe (CString))
toSingleBuffer (L.uncons -> Nothing) =
    return $ Just nullPtr
toSingleBuffer s = do
    let (fromIntegral -> l) = L.length s
    t <- catchAlloc $ mallocBytes l
    if t /= nullPtr
        then do
            void $ L.foldlChunks
                (\a s -> do
                    off <- a
                    let l = B.length s
                    B.unsafeUseAsCString s $ flip (copyBytes $ plusPtr t off) l
                    return $ off + l
                ) (return 0) s
            return $ Just t
        else return Nothing

unpack :: IO (Maybe (CString)) -> IO (CString)
unpack = (>>= maybe (ioError $ userError "oops") return)

foreign export ccall showProtos :: IO()
showProtos = putStrLn (showMessage componentExample)

foreign export ccall getProto :: IO (CString)
getProto = unpack(toSingleBuffer(L.fromStrict(encodeMessage(componentExample))))

--http://google.github.io/proto-lens/tutorial.html

addNumbers :: Int
addNumbers = 1 + 7

foreign export ccall foo :: Int -> IO Int

foo :: Int -> IO Int
foo n = return (length (f n))

f :: Int -> [Int]
f 0 = []
f n = n:(f (n-1))
