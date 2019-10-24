{-# LANGUAGE ForeignFunctionInterface, ViewPatterns, OverloadedStrings #-}
module Validator where

import           Proto.Types
import           Proto.Analysis
import           Proto.Analysis_Fields
import           Proto.Release
import           Data.ProtoLens (defMessage, showMessage, encodeMessage, decodeMessageOrDie)
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

-- BEGIN STANDARD API
foreign export ccall validate_analysis :: CString -> Int -> IO (Bool)
validate_analysis analysisBuffer analysisLen = do
  analysisMessage <- B.packCStringLen((analysisBuffer, analysisLen))
  let analysis = decodeMessageOrDie(analysisMessage) :: Proto.Analysis.Analysis
  putStrLn "analysis for validation:"
  putStrLn $ showMessage analysis
  return True

foreign export ccall compute_epsilon :: CString -> Int -> IO (Double)
compute_epsilon analysisBuffer analysisLen = do

  analysisMessage <- B.packCStringLen((analysisBuffer, analysisLen))
  let analysis = decodeMessageOrDie(analysisMessage) :: Proto.Analysis.Analysis
  putStrLn "analysis for computing epsilon:"
  putStrLn $ showMessage analysis
  return 0.1

foreign export ccall generate_report :: CString -> Int -> CString -> Int -> IO CString
generate_report analysisBuffer analysisLen releaseBuffer releaseLen = do
  analysisMessage <- B.packCStringLen((analysisBuffer, analysisLen))
  let analysis = decodeMessageOrDie(analysisMessage) :: Proto.Analysis.Analysis

  releaseBuffer <- B.packCStringLen((releaseBuffer, releaseLen))
  let analysis = decodeMessageOrDie(releaseBuffer) :: Proto.Release.Release

  newCString "{\"key\": \"This is a release in json schema format.\"}"

foreign export ccall free_ptr :: CString -> IO ()
free_ptr ptr = return ()
