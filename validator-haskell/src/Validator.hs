{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE ForeignFunctionInterface #-}
module Validator (foo) where

import Proto.Types
import Proto.Analysis
import Proto.Analysis_Fields
import Data.ProtoLens (defMessage, showMessage)
import Lens.Micro
import           Data.ProtoLens.Labels ()
import           Lens.Micro.Extras (view)

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

foreign export ccall showProtos :: IO()
showProtos = putStrLn (showMessage componentExample)


--http://google.github.io/proto-lens/tutorial.html

addNumbers :: Int
addNumbers = 1 + 7

foreign export ccall foo :: Int -> IO Int

foo :: Int -> IO Int
foo n = return (length (f n))

f :: Int -> [Int]
f 0 = []
f n = n:(f (n-1))
