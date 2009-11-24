{-# LANGUAGE OverloadedStrings  #-}


import System.IO
import Network.Starling
import Network

import Data.ByteString.Char8


openUnix :: String -> IO Connection
openUnix socket
    = do
  h <- connectTo socket $ UnixSocket socket
  hSetBuffering h NoBuffering
  open h

