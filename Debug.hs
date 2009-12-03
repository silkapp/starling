{-# LANGUAGE OverloadedStrings  #-}


import System.IO
import Network.Starling
import Network

import Data.ByteString.Lazy.Char8()
import qualified Data.ByteString.Lazy as BS

import Control.Concurrent

openUnix :: String -> IO Connection
openUnix socket
    = do
  h <- connectTo socket $ UnixSocket socket
  hSetBuffering h NoBuffering
  open h

forkExec :: IO a -> IO (IO a)
forkExec k
    = do
  result <- newEmptyMVar
  _ <- forkIO $ k >>= putMVar result
  return (takeMVar result)

updateFn :: Value -> IO (Maybe Value)
updateFn val = do
  threadDelay $ 5 * floor 10e4 -- pause!
  return Nothing
  -- return . return $ "updated:" `BS.append` val

-- Test that should cause the update to fail,
-- as the update function has a thread-delay in it.
-- While we're in the middle of the update we change
-- the value of the key.
testUpdate :: Connection -> IO ()
testUpdate con
    = do
  _ <- delete con "key"
  putStrLn "Deleted key"

  set con "key" "value"
  putStrLn "Set 'key' to 'value'"

  updateResSus <- forkExec $ update con "key" updateFn
  putStrLn "forked update of 'key', waiting for a bit..."

  threadDelay $ floor 10e4

  set con "key" "value2"
  putStrLn "set 'value2'"

  result <- get con "key"
  putStr "Get: "
  print result

  putStrLn "Waiting for update ..."
  result <- updateResSus
  putStr "Update result: "
  print result

  result <- get con "key"
  putStr "Get: "
  print result
