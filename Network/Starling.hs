

{-|

Module: Network.Starling
Copyright: Antoine Latter 2009
Maintainer: Antoine Latter <aslatter@gmail.com>

A haskell implementation of the memcahed
protocol.

This implements the new binary protocol, so
it only works with memcached version 1.3 and newer.

Example of usage, using the network package to obain
a handle, and the OverloadedStrings language extension:

> h <- connectTo "filename" $ UnixSocket "filename"
> hSetBuffering h NoBuffering
> con <- open h

> set con "hello" "world"
> get con "hello"

In the above example we connect to a unix socket in the file \"filename\",
set the key \"hello\" to the value \"world\" and then retrieve the value.

Operations are thread safe - multiple threads of execution may make
concurrent requests on the memcahced connection.

Operations are blocking, but do not block other concurrent threads
from placing requests on the connection.

-}
module Network.Starling
    ( open
    , close
    , Connection
    , Key
    , Value
    , Result
    , ResponseStatus(..)
    , set
    , get
    , delete
    -- , add
    -- , replace
    -- , increment
    -- , decrement
    , flush
    , stats
    -- , oneStat -- doesn't seem to work for me
    , version
    ) where

import Network.Starling.Connection
import Network.Starling.Core hiding
    ( get
    , set
    , delete
    , add
    , replace
    , increment
    , decrement
    , flush
    , stat
    , version
    , quit
    )
import qualified Network.Starling.Core as Core

import Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BS

type Result a = IO (Either (ResponseStatus, ByteString) a)

-- | Set a value in the cache
set :: Connection -> Key -> Value -> Result ()
set con key value = simpleRequest con (Core.set key value) (const ())

-- | Retrive a value from the cache
get :: Connection -> Key -> Result ByteString
get con key = simpleRequest con (Core.get key) rsBody

-- | Delete an entry in the cache
delete :: Connection -> Key -> Result ()
delete con key = simpleRequest con (Core.delete key) (const ())

-- | Delete all entries in the cache
flush :: Connection -> Result ()
flush con = simpleRequest con Core.flush (const ())

simpleRequest :: Connection -> Request -> (Response -> a) -> Result a
simpleRequest con req f
    = do
  response <- synchRequest con req
  if rsStatus response == NoError
   then return . Right . f $ response
   else return . errorResult $ response

errorResult response = Left (rsStatus response, rsBody response)

-- | Returns a list of stats about the server in key,value pairs
stats :: Connection -> Result [(ByteString,ByteString)]
stats con
    = do
  resps <- synchRequestMulti con $ Core.stat Nothing
  if null resps then error "fatal error in Network.Starling.stats"
   else do
     let resp = head resps
     if rsStatus resp == NoError
      then return . Right . unpackStats $ resps
      else return $ errorResult resp
 where unpackStats = filter (\(x,y) -> not (BS.null x && BS.null y)) .
                     map (\response -> (rsKey response, rsBody response))

-- | Returns a single stat. Example: 'stat con "pid"' will return
-- the 
oneStat :: Connection -> Key -> Result ByteString
oneStat con key = simpleRequest con (Core.stat $ Just key) rsBody

-- | Returns the version of the server
version :: Connection -> Result ByteString
version con = simpleRequest con Core.version rsBody

