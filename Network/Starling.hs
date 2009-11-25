

{-|

Module: Network.Starling
Copyright: Antoine Latter 2009
Maintainer: Antoine Latter <aslatter@gmail.com>

A haskell implementation of the memcahed
protocol.

This implements the new binary protococl, so
it only works with version 1.3 and newer.

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
    , oneStat
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

