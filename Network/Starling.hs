

{-| A haskell implementation of the memcahed
  protocol.

-}
module Network.Starling
    ( open
    , Connection
    , set
    , get
    , delete
    -- , add
    -- , replace
    -- , increment
    -- , decrement
    , flush
    , stats
    , version
    -- , quit
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
    , quit
    )
import qualified Network.Starling.Core as Core

import Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BS

type Result a = IO (Either (ResponseStatus, ByteString) a)

set :: Connection -> Key -> Value -> Result ()
set con key value
    = do
  response <- synchRequest con $ Core.set key value
  if rsStatus response == Core.NoError
   then return (Right ())
   else return $ errorResult response

errorResult response = Left (Core.rsStatus response, Core.rsBody response)

get :: Connection -> Key -> Result ByteString
get con key
    = do
  response <- synchRequest con $ Core.get key
  if rsStatus response == Core.NoError
   then return (Right $ Core.rsBody response)
   else return $ errorResult response

delete :: Connection -> Key -> Result ()
delete con key
    = do
  response <- synchRequest con $ Core.delete key
  if rsStatus response == Core.NoError
   then return (Right ())
   else return $ errorResult response

flush :: Connection -> Result ()
flush con
    = do
  response <- synchRequest con $ Core.flush
  if Core.rsStatus response == Core.NoError
   then return (Right ())
   else return $ errorResult response

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
