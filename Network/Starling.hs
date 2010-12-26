{-# LANGUAGE DeriveDataTypeable,
      FlexibleContexts
      #-}

{-|

Module: Network.Starling
Copyright: Antoine Latter 2009-2010
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
    , StarlingError(..)
    , ResponseStatus(..)
    , set
    , get
    , delete
    , add
    , replace
    , update
    , increment
    , decrement
    , flush
    , stats
    -- , oneStat -- doesn't seem to work for me
    , version
    , listAuthMechanisms
    , auth
    , AuthMechanism
    , AuthData
    , AuthCallback(..)
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
    , listAuthMechanisms
    , startAuth
    , stepAuth
    )
import qualified Network.Starling.Core as Core

import Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BS
import qualified Data.ByteString.Lazy.Char8 as BS8
import qualified Data.Binary.Get as B

import Data.Word
import Data.Typeable
import Control.Exception (Exception(..))

import Control.Monad.IO.Class (liftIO, MonadIO)

import Control.Failure

-- | An error consists of the error code returned by the
-- server and a human-readble error string returned by the server.
data StarlingError = StarlingError ResponseStatus ByteString
 deriving Typeable

instance Show StarlingError where
    show (StarlingError err str)
        = "StarlingError: " ++ show err ++ " " ++ show (BS8.unpack str)

instance Exception StarlingError

-- | Set a value in the cache
set :: (MonadIO m, Failure StarlingError m)
       => Connection -> Key -> Value -> m ()
set con key value = simpleRequest con (Core.set key value) (const ())

-- | Set a vlue in the cache. Fails if a value is already defined
-- for the indicated key.
add :: (MonadIO m, Failure StarlingError m) =>
       Connection -> Key -> Value -> m ()
add con key value = simpleRequest con (Core.add key value) (const ())

-- | Set a value in the cache. Fails if a value is not already defined
-- for the indicated key.
replace :: (MonadIO m, Failure StarlingError m) =>
           Connection -> Key -> Value -> m ()
replace con key value = simpleRequest con (Core.replace key value) (const ())

-- | Retrive a value from the cache
get :: (MonadIO m, Failure StarlingError m) =>
       Connection -> Key -> m ByteString
get con key = simpleRequest con (Core.get key) rsBody

-- | Delete an entry in the cache
delete :: (MonadIO m, Failure StarlingError m) =>
          Connection -> Key -> m ()
delete con key = simpleRequest con (Core.delete key) (const ())

-- | Update a value in the cache. This operation requires two round trips.
-- This operation can fail if the key is not present in the cache, or if
-- the value changes in the cache between the two calls.
-- So watch out! Even if the value exists the operation might
-- not go through in the face of concurrent access.
--
-- Testing indicates that if we fail because we could not gaurantee
-- atomicity the failure code will be 'KeyExists'.
update :: (MonadIO m, Failure StarlingError m) =>
          Connection -> Key -> (Value -> m (Maybe Value)) -> m ()
update con key f
    = do
  response <- liftIO $ synchRequest con (Core.get key)
  case rsStatus response of
    NoError -> do
        let oldVal = rsBody response
            cas = rsCas response
        res <- f oldVal
        let baseRequest = case res of
                            Nothing -> Core.delete key
                            Just newVal -> Core.replace key newVal
            request = addCAS cas $ baseRequest
        simpleRequest con request (const ())
    _ -> errorResult response

-- | Increment a value in the cache. The first 'Word64' argument is the
-- amount by which to increment and the second is the intial value to
-- use if the key does not yet have a value.
-- The return value is the updated value in the cache.
increment :: (MonadIO m, Failure StarlingError m) =>
             Connection -> Key -> Word64 -> Word64 -> m Word64
increment con key amount init
    = simpleRequest con (Core.increment key amount init) $ \response ->
      B.runGet B.getWord64be (rsBody response)

-- | Decrement a value in the cache. The first 'Word64' argument is the
-- amount by which to decrement and the second is the intial value to
-- use if the key does not yet have a value.
-- The return value is the updated value in the cache.
decrement :: (MonadIO m, Failure StarlingError m) =>
             Connection -> Key -> Word64 -> Word64 -> m Word64
decrement con key amount init
    = simpleRequest con (Core.decrement key amount init) $ \response ->
      B.runGet B.getWord64be (rsBody response)

-- | Delete all entries in the cache
flush :: (MonadIO m, Failure StarlingError m) =>
         Connection -> m ()
flush con = simpleRequest con Core.flush (const ())

simpleRequest :: (MonadIO m, Failure StarlingError m) =>
                 Connection -> Request -> (Response -> a) -> m a
simpleRequest con req f
    = do
  response <- liftIO $ synchRequest con req
  if rsStatus response == NoError
   then return . f $ response
   else errorResult response

errorResult response = failure $ StarlingError (rsStatus response) (rsBody response)

-- | Returns a list of stats about the server in key,value pairs
stats :: (MonadIO m, Failure StarlingError m) =>
         Connection -> m [(ByteString,ByteString)]
stats con
    = do
  resps <- liftIO $ synchRequestMulti con $ Core.stat Nothing
  if null resps then error "fatal error in Network.Starling.stats"
   else do
     let resp = head resps
     if rsStatus resp == NoError
      then return . unpackStats $ resps
      else errorResult resp

 where

   unpackStats
       = filter (\(x,y) -> not (BS.null x && BS.null y)) .
         map (\response -> (rsKey response, rsBody response))

-- | Returns a single stat. Example: 'stat con "pid"' will return
-- the 
oneStat :: (MonadIO m, Failure StarlingError m) =>
           Connection -> Key -> m ByteString
oneStat con key = simpleRequest con (Core.stat $ Just key) rsBody

-- | List allowed SASL mechanisms. The server must support SASL
-- authentication.
listAuthMechanisms :: (MonadIO m, Failure StarlingError m) =>
                      Connection -> m [AuthMechanism]
listAuthMechanisms con
    = simpleRequest con Core.listAuthMechanisms (BS8.words . rsBody)

-- | Some authentications require mutliple back and forths between the
-- client and the server. This type encapsulates that.
data AuthCallback m = AuthCallback (ByteString -> m (AuthData, Maybe (AuthCallback m)))

-- | SASL authenitcation. Multi-step authentication is supported by un-folding
-- the passed-in AuthCallback. Returns 'True' if authentication is supported
-- and complete. If the supplied callback completes while there are still steps
-- remaining we throw FurtherAuthRequired.
auth :: (MonadIO m, Failure StarlingError m) =>
        Connection -> AuthMechanism -> AuthData -> Maybe (AuthCallback m) -> m Bool
auth con mech auth authCallback
    = auth' Core.startAuth con mech auth authCallback

auth' req con mech auth authCallback = do
  response <- liftIO $ synchRequest con $ req mech auth
  case rsStatus response of
    NoError -> return True
    AuthRequired -> return False
    FurtherAuthRequired
        -> do
      case authCallback of
        Nothing -> errorResult response
        Just (AuthCallback f) -> do
                           next <- f (rsBody response)
                           case next of
                             (newAuth, newCallback)
                                     -> auth' Core.stepAuth con mech newAuth newCallback
    _ -> errorResult response
        

-- | Returns the version of the server
version :: (MonadIO m, Failure StarlingError m) =>
           Connection -> m ByteString
version con = simpleRequest con Core.version rsBody

