{-# LANGUAGE RecordWildCards  #-}

{-|

All of the craziness for thread-safety
and asynchronous operations lives here.

The idea is that if someone comes up
with a better way of managing connection
state they can build what they want on top
of the Core module.

Operations are not entirely asynch -
they block until a response is returned.

But we don't hold the connection lock while
we're blocking, so other threads can still
put requests on the connection.

This should work well where each thread needs one
request to do it's job.

If you have a good idea for what an asynchronous
API should look like let me know. It shouldn't be too
hard to add on to what's already here.

-}
module Network.Starling.Connection
    ( Connection
    , open
    , close
    , synchRequest
    , synchRequestMulti
    , ignorantRequest
    ) where

import Network.Starling.Core

import Control.Concurrent (forkIO)
import Control.Concurrent.Chan
import Control.Concurrent.MVar
import Control.Exception (handle)
import Data.IORef

import System.IO

import qualified Data.Binary.Builder as B
import Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BS


-- | For thread safety of operations, we perform
-- all requests on a wrapper around a handle.
data Connection = Conn 
    { con_lock :: MVar ()
    , con_h :: Handle
    , con_q :: Chan QItem
    , con_opaque :: IORef Opaque
    }

-- | The connection keeps a queue of callbacks.
-- These are entries on that queue
data QItem
    = QDone
    | QHandleResp Opaque (Response -> IO ())
    | QHandleMulti Opaque ([Response] -> IO ())

-- | Create a connection.
-- Please don't use the handle after opening a
-- connection with it.
open :: Handle -> IO Connection
open h
    = do
  lock <- newMVar ()
  queue <- newChan
  opaque <- newIORef 0
  forkIO $ flip handle (readLoop h queue) $ \StarlingReadError ->
      return ()
  return $ Conn lock h queue opaque
           
-- | Process the callback queue
readLoop :: Handle -> Chan QItem -> IO ()
readLoop h q
    = do
  response <- getResponse h
  tryNextQItem h q response $ readLoop h q

tryNextQItem :: Handle -> Chan QItem -> Response -> IO () -> IO ()
tryNextQItem h q response k
    = do
  qItem <- readChan q
 
  case compareOpaque response qItem of
    KeepQ -> unGetChan q qItem >> k
    KeepR -> tryNextQItem h q response k
    Match -> processResponse h q response qItem k
    Done  -> return ()

-- | Until we have an implementation of RFC 1982, we
-- never return 'KeepR'
compareOpaque :: Response -> QItem -> CompareResult
compareOpaque response qItem
    = let qIdentm = qOpaque qItem
          rIdent = rsOpaque response
      in case qIdentm of
           Nothing -> Done
           Just qIdent ->
            case compare qIdent rIdent of
              EQ -> Match
              LT -> KeepQ
              GT -> KeepQ

qOpaque QDone = Nothing
qOpaque (QHandleResp ident _) = Just ident
qOpaque (QHandleMulti ident _) = Just ident

data CompareResult
    = KeepQ
    | KeepR
    | Match
    | Done

processResponse :: Handle -> Chan QItem
                -> Response -> QItem -> IO () -> IO ()
processResponse h q response qItem k
    = case qItem of
        QHandleResp _ f -> f response >> k
        QHandleMulti ident f -> do
                   (resps, left) <- takeResponses ident h
                   f (response : resps)
                   tryNextQItem h q left k

-- | Take many responses off of the queue as long as they
-- match the passed in senquence. The second returned
-- value is the response which did not match.
takeResponses :: Opaque -> Handle -> IO ([Response], Response)
takeResponses ident h
    = go []
 where go xs = do
         response <- getResponse h
         if rsOpaque response == ident
          then go (response:xs)
          else return (reverse xs, response)

withConLock :: Connection -> IO a -> IO a
withConLock Conn{..} k
    = withMVar_ con_lock k

withMVar_ :: MVar a -> IO b -> IO b
withMVar_ mvar f = withMVar mvar $ const f


-- | This function ignores anything coming back from
-- the server.
-- Non-blocking.
ignorantRequest :: Connection -> Request -> IO ()
ignorantRequest conn req
    = withConLock conn $
      putRequest conn req >>= \_ -> return ()
      -- don't enqueue response handler

-- | Place a synchronous request which only returns
-- one reply
synchRequest :: Connection -> Request -> IO Response
synchRequest conn req
    = do
  result <- newEmptyMVar
  withConLock conn $ do
    opaque <- putRequest conn req
    enqueue conn $ QHandleResp opaque $ putMVar result
  readMVar result

-- | Place a synchronous request which may return
-- multiple response ('Stat', pretty much)
synchRequestMulti :: Connection -> Request -> IO [Response]
synchRequestMulti conn req
    = do
  result <- newEmptyMVar
  withConLock conn $ do
    opaque <- putRequest conn req
    enqueue conn $ QHandleMulti opaque $ putMVar result
    _ <- putRequest conn noop
    return ()
  readMVar result

-- | Shut down the connection.
-- Non-blocking.
close :: Connection -> IO ()
close conn
    =
  withConLock conn $ do
    enqueue conn QDone
    _ <- putRequest conn quit
    return ()

-- | Put a request onto the handle
putRequest :: Connection -> Request -> IO Opaque
putRequest conn@Conn{..} req
    = do
  opaque <- nextOpaque conn
  let chunk =  B.toLazyByteString $ serialize $ addOpaque opaque req
  BS.hPut con_h chunk
  return opaque

-- | Grab the next sequence number
nextOpaque :: Connection -> IO Opaque
nextOpaque Conn{..} = do
  current <- readIORef con_opaque
  let next = current + 1
  writeIORef con_opaque next
  return $ next `seq` current

-- | Add an item on the callback queue
enqueue :: Connection -> QItem -> IO ()
enqueue Conn{..} item
    = writeChan con_q item
