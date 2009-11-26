{-# LANGUAGE
  RecordWildCards
  , DeriveDataTypeable
 #-}

{-|

Primitives for the memcached protocol.

-}
module Network.Starling.Core
    ( Request
    , requestOp
    , Key
    , Value
    , set
    , add
    , replace
    , get
    , increment
    , decrement
    , append
    , prepend
    , delete
    , quit
    , flush
    , noop
    , version
    , stat
    , addOpaque
    , addCAS
    , Response(..)
    , getResponse
    , StarlingReadError(..)
    , Serialize(..)
    , Deserialize(..)
    , Opaque
    , OpCode(..)
    , DataType(..)
    , CAS
    , nullCAS
    , ResponseStatus(..)
    ) where

import System.IO

import Control.Applicative ((<$>))
import Control.Exception
import Data.Maybe (fromMaybe)
import Data.Typeable
import Data.Word
import Data.Monoid (mconcat)

import Data.ByteString.Lazy (ByteString)
import qualified Data.ByteString.Lazy as BS

import qualified Data.Binary.Builder as B
import qualified Data.Binary.Get as B

type Opaque = Word32
type Key = ByteString
type Value = ByteString
type ErrorInfo = ByteString

-- | Set a value in the cache.
set :: Key -> Value -> Request
set key value
    = let extras = setExtras 0 0
      in request Set extras key value

-- | Add a value to cache. Fails if
-- already present.
add :: Key -> Value -> Request
add key value
    = let extras = setExtras 0 0
      in request Add extras key value

-- | Replaces a value in cahce. Fails if
-- not present.
replace :: Key -> Value -> Request
replace key value
    = let extras = setExtras 0 0
      in request Replace extras key value

setExtras :: Word32 -> Word32 -> ByteString
setExtras flags expiry
    = B.toLazyByteString $ mconcat
      [ B.putWord32be flags
      , B.putWord32be expiry
      ]

-- | Get a value from cache
get :: Key -> Request
get key = request Get BS.empty key BS.empty

increment :: Key
          -> Word64 -- ^ amount
          -> Word64 -- ^ initial value
          -> Request
increment key amount init
    = let extras = incExtras amount init 0
      in request Increment extras key BS.empty

decrement :: Key
          -> Word64 -- ^ amount
          -> Word64 -- ^ initial value
          -> Request
decrement key amount init
    = let extras = incExtras amount init 0
      in request Decrement extras key BS.empty

incExtras :: Word64 -> Word64 -> Word32 -> ByteString
incExtras amount init expiry
    = B.toLazyByteString $ mconcat
      [ B.putWord64be amount
      , B.putWord64be init
      , B.putWord32be expiry
      ]

-- | Delete a cache entry
delete :: Key -> Request
delete key
    = request Delete BS.empty key BS.empty

-- | Quit
quit :: Request
quit = request Quit BS.empty BS.empty BS.empty

-- | Flush the cache
flush :: Request
flush
    = let extras = B.toLazyByteString $ B.putWord32be 0 -- expiry
      in request Flush extras BS.empty BS.empty

-- | Keepalive. Flushes responses for quiet operations.
noop :: Request
noop = request NoOp BS.empty BS.empty BS.empty

-- | Returns the server version
version :: Request
version = request Version BS.empty BS.empty BS.empty

-- | Appends the value to the value in the cache
append :: Key -> Value -> Request
append key value
    = request Append BS.empty key value

-- | Prepends the value to the value in the cache
prepend :: Key -> Value -> Request
prepend key value
    = request Prepend BS.empty key value

-- | Fetch statistics about the cahce. Returns a sequence
-- of responses.
stat :: Maybe Key -> Request
stat mkey = request Stat BS.empty (fromMaybe BS.empty mkey) BS.empty

-- | Add an opaque marker to a request.
-- This is returned unchanged in the corresponding
-- response.
addOpaque :: Opaque -> Request -> Request
addOpaque n req = req { rqOpaque = n }

-- | Add a version tag to a request. When
-- added to a set/replace request, the request
-- will fail if the data has been modified since
-- the CAS was retrieved for the item.
addCAS :: CAS -> Request -> Request
addCAS n req = req { rqCas = n }

class Serialize a where
    serialize :: a -> B.Builder

class Deserialize a where
    deserialize :: B.Get a

data Request
    = Req
      { rqMagic    :: RqMagic
      , rqOp       :: OpCode
      , rqDataType :: DataType
      , rqOpaque   :: Opaque
      , rqCas      :: CAS
      , rqExtras   :: ByteString
      , rqKey      :: ByteString
      , rqBody     :: ByteString
      }
 deriving (Eq, Ord, Read, Show)

instance Serialize Request where
    serialize Req{..} =
        let keyLen = BS.length rqKey
            extraLen = BS.length rqExtras
            bodyLen = BS.length rqBody
        in mconcat
            [ serialize            rqMagic
            , serialize            rqOp
            , B.putWord16be        (fromIntegral keyLen)
            , B.singleton           (fromIntegral extraLen)
            , serialize            rqDataType
            , B.putWord16be        0 -- reserved
            , B.putWord32be        (fromIntegral $ keyLen + extraLen + bodyLen)
            , B.putWord32be        rqOpaque
            , serialize            rqCas
            , B.fromLazyByteString rqExtras
            , B.fromLazyByteString rqKey
            , B.fromLazyByteString rqBody
            ]

-- | A starter request with fields set to reasonable
-- defaults. The opcode field is left undefined.
baseRequest :: Request
baseRequest
    = Req { rqMagic = Request
          , rqKey = BS.empty
          , rqExtras = BS.empty
          , rqDataType = RawData
          , rqBody = BS.empty
          , rqOpaque = 0
          , rqCas = nullCAS
          }

-- | Returns the operation the request will perform
requestOp :: Request -> OpCode
requestOp = rqOp

request :: OpCode
        -> BS.ByteString  -- ^ Extras
        -> BS.ByteString  -- ^ Key
        -> BS.ByteString  -- ^ Body
        -> Request
request opCode extras key body
    = let extraLen = fromIntegral (BS.length extras)
          keyLen = fromIntegral (BS.length key)
      in baseRequest
          { rqOp     = opCode
          , rqExtras = extras
          , rqKey    = key
          , rqBody   = body
          }

newtype CAS = CAS Word64
 deriving (Eq, Ord, Read, Show)

instance Serialize CAS where
    serialize (CAS n)
        = B.putWord64be n

instance Deserialize CAS where
    deserialize = CAS <$> B.getWord64be

nullCAS :: CAS
nullCAS = CAS 0

data Response
    = Res
      { rsMagic  :: RsMagic
      , rsOp     :: OpCode
      , rsDataType :: DataType
      , rsStatus :: ResponseStatus
      , rsOpaque :: Opaque
      , rsCas :: CAS
      , rsExtras :: ByteString
      , rsKey :: ByteString
      , rsBody :: ByteString
      }
 deriving (Eq, Ord, Read, Show)

instance Deserialize Response where
    deserialize = do
      rsMagic <- deserialize
      rsOp <- deserialize
      rsKeyLen <- B.getWord16be
      rsExtraLen <- B.getWord8
      rsDataType <- deserialize
      rsStatus <- deserialize
      rsTotalLen <- B.getWord32be
      let totalLen = fromIntegral rsTotalLen
          keyLen   = fromIntegral rsKeyLen
          extraLen = fromIntegral rsExtraLen
      rsOpaque <- B.getWord32be
      rsCas <- deserialize
      rsExtras <- B.getLazyByteString extraLen
      rsKey <- B.getLazyByteString keyLen
      rsBody <- B.getLazyByteString (totalLen - extraLen - keyLen)
      return Res{..}

newtype ResponseHeader
    = ResHead
      { rsHeadTotalLen :: Word32 }

instance Deserialize ResponseHeader where
    deserialize = do
      _ <- B.getBytes 8
      rsHeadTotalLen <- B.getWord32be
      _ <- B.getBytes 12
      return ResHead{..}

-- | Pulls a reponse to an operation
-- off of a handle.
-- May throw a 'StarlingReadError'
getResponse :: Handle -> IO Response
getResponse h = do
  chunk <- BS.hGet h 24
  if BS.length chunk /= 24 then throw StarlingReadError else do
  let resHeader = B.runGet deserialize chunk
      bodyLen = rsHeadTotalLen resHeader
  rest <- BS.hGet h $ fromIntegral bodyLen
  return . B.runGet deserialize $ chunk `BS.append` rest

data StarlingReadError = StarlingReadError
 deriving (Show, Typeable)

instance Exception StarlingReadError

data RqMagic = Request
 deriving (Eq, Ord, Read, Show)
instance Serialize RqMagic where
    serialize Request = B.singleton 0x80

data RsMagic = Response
 deriving (Eq, Ord, Read, Show)
instance Deserialize RsMagic where
    deserialize = do
      magic <- B.getWord8
      case magic of
        0x81 -> return Response

data DataType = RawData
 deriving (Eq, Ord, Read, Show)
instance Serialize DataType where
    serialize RawData = B.singleton 0x00
instance Deserialize DataType where
    deserialize = do
      dtype <- B.getWord8
      case dtype of
        0x00 -> return RawData

data ResponseStatus
    = NoError
    | KeyNotFound
    | KeyExists
    | ValueTooLarge
    | InvalidArguments
    | ItemNotStored
    | IncrDecrOnNonNumeric
    | UnknownCommand
    | OutOfMemory
 deriving (Eq, Ord, Read, Show)

instance Deserialize ResponseStatus where
    deserialize = do
      status <- B.getWord16be
      return $ case status of
        0x0000 -> NoError
        0x0001 -> KeyNotFound
        0x0002 -> KeyExists
        0x0003 -> ValueTooLarge
        0x0004 -> InvalidArguments
        0x0005 -> ItemNotStored
        0x0006 -> IncrDecrOnNonNumeric
        0x0081 -> UnknownCommand
        0x0082 -> OutOfMemory

data OpCode
    = Get
    | Set
    | Add
    | Replace
    | Delete
    | Increment
    | Decrement
    | Quit
    | Flush
    | GetQ
    | NoOp
    | Version
    | GetK
    | GetKQ
    | Append
    | Prepend
    | Stat
    | SetQ
    | AddQ
    | ReplaceQ
    | DeleteQ
    | IncrementQ
    | DecrementQ
    | QuitQ
    | FlushQ
    | AppendQ
    | PrependQ
 deriving (Eq, Ord, Read, Show)

instance Serialize OpCode where
    serialize Get = B.singleton 0x00
    serialize Set = B.singleton 0x01
    serialize Add = B.singleton 0x02
    serialize Replace = B.singleton 0x03
    serialize Delete = B.singleton 0x04
    serialize Increment = B.singleton 0x05
    serialize Decrement = B.singleton 0x06
    serialize Quit = B.singleton 0x07
    serialize Flush = B.singleton 0x08
    serialize GetQ = B.singleton 0x09
    serialize NoOp = B.singleton 0x0a
    serialize Version = B.singleton 0x0b
    serialize GetK = B.singleton 0x0c
    serialize GetKQ = B.singleton 0x0d
    serialize Append = B.singleton 0x0e
    serialize Prepend = B.singleton 0x0f
    serialize Stat = B.singleton 0x10
    serialize SetQ = B.singleton 0x11
    serialize AddQ = B.singleton 0x12
    serialize ReplaceQ = B.singleton 0x13
    serialize DeleteQ = B.singleton 0x14
    serialize IncrementQ = B.singleton 0x15
    serialize DecrementQ = B.singleton 0x16
    serialize QuitQ = B.singleton 0x17
    serialize FlushQ = B.singleton 0x18
    serialize AppendQ = B.singleton 0x19
    serialize PrependQ = B.singleton 0x1a

instance Deserialize OpCode where
    deserialize = do
      command <- B.getWord8
      return $ case command of
        0x00 -> Get
        0x01 -> Set
        0x02 -> Add
        0x03 -> Replace
        0x04 -> Delete
        0x05 -> Increment
        0x06 -> Decrement
        0x07 -> Quit
        0x08 -> Flush
        0x09 -> GetQ
        0x0a -> NoOp
        0x0b -> Version
        0x0c -> GetK
        0x0d -> GetKQ
        0x0e -> Append
        0x0f -> Prepend
        0x10 -> Stat
        0x11 -> SetQ
        0x12 -> AddQ
        0x13 -> ReplaceQ
        0x14 -> DeleteQ
        0x15 -> IncrementQ
        0x16 -> DecrementQ
        0x17 -> QuitQ
        0x18 -> FlushQ
        0x19 -> AppendQ
        0x20 -> PrependQ

