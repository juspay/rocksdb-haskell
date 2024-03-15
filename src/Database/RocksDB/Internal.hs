{-# LANGUAGE RecordWildCards #-}
-- |
-- Module      : Database.RocksDB.Internal
-- Copyright   : (c) 2012-2013 The leveldb-haskell Authors
--               (c) 2014-2020 The rocksdb-haskell Authors
-- License     : BSD3
-- Maintainer  : jprupp@protonmail.ch
-- Stability   : experimental
-- Portability : non-portable
--

module Database.RocksDB.Internal
    ( Config (..)
    , DB (..)

    -- * Smart constructors & extractors
    , createOptions
    , destroyOptions
    , createReadOpts
    , destroyReadOpts
    , createWriteOpts
    , destroyWriteOpts
    , withOptions
    , withOptionsCF
    , withReadOpts
    , withWriteOpts

    -- * Utilities
    , freeCString
    , throwIfErr
    , cSizeToInt
    , intToCSize
    , intToCInt
    , cIntToInt
    , boolToNum
    ) where

import           Control.Monad
import           Data.Default
import           Database.RocksDB.C
import           UnliftIO
import           UnliftIO.Foreign

type DisableWAL = Bool

data DB = DB { rocksDB        :: !RocksDB
             , columnFamilies :: ![ColumnFamily]
             , readOpts       :: !ReadOpts
             , writeOpts      :: !WriteOpts
             }

data Config = Config { createIfMissing :: !Bool
                     , errorIfExists   :: !Bool
                     , paranoidChecks  :: !Bool
                     , maxFiles        :: !(Maybe Int)
                     , prefixLength    :: !(Maybe Int)
                     , bloomFilter     :: !Bool
                     , disableWAL      :: !Bool
                     , writeBufferSize :: !(Maybe Int)
                     , maxWriteBufferNumber :: !(Maybe Int)
                     , minWriteBufferNumberToMerge :: !(Maybe Int)
                     , dbWriteBufferSize :: !(Maybe Int)
                     , maxWriteBufferSizeToMaintain :: !(Maybe Int)
                     } deriving (Eq, Show)

instance Default Config where
    def = Config { createIfMissing  = False
                 , errorIfExists    = False
                 , paranoidChecks   = False
                 , maxFiles         = Nothing
                 , prefixLength     = Nothing
                 , bloomFilter      = False
                 , disableWAL       = False
                 , writeBufferSize  = Nothing
                 , maxWriteBufferNumber = Nothing
                 , minWriteBufferNumberToMerge = Nothing
                 , dbWriteBufferSize = Nothing
                 , maxWriteBufferSizeToMaintain = Nothing
                 }

destroyOptions :: MonadUnliftIO m => Options -> m ()
destroyOptions = liftIO . c_rocksdb_options_destroy

createOptions :: MonadUnliftIO m => Config -> m Options
createOptions Config {..} = do
    opts <- liftIO c_rocksdb_options_create
    liftIO $ do
        when bloomFilter $ do
            fp <- c_rocksdb_filterpolicy_create_bloom_full 10
            bo <- c_rocksdb_block_based_options_create
            c_rocksdb_block_based_options_set_filter_policy bo fp
            c_rocksdb_options_set_block_based_table_factory opts bo
        forM_ prefixLength $ \l -> do
            t <- c_rocksdb_slicetransform_create_fixed_prefix (intToCSize l)
            c_rocksdb_options_set_prefix_extractor opts t
        forM_ maxFiles $
            c_rocksdb_options_set_max_open_files opts . intToCInt
        c_rocksdb_options_set_create_if_missing
            opts (boolToCBool createIfMissing)
        c_rocksdb_options_set_error_if_exists
            opts (boolToCBool errorIfExists)
        c_rocksdb_options_set_paranoid_checks
            opts (boolToCBool paranoidChecks)
    pure opts

withOptions :: MonadUnliftIO m => Config -> (Options -> m a) -> m a
withOptions Config {..} f = with_opts $ \opts -> do
    liftIO $ do
        when bloomFilter $ do
            fp <- c_rocksdb_filterpolicy_create_bloom_full 10
            bo <- c_rocksdb_block_based_options_create
            c_rocksdb_block_based_options_set_filter_policy bo fp
            c_rocksdb_options_set_block_based_table_factory opts bo
        forM_ prefixLength $ \l -> do
            t <- c_rocksdb_slicetransform_create_fixed_prefix (intToCSize l)
            c_rocksdb_options_set_prefix_extractor opts t
        forM_ maxFiles $
            c_rocksdb_options_set_max_open_files opts . intToCInt
        buffer_size_opts opts
        max_write_buffer_number_opts opts
        min_write_buffer_number_to_merge_opts opts
        db_buffer_size_opts opts
        max_write_buffer_size_to_maintain opts
        c_rocksdb_options_set_create_if_missing
            opts (boolToCBool createIfMissing)
        c_rocksdb_options_set_error_if_exists
            opts (boolToCBool errorIfExists)
        c_rocksdb_options_set_paranoid_checks
            opts (boolToCBool paranoidChecks)
    f opts
  where
    with_opts =
        bracket
        (liftIO c_rocksdb_options_create)
        (liftIO . c_rocksdb_options_destroy)
    buffer_size_opts opts = 
        case writeBufferSize of
            Nothing -> return ()
            Just size -> c_rocksdb_options_set_max_buffer_size opts (intToCSize size)
    max_write_buffer_number_opts opts = 
        case maxWriteBufferNumber of
            Nothing -> return ()
            Just num -> c_rocksdb_options_set_max_write_buffer_number opts (intToCInt num)
    min_write_buffer_number_to_merge_opts opts = 
        case minWriteBufferNumberToMerge of
            Nothing -> return ()
            Just num -> c_rocksdb_options_set_min_write_buffer_number_to_merge opts (intToCInt num)
    db_buffer_size_opts opts = 
        case dbWriteBufferSize of
            Nothing -> return ()
            Just size -> c_rocksdb_options_set_db_write_buffer_size opts (intToCSize size)
    max_write_buffer_size_to_maintain opts = 
        case maxWriteBufferSizeToMaintain of
            Nothing -> return ()
            Just num -> c_rocksdb_options_set_max_write_buffer_size_to_maintain opts (intToCInt num)

withOptionsCF :: MonadUnliftIO m => [Config] -> ([Options] -> m a) -> m a
withOptionsCF cfgs f =
    go [] cfgs
  where
    go acc [] = f (reverse acc)
    go acc (c:cs) = withOptions c $ \o -> go (o:acc) cs

destroyReadOpts :: MonadUnliftIO m => ReadOpts -> m ()
destroyReadOpts = liftIO . c_rocksdb_readoptions_destroy

createReadOpts :: MonadUnliftIO m => Maybe Snapshot -> m ReadOpts
createReadOpts maybe_snap_ptr = create_read_opts
  where
    create_read_opts = liftIO $ do
        read_opts_ptr <- c_rocksdb_readoptions_create
        forM_ maybe_snap_ptr $ c_rocksdb_readoptions_set_snapshot read_opts_ptr
        return read_opts_ptr

withReadOpts :: MonadUnliftIO m => Maybe Snapshot -> (ReadOpts -> m a) -> m a
withReadOpts maybe_snap_ptr =
    bracket
    create_read_opts
    (liftIO . c_rocksdb_readoptions_destroy)
  where
    create_read_opts = liftIO $ do
        read_opts_ptr <- c_rocksdb_readoptions_create
        forM_ maybe_snap_ptr $ c_rocksdb_readoptions_set_snapshot read_opts_ptr
        return read_opts_ptr

destroyWriteOpts :: MonadUnliftIO m => WriteOpts -> m ()
destroyWriteOpts = liftIO . c_rocksdb_writeoptions_destroy

createWriteOpts :: MonadUnliftIO m => DisableWAL -> m WriteOpts
createWriteOpts disableWAL = createWriteOpts
  where
    createWriteOpts = liftIO $ do
      write_opts_ptr <- c_rocksdb_writeoptions_create
      when disableWAL $
        c_rocksdb_writeoptions_disable_WAL  write_opts_ptr (toEnum 1)
      return write_opts_ptr

withWriteOpts :: MonadUnliftIO m => DisableWAL -> (WriteOpts -> m a) -> m a
withWriteOpts disableWAL =
    bracket
    createWriteOpts
    (liftIO . c_rocksdb_writeoptions_destroy)
  where
    createWriteOpts = liftIO $ do
      write_opts_ptr <- c_rocksdb_writeoptions_create
      when disableWAL $
        c_rocksdb_writeoptions_disable_WAL  write_opts_ptr (toEnum 1)
      return write_opts_ptr

freeCString :: CString -> IO ()
freeCString = c_rocksdb_free

throwIfErr :: MonadUnliftIO m => String -> (ErrPtr -> m a) -> m a
throwIfErr s f = alloca $ \err_ptr -> do
    liftIO $ poke err_ptr nullPtr
    res  <- f err_ptr
    err_cstr <- liftIO $ peek err_ptr
    when (err_cstr /= nullPtr) $ do
        err <- liftIO $ peekCString err_cstr
        liftIO $ free err_cstr
        throwIO $ userError $ s ++ ": " ++ err
    return res

boolToCBool :: Bool -> CBool
boolToCBool True  = 1
boolToCBool False = 0
{-# INLINE boolToCBool #-}

cSizeToInt :: CSize -> Int
cSizeToInt = fromIntegral
{-# INLINE cSizeToInt #-}

intToCSize :: Int -> CSize
intToCSize = fromIntegral
{-# INLINE intToCSize #-}

intToCInt :: Int -> CInt
intToCInt = fromIntegral
{-# INLINE intToCInt #-}

cIntToInt :: CInt -> Int
cIntToInt = fromIntegral
{-# INLINE cIntToInt #-}

boolToNum :: Num b => Bool -> b
boolToNum True  = fromIntegral (1 :: Int)
boolToNum False = fromIntegral (0 :: Int)
{-# INLINE boolToNum #-}
