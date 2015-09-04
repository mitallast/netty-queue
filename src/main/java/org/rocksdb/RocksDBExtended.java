package org.rocksdb;

public class RocksDBExtended extends RocksDB {

    public void put(final byte[] key, final int keyLength, final byte[] value, final int valueLength) throws RocksDBException {
        put(nativeHandle_, key, keyLength, value, valueLength);
    }

    public void put(final byte[] key, final byte[] value, final int valueLength) throws RocksDBException {
        put(nativeHandle_, key, key.length, value, valueLength);
    }

    public static RocksDBExtended open(final Options options, final String path) throws RocksDBException {

        RocksDBExtended db = new RocksDBExtended();
        db.open(options.nativeHandle_, path);

        db.options_ = options;
        return db;
    }
}
