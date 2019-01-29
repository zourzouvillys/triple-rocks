package zrz.rocksdb;

import org.rocksdb.RocksIterator;

public interface JRocksReader {

  RocksIterator newIterator(JAttachedColumnFamily cf);

  int get(JAttachedColumnFamily cf, byte[] key, byte[] value);

}
