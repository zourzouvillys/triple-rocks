package zrz.jrocks;

import org.rocksdb.ReadOptions;
import org.rocksdb.RocksIterator;

public interface JRocksReader {

  RocksIterator newIterator(JAttachedColumnFamily cf);

  int get(JAttachedColumnFamily cf, byte[] key, byte[] value);

  RocksIterator newIterator(JAttachedColumnFamily cf, ReadOptions opts);

}
