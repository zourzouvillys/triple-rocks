package zrz.jrocks;

import org.rocksdb.RocksIterator;

/**
 * a logical column family.
 * 
 * @author theo
 *
 */

public interface JRocksColumnFamily {

  int get(JRocksReadableWriter ctx, byte[] key, byte[] value);

  JRocksKeyValueIterator<byte[], byte[]> newIterator(JRocksReadableWriter ctx, byte[] prefix);

  JRocksKeyValueIterator<byte[], byte[]> newIterator(JRocksReadableWriter ctx);

  void delete(JRocksWriter ctx, byte[] key);

  void put(JRocksWriter ctx, byte[] key, byte[] value);

}
