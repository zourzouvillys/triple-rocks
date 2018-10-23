package zrz.rocksdb;

import org.rocksdb.RocksIterator;

/**
 * interface implemented by logical write/read sources of raw bytes.
 */

public interface JRocksReadableWriter extends JRocksWriter {

  RocksIterator newIterator(JAttachedColumnFamily cf);

  int get(JAttachedColumnFamily cf, byte[] key, byte[] value);

}
