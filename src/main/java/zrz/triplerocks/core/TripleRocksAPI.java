package zrz.triplerocks.core;

import org.rocksdb.RocksIterator;

public interface TripleRocksAPI {

  void insert(byte[] s, byte[] p, byte[] o);

  void delete(byte[] s, byte[] p, byte[] o);

  RocksIterator createIterator(IndexKind index);

  boolean contains(byte[] s, byte[] p, byte[] o);

}
