package zrz.rocksdb;

public interface JRocksWriter {

  void put(JAttachedColumnFamily h, byte[] key, byte[] value);

  void delete(JAttachedColumnFamily h, byte[] key);


}
