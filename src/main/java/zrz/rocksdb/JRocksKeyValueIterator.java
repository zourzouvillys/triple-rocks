package zrz.rocksdb;

public interface JRocksKeyValueIterator<KeyT, ValueT> extends JRocksBaseIterator {

  KeyT key();

  ValueT value();

  void seek(KeyT target);

  void seekForPrev(KeyT target);

}
