package zrz.rocksdb;

/**
 * logical grouping of a set of records.
 * 
 * the underlying implementation may not map directly to a single CF. it may add a prefix to perform other logic to
 * materialize. however the exposed API will keep the same semantics.
 * 
 * @author theo
 *
 */
public interface JRocksMap<KeyT, ValueT> extends JRocksCollection {

  boolean contains(KeyT key);

  ValueT get(KeyT key);

  void put(KeyT key, ValueT value);

  void delete(KeyT key);

  JRocksKeyValueIterator<KeyT, ValueT> createIterator();

}
