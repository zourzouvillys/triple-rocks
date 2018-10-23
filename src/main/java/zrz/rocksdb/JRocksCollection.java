package zrz.rocksdb;

public interface JRocksCollection {

  JRocksBaseIterator createIterator(JRocksReadableWriter ctx);

}
