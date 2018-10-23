package zrz.rocksdb;

import java.util.OptionalInt;

public interface JRocksIterator<TypeT> extends JRocksBaseIterator {

  TypeT entry();

}
