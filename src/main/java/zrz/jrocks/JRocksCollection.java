package zrz.jrocks;

public interface JRocksCollection {

  JRocksBaseIterator createIterator(JRocksReadableWriter ctx);

}
