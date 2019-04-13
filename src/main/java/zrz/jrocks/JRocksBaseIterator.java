package zrz.jrocks;

public interface JRocksBaseIterator extends AutoCloseable {

  void seekToFirst();

  void close();

  boolean isValid();

  void next();

}
