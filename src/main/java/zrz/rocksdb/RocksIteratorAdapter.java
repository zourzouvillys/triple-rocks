package zrz.rocksdb;

import org.rocksdb.RocksIterator;

public class RocksIteratorAdapter implements JRocksKeyValueIterator<byte[], byte[]> {

  private RocksIterator it;
  private byte[] prefix;

  public RocksIteratorAdapter(RocksIterator it) {
    this(it, null);
  }

  public RocksIteratorAdapter(RocksIterator it, byte[] prefix) {
    this.it = it;
    this.prefix = prefix;
  }

  @Override
  public void seek(byte[] target) {
    it.seek(target);
  }

  @Override
  public void seekForPrev(byte[] target) {
    it.seekForPrev(target);
  }

  @Override
  public void seekToFirst() {
    it.seekToFirst();
  }

  @Override
  public void close() {
    it.close();
  }

  @Override
  public boolean isValid() {
    return it.isValid();
  }

  @Override
  public void next() {
    it.next();
  }

  @Override
  public byte[] key() {
    return it.key();
  }

  @Override
  public byte[] value() {
    return it.value();
  }

}
