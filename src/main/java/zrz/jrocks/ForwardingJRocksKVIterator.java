package zrz.jrocks;

public class ForwardingJRocksKVIterator<K, V> implements JRocksKeyValueIterator<K, V> {

  private final JRocksKeyValueIterator<K, V> it;

  public ForwardingJRocksKVIterator(JRocksKeyValueIterator<K, V> it) {
    this.it = it;
  }

  @Override
  public void seekToFirst() {
    it.seekToFirst();
  }

  @Override
  public void seek(K target) {
    it.seek(target);
  }

  @Override
  public void seekForPrev(K target) {
    it.seekForPrev(target);
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
  public K key() {
    return it.key();
  }

  @Override
  public V value() {
    return it.value();
  }

}
