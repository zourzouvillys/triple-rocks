package zrz.rocksdb;

public interface JRocksKeyValueIterator<KeyT, ValueT> extends JRocksBaseIterator {

  KeyT key();

  ValueT value();

  void seek(KeyT target);

  void seekForPrev(KeyT target);

  /**
   * convert to an iterator which maps just the key.
   * 
   * @param mapper
   * @return
   */

  default <T> JRocksIterator<KeyT> toKeyIterator() {

    JRocksKeyValueIterator<KeyT, ValueT> it = this;

    return new JRocksIterator<KeyT>() {

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
      public KeyT entry() {
        return it.key();
      }

    };

  }

}
