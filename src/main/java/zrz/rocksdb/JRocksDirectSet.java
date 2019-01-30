package zrz.rocksdb;

import org.apache.jena.ext.com.google.common.base.Verify;
import org.rocksdb.RocksIterator;

public class JRocksDirectSet<T> implements JRocksSet<T> {

  private static final byte[] EMPTY_BYTES = new byte[0];

  private final JRocksMapper<T> mapper;

  private JRocksColumnFamily cf;

  public JRocksDirectSet(JRocksMapper<T> mapper, JRocksColumnFamily cf) {
    this.mapper = mapper;
    this.cf = cf;
  }

  @Override
  public boolean contains(JRocksReadableWriter ctx, T key) {
    return cf.get(ctx, mapper.toByteArray(key), EMPTY_BYTES) >= 0;
  }

  @Override
  public void put(JRocksWriter ctx, T key) {
    Verify.verifyNotNull(key, "key");
    cf.put(ctx, mapper.toByteArray(key), EMPTY_BYTES);

  }

  @Override
  public void delete(JRocksWriter ctx, T key) {
    cf.delete(ctx, mapper.toByteArray(key));
  }

  @Override
  public JRocksIterator<T> createIterator(JRocksReadableWriter ctx) {

    Verify.verifyNotNull(ctx, "ctx");
    
    return new JRocksIterator<T>() {

      JRocksKeyValueIterator<byte[], byte[]> it = cf.newIterator(ctx);

      @Override
      public T entry() {
        if (!it.isValid())
          return null;
        return mapper.parseFrom(it.key());
      }

      @Override
      public void close() {
        if (it != null) {
          it.close();
          it = null;
        }
      }

      @Override
      public void seekToFirst() {
        it.seekToFirst();
      }

      @Override
      public boolean isValid() {
        if (it == null)
          return false;
        return it.isValid();
      }

      @Override
      public void next() {
        it.next();
      }

    };

  }

  public JRocksColumnFamily columnFamily() {
    return this.cf;
  }

}
