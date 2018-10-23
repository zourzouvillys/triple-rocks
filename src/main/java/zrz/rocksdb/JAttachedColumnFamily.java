package zrz.rocksdb;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatch;

/**
 * a column family which is attached to the {@link JRocksEngine} instance.
 * 
 * @author theo
 *
 */
public class JAttachedColumnFamily implements JRocksColumnFamily {

  ColumnFamilyHandle h;
  private JRocksEngine engine;

  public JAttachedColumnFamily(JRocksEngine engine, ColumnFamilyHandle h) {
    this.engine = engine;
    this.h = h;
  }

  public void compactRange(JRocksEngine engine) {
    try {
      engine.db.compactRange(h);
    }
    catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {
    h.close();
  }

  ///
  /// methods which provide raw access to/from the column family and the underlying storage.
  ///
  ///

  @Override
  public int get(JRocksReadableWriter ctx, byte[] key, byte[] value) {
    return ctx.get(this, key, value);
  }

  @Override
  public JRocksKeyValueIterator<byte[], byte[]> newIterator(JRocksReadableWriter ctx) {
    return new RocksIteratorAdapter(ctx.newIterator(this));
  }

  @Override
  public void delete(JRocksWriter ctx, byte[] key) {
    ctx.delete(this, key);
  }

  @Override
  public void put(JRocksWriter ctx, byte[] key, byte[] value) {
    ctx.put(this, key, value);
  }

}
