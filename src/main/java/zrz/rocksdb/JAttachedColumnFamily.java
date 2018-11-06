package zrz.rocksdb;

import java.util.Arrays;

import org.apache.jena.ext.com.google.common.base.Verify;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;

/**
 * a column family which is attached to the {@link JRocksEngine} instance.
 * 
 * @author theo
 *
 */
public class JAttachedColumnFamily implements JRocksColumnFamily {

  ColumnFamilyHandle h;
  private JRocksEngine engine;
  private byte[] name;

  public JAttachedColumnFamily(JRocksEngine engine, ColumnFamilyHandle h) {
    this.engine = engine;
    this.h = h;
    try {
      byte[] name = h.getName();
      this.name = Arrays.copyOf(name, name.length);
    }
    catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
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
    return new RocksIteratorAdapter(Verify.verifyNotNull(ctx, "ctx").newIterator(this));
  }

  @Override
  public JRocksKeyValueIterator<byte[], byte[]> newIterator(JRocksReadableWriter ctx, byte[] prefix) {
    // TODO: add. (currently only used for preficx, but suspect useful some time).
    throw new UnsupportedOperationException();
  }

  @Override
  public void delete(JRocksWriter ctx, byte[] key) {
    ctx.delete(this, key);
  }

  @Override
  public void put(JRocksWriter ctx, byte[] key, byte[] value) {
    ctx.put(this, key, value);
  }

  public String toString() {
    return "CF[" + new String(this.name) + "]";
  }

}
