package zrz.rocksdb;

import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;

public class JRocksBoundBatchWithIndex implements JRocksReadableWriter {

  private JRocksBatchWithIndex batch;
  private JRocksEngine base;

  public JRocksBoundBatchWithIndex(JRocksBatchWithIndex batch, JRocksEngine base) {
    this.batch = batch;
    this.base = base;
  }

  @Override
  public RocksIterator newIterator(JAttachedColumnFamily cf) {
    throw new IllegalArgumentException("not implemented");
  }

  @Override
  public int get(JAttachedColumnFamily cf, byte[] key, byte[] value) {
    try (ReadOptions opts = new ReadOptions()) {
      byte[] res = this.batch.batch.getFromBatchAndDB(base.db, cf.h, opts, key);
      if (res == null) {
        return -1;
      }
      System.arraycopy(res, 0, value, 0, Math.min(res.length, value.length));
      return res.length;
    }
    catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(JAttachedColumnFamily cf, byte[] key, byte[] value) {
    batch.put(cf, key, value);
  }

  @Override
  public void delete(JAttachedColumnFamily cf, byte[] key) {
    batch.delete(cf, key);
  }

}
