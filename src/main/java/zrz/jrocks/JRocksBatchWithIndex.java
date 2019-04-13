package zrz.jrocks;

import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.WriteBatchWithIndex;

public class JRocksBatchWithIndex implements JRocksReadableWriter {

  WriteBatchWithIndex batch;
  private JRocksEngine db;

  public JRocksBatchWithIndex() {
    this(new WriteBatchWithIndex(true));
  }

  public JRocksBatchWithIndex(JRocksEngine db) {
    this(new WriteBatchWithIndex(true));
    this.db = db;
  }

  public JRocksBatchWithIndex(WriteBatchWithIndex batch) {
    this.batch = batch;
  }

  /**
   * return a new handle that binds the reader context to the backing engine, so newIterator() and
   * get() will return entries from this but overlaying the db.
   * 
   * @param backing
   * @return
   */

  public JRocksBoundBatchWithIndex bindTo(JRocksEngine backing) {
    return new JRocksBoundBatchWithIndex(this, backing);
  }

  public void clear() {
    this.batch.clear();
  }

  /**
   * the number of updates in this batch
   */

  public long count() {
    return batch.count();
  }

  @Override
  public RocksIterator newIterator(JAttachedColumnFamily cf) {

    if (this.db != null) {
      return batch.newIteratorWithBase(cf.h, db.newIterator(cf));
    }

    throw new IllegalArgumentException("not implemented");
  }

  @Override
  public int get(JAttachedColumnFamily cf, byte[] key, byte[] value) {

    if (this.db != null) {
      try (ReadOptions opts = new ReadOptions()) {
        byte[] res = this.batch.getFromBatchAndDB(this.db.db, cf.h, opts, key);
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

    try (DBOptions opts = new DBOptions()) {
      byte[] res = this.batch.getFromBatch(cf.h, opts, key);
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
  public void put(JAttachedColumnFamily h, byte[] key, byte[] value) {
    try {
      this.batch.put(h.h, key, value);
    }
    catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(JAttachedColumnFamily h, byte[] key) {
    try {
      this.batch.delete(h.h, key);
    }
    catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public void close() {
    this.batch.close();
  }

}
