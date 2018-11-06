package zrz.rocksdb;

import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;

public class JRocksBatch implements AutoCloseable, JRocksWriter {

  WriteBatch batch;

  public JRocksBatch() {
    this.batch = new WriteBatch();
  }

  public JRocksBatch(WriteBatch batch) {
    this.batch = batch;
  }

  @Override
  public void close() {
    if (this.batch == null)
      return;
    this.batch.close();
    this.batch = null;
  }

  @Override
  public void put(JAttachedColumnFamily h, byte[] key, byte[] value) {
    try {
      batch.put(h.h, key, value);
    }
    catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(JAttachedColumnFamily h, byte[] key) {
    try {
      batch.delete(h.h, key);
    }
    catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

}
