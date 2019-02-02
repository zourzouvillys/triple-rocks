package zrz.rocksdb;

import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;

public class JRocksSnapshot implements AutoCloseable, JRocksReader {

  private static final byte[] EMPTY_BYTES = new byte[0];

  private final RocksDB db;

  private Snapshot snapshot;

  private ReadOptions ro;

  public JRocksSnapshot(RocksDB db) {
    this.db = db;
  }

  public long sequenceNumber() {
    return this.snapshot().getSequenceNumber();
  }

  Snapshot snapshot() {
    if (this.snapshot == null) {
      this.snapshot = db.getSnapshot();
      this.ro = new ReadOptions();
      this.ro.setSnapshot(snapshot());
    }
    return this.snapshot;
  }

  private ReadOptions readOptions() {
    if (this.ro == null) {
      this.snapshot();
    }
    return this.ro;
  }

  public void finalize() {
    if (this.snapshot != null) {
      System.err.println("WARN: closing leaked snapshot with seq " + this.sequenceNumber());
      this.close();
    }
  }

  @Override
  public RocksIterator newIterator(JAttachedColumnFamily cf) {
    return db.newIterator(cf.h, this.readOptions());
  }

  @Override
  public RocksIterator newIterator(JAttachedColumnFamily cf, ReadOptions opts) {
    opts.setSnapshot(snapshot());
    return db.newIterator(cf.h, opts);
  }

  @Override
  public int get(JAttachedColumnFamily cf, byte[] key, byte[] value) {
    try {
      return this.db.get(cf.h, this.readOptions(), key, value);
    }
    catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public boolean exists(JAttachedColumnFamily cf, byte[] key) {
    try {
      return this.db.get(cf.h, this.readOptions(), key, EMPTY_BYTES) != RocksDB.NOT_FOUND;
    }
    catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() {
    if (this.snapshot != null) {
      this.db.releaseSnapshot(this.snapshot);
      this.snapshot = null;
      if (this.ro != null) {
        this.ro.close();
        this.ro = null;
      }
    }
  }

}
