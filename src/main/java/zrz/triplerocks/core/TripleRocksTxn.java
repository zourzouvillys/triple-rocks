package zrz.triplerocks.core;

import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteBatchInterface;
import org.rocksdb.WriteBatchWithIndex;
import org.rocksdb.WriteOptions;

import com.google.common.base.Preconditions;

public class TripleRocksTxn implements TripleRocksAPI {

  private final AbstractRocksTripleStore store;
  private Snapshot snapshot;

  // use writeBatch() instead of accessing this directly.
  private WriteBatchWithIndex _wb;

  private TripleRocksTxn(final AbstractRocksTripleStore store) {
    this.store = store;
  }

  public Snapshot snapshot() {
    return this.snapshot;
  }

  void begin() {
    Preconditions.checkState(this.snapshot == null);
    this.snapshot = this.store.snapshot();
  }

  public void abort() {
    if (this._wb != null) {
      this._wb.close();
    }
    this.cleanup();
  }

  public void commit() {
    try {
      if (this._wb != null) {
        try (WriteOptions opts = new WriteOptions()) {
          this.store.db.write(opts, this._wb);
        }
        catch (final RocksDBException e) {
          throw new RuntimeException(e);
        }
      }
    }
    finally {
      this.cleanup();
    }
  }

  private void cleanup() {
    if (this.snapshot != null) {
      this.store.release(this.snapshot);
      this.snapshot.close();
    }
  }

  /**
   * create a snapshot immediately.
   *
   * @param store
   * @return
   */

  static TripleRocksTxn begin(final AbstractRocksTripleStore store) {
    final TripleRocksTxn txn = new TripleRocksTxn(store);
    txn.begin();
    return txn;
  }

  // the implementations.

  /**
   *
   */

  private WriteBatchInterface writeBatch() {
    if (this._wb == null) {
      // note: overwrite_key must be true to allow NewIteratorWithBase
      this._wb = new WriteBatchWithIndex(true);
    }
    return this._wb;
  }

  @Override
  public void insert(final byte[] s, final byte[] p, final byte[] o) {
    for (final IndexKind i : IndexKind.values()) {
      final byte[] key = i.toKey(s, p, o);
      try {
        this.writeBatch().put(this.store.indexes[i.ordinal()], key, AbstractRocksTripleStore.EMPTY);
      }
      catch (final RocksDBException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public void delete(final byte[] s, final byte[] p, final byte[] o) {
    Preconditions.checkNotNull(s);
    Preconditions.checkNotNull(p);
    Preconditions.checkNotNull(o);
    for (final IndexKind i : IndexKind.values()) {
      final byte[] key = i.toKey(s, p, o);
      try {
        this.writeBatch().delete(this.store.indexes[i.ordinal()], key);
      }
      catch (final RocksDBException e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public RocksIterator createIterator(final IndexKind index) {
    final ColumnFamilyHandle cf = this.store.indexes[index.ordinal()];
    try (ReadOptions opts = new ReadOptions()) {
      opts.setSnapshot(this.snapshot);
      final RocksIterator it = this.store.db.newIterator(cf, opts);
      if (this._wb != null) {
        return this._wb.newIteratorWithBase(cf, it);
      }
      return it;
    }
  }

  @Override
  public boolean contains(final byte[] s, final byte[] p, final byte[] o) {

    final ColumnFamilyHandle cf = this.store.indexes[IndexKind.SPO.ordinal()];

    final byte[] key = MultiKey.create(s, p, o);

    // TODO:TPZ: we should benchmark this. are the cost of 2 JNI calls more in common use cases than a single get?
    // suspect so, hence why it is here...
    try (ReadOptions opts = new ReadOptions()) {

      opts.setSnapshot(this.snapshot);

      if (this._wb != null) {
        return this._wb.getFromBatchAndDB(this.store.db, cf, opts, key) != null;
      }

      // otherwise, try the store.

      if (!this.store.db.keyMayExist(opts, cf, key, null)) {
        return false;
      }

      return this.store.db.get(cf, opts, key) != null;
    }
    catch (final RocksDBException e) {

      throw new RuntimeException(e);

    }

  }

}
