package zrz.triplerocks.core;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.Snapshot;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteBatchWithIndex;
import org.rocksdb.WriteOptions;

public class AbstractRocksTripleStore implements TripleRocksAPI {

  static final byte[] EMPTY = new byte[] {};

  RocksDB db;
  protected final ColumnFamilyHandle[] indexes = new ColumnFamilyHandle[IndexKind.values().length];

  public AbstractRocksTripleStore(final Path path) {

    try {

      final ColumnFamilyDescriptor[] cfd = new ColumnFamilyDescriptor[1 + IndexKind.values().length];

      cfd[0] = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY);

      for (final IndexKind kind : IndexKind.values()) {
        final ColumnFamilyOptions cfo = new ColumnFamilyOptions();
        cfd[1 + kind.ordinal()] = new ColumnFamilyDescriptor(kind.toString().getBytes(), cfo);
      }

      final List<ColumnFamilyHandle> cfh = new ArrayList<>();

      try (final DBOptions opts = new DBOptions()) {
        opts.setCreateIfMissing(true);
        opts.setCreateMissingColumnFamilies(true);
        this.db = RocksDB.open(opts, path.toString(), Arrays.asList(cfd), cfh);
      }

      for (final IndexKind kind : IndexKind.values()) {
        this.indexes[kind.ordinal()] = cfh.get(kind.ordinal() + 1);
      }

    }
    catch (final RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * create a new isolated view which answers all queries with a consistent view at the time it was made.
   *
   * any writes will be added to a {@link WriteBatchWithIndex}, and reads will see the writes made into it. once
   * commited, it will be merged.
   *
   * note that no conflict detection is done. any writes which happened in another transaction will be overwritten. most
   * notably if you perform any logic in the txn that depends on another value, that value may have gone or changed by
   * the time you commited.
   *
   * @return
   */

  public TripleRocksTxn createTransaction() {
    return TripleRocksTxn.begin(this);
  }

  @Override
  @SuppressWarnings("resource")
  public boolean contains(final byte[] s, final byte[] p, final byte[] o) {

    final ColumnFamilyHandle cf = this.indexes[IndexKind.SPO.ordinal()];

    final byte[] key = MultiKey.create(s, p, o);

    // TODO:TPZ: we should benchmark this. are the cost of 2 JNI calls more in common use cases than a single get?
    // suspect so, hence why it is here...

    if (!this.db.keyMayExist(cf, key, null)) {
      return false;
    }

    try {
      return this.db.get(cf, key) != null;
    }
    catch (final RocksDBException e) {
      throw new RuntimeException(e);
    }

  }

  public Snapshot snapshot() {
    return this.db.getSnapshot();
  }

  public void release(final Snapshot snapshot) {
    this.db.releaseSnapshot(snapshot);
  }

  @Override
  public RocksIterator createIterator(final IndexKind index) {
    final ColumnFamilyHandle cf = this.indexes[index.ordinal()];
    try (ReadOptions opts = new ReadOptions()) {
      final RocksIterator it = this.db.newIterator(cf, opts);
      return it;
    }
  }

  @Override
  public void insert(final byte[] s, final byte[] p, final byte[] o) {

    try (final WriteBatch wb = new WriteBatch()) {

      for (final IndexKind i : IndexKind.values()) {
        final byte[] key = i.toKey(s, p, o);
        try {
          wb.put(this.indexes[i.ordinal()], key, EMPTY);
        }
        catch (final RocksDBException e) {
          throw new RuntimeException(e);
        }
      }

      try (WriteOptions wo = new WriteOptions()) {
        this.db.write(wo, wb);
      }

    }
    catch (final RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(final byte[] s, final byte[] p, final byte[] o) {
    try (final WriteBatch wb = new WriteBatch()) {

      for (final IndexKind i : IndexKind.values()) {

        final byte[] key = i.toKey(s, p, o);

        try {
          wb.delete(this.indexes[i.ordinal()], key);
        }
        catch (final RocksDBException e) {
          throw new RuntimeException(e);
        }

      }

      this.db.write(null, wb);

    }
    catch (final RocksDBException e) {
      throw new RuntimeException(e);
    }

  }

  public long latestSequenceNumber() {
    return this.db.getLatestSequenceNumber();
  }

}
