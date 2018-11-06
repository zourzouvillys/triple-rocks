package zrz.triplerocks.core;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;

import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.ReadOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.RocksMemEnv;
import org.rocksdb.Snapshot;
import org.rocksdb.Statistics;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteBatchWithIndex;
import org.rocksdb.WriteOptions;

import zrz.rocksdb.JRocksEngine;

public class BaseRocksTripleStore implements TripleRocksAPI {

  static final byte[] EMPTY = new byte[] {};

  RocksDB db;
  protected final ColumnFamilyHandle[] indexes = new ColumnFamilyHandle[IndexKind.values().length];

  private ColumnFamilyOptions cfo;
  private List<ListenerContext> listeners = new ArrayList<>();

  protected Statistics stats;

  /**
   * creates an in memory store.
   */

  public BaseRocksTripleStore() {

    try {

      this.cfo = new ColumnFamilyOptions();
      final ColumnFamilyDescriptor[] cfd = indexDescriptors(cfo);
      final List<ColumnFamilyHandle> cfh = new ArrayList<>();

      try (final DBOptions opts = new DBOptions()) {
        opts.setCreateIfMissing(true);
        opts.setCreateMissingColumnFamilies(true);
        opts.setEnv(new RocksMemEnv());
        enableStats(opts);
        this.db = RocksDB.open(opts, "/tmp/unused-triplerocks-memdb", Arrays.asList(cfd), cfh);
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
   * 
   * @param cfo
   * @return
   */

  @Deprecated
  public static final ColumnFamilyDescriptor[] indexDescriptors(ColumnFamilyOptions cfo) {

    final ColumnFamilyDescriptor[] cfd = new ColumnFamilyDescriptor[1 + IndexKind.values().length];

    cfd[0] = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfo);

    for (final IndexKind kind : IndexKind.values()) {
      cfd[1 + kind.ordinal()] = new ColumnFamilyDescriptor(kind.toString().getBytes(), cfo);
    }

    return cfd;

  }


  public BaseRocksTripleStore(final Path path) {

    try {

      this.cfo = new ColumnFamilyOptions();
      final ColumnFamilyDescriptor[] cfd = indexDescriptors(cfo);
      final List<ColumnFamilyHandle> cfh = new ArrayList<>();

      try (final DBOptions opts = new DBOptions()) {
        opts.setCreateIfMissing(true);
        opts.setCreateMissingColumnFamilies(true);
        enableStats(opts);
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

  private void enableStats(DBOptions opts) {
    opts.setStatsDumpPeriodSec(1);
    opts.setDumpMallocStats(true);
    this.stats = new Statistics();
    opts.setStatistics(stats);
  }

  /**
   * add a subscriber that will receive indications of store changes.
   * 
   * note that the changes will be delivered AFTER the batch has been written out, and will be made available in the
   * same order as the commits happened.
   * 
   * each listener will be interrogated to ask for the most recent write it has received (which may be null). it will
   * then receive all changes since that point if they are available. it not, the add will be given a chance to
   * interrogate the store as a snapshot, and once done will receive new changes since the snapshot.
   * 
   * @param listener
   * @param id
   * 
   */

  public void addListener(StoreChangeListener listener, long id) {
    ListenerContext lctx = new ListenerContext(this, listener);
    lctx.start(id);
    this.listeners.add(lctx);
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

      this.commit(wb, null);

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

      this.commit(wb, null);

    }

  }

  public long latestSequenceNumber() {
    return this.db.getLatestSequenceNumber();
  }

  /**
   * commit logic.
   * 
   * 
   * 
   * 
   * @param wb
   *          the batch to write. we take ownership and will release once completed.
   * @param snapshot
   *          the snapshot that was used for reads in this batch. we take ownership and will release once completed.
   * 
   */

  void commit(WriteBatchWithIndex wb, Snapshot snapshot) {
    try (WriteOptions opts = new WriteOptions()) {
      this.db.write(opts, wb);
      for (ListenerContext l : this.listeners) {
        try {
          l.accept(wb);
        }
        catch (Throwable t) {
          // well fuck.
          t.printStackTrace();
        }
      }
    }
    catch (final RocksDBException e) {
      throw new RuntimeException(e);
    }
    finally {
      wb.close();
      if (snapshot != null) {
        this.release(snapshot);
        snapshot.close();
      }
    }
  }

  void commit(WriteBatch wb, Snapshot snapshot) {
    try (WriteOptions opts = new WriteOptions()) {
      this.db.write(opts, wb);
      for (ListenerContext l : this.listeners) {
        try {
          l.notify(wb);
        }
        catch (Throwable t) {
          // well fuck.
          t.printStackTrace();
        }
      }
    }
    catch (final RocksDBException e) {
      throw new RuntimeException(e);
    }
    finally {
      wb.close();
      if (snapshot != null) {
        this.release(snapshot);
        snapshot.close();
      }
    }
  }

  public RocksDB db() {
    return this.db;
  }

  /**
   * create a checkpoint of this database.
   */

  public Path checkpoint(Path parentFolder) {
    try (Checkpoint cp = Checkpoint.create(db)) {
      Path path = parentFolder.resolve(UUID.randomUUID().toString());
      cp.createCheckpoint(path.toString());
      return path;
    }
    catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

}
