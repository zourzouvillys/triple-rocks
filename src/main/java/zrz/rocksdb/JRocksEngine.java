package zrz.rocksdb;

import static java.util.Objects.requireNonNull;

import java.io.Closeable;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

import org.rocksdb.Cache;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.FlushOptions;
import org.rocksdb.OptionsUtil;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.RocksIterator;
import org.rocksdb.SstFileManager;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteBatchWithIndex;
import org.rocksdb.WriteOptions;

import zrz.triplerocks.core.IndexKind;

public class JRocksEngine implements Closeable, JRocksBatchWriter, JRocksReadableWriter {

  static {
    RocksDB.loadLibrary();
  }

  final RocksDB db;
  private DBOptions opts;
  private ColumnFamilyOptions cfo;
  private SstFileManager sst;
  private Cache cache;
  private Map<String, JAttachedColumnFamily> cfs;

  private JRocksEngine(
      RocksDB db,
      List<ColumnFamilyHandle> cfh,
      DBOptions opts,
      ColumnFamilyOptions cfo) {

    this.db = db;
    this.opts = opts;
    this.cfo = cfo;

    this.cfs = cfh.stream().collect(
        Collectors.toMap(
            h -> {
              try {
                return new String(h.getName(), StandardCharsets.UTF_8);
              }
              catch (RocksDBException e) {
                throw new RuntimeException(e);
              }
            },
            h -> new JAttachedColumnFamily(this, h)));

  }

  public JAttachedColumnFamily columnFamily(String cfname) {
    return this.openColumnFamilyHandle(cfname);
  }

  /**
   * 
   * @param cfname
   * @return
   */

  private JAttachedColumnFamily openColumnFamilyHandle(String cfname) {
    return this.cfs.computeIfAbsent(
        cfname,
        key -> {
          ColumnFamilyDescriptor cfd = new ColumnFamilyDescriptor(key.getBytes(), cfo);
          try {
            return new JAttachedColumnFamily(this, this.db.createColumnFamily(cfd));
          }
          catch (RocksDBException e) {
            throw new RuntimeException(e);
          }
        });
  }

  /**
   * 
   */

  public void backup() {

  }

  public void add(WriteBatch... batches) {
    try {
      try (WriteOptions opts = new WriteOptions()) {
        opts.setSync(false);
        opts.setDisableWAL(false);
        for (WriteBatch b : batches) {
          this.db.write(opts, requireNonNull(b));
        }
      }
    }
    catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  public void add(WriteBatchWithIndex... batches) {
    try {
      try (WriteOptions opts = new WriteOptions()) {
        opts.setSync(false);
        opts.setDisableWAL(false);
        for (WriteBatchWithIndex b : batches) {
          this.db.write(opts, b);
        }
      }
    }
    catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * compact all ranges for all CFs, including the default.
   */

  public void compactAllRanges() {
    for (JAttachedColumnFamily ifx : this.cfs.values()) {
      ifx.compactRange(this);
    }
  }

  /**
   * ensure everything is written out to disk. blocks until the flush is complete.
   */

  public void flush() {
    try (FlushOptions opts = new FlushOptions()) {
      opts.setWaitForFlush(true);
      try {
        this.db.flush(opts);
      }
      catch (RocksDBException e) {
        // TODO Auto-generated catch block
        throw new RuntimeException(e);
      }
    }
  }

  /**
   * 
   */

  public long totalSize() {
    return sst.getTotalSize();
  }

  public Map<String, Long> trackedFiles() {
    return sst.getTrackedFiles();
  }

  /**
   * close this database instance.
   */

  @Override
  public void close() {

    // List<RocksDB> dbs = new ArrayList<>(1);
    // dbs.add(db);
    // Set<Cache> caches = new HashSet<>(1);
    // caches.add(cache);
    // Map<MemoryUsageType, Long> usage = MemoryUtil.getApproximateMemoryUsageByType(dbs, caches);

    // db.getAggregatedLongProperty(MEMTABLE_SIZE)
    // db.getAggregatedLongProperty(UNFLUSHED_MEMTABLE_SIZE)
    // db.getAggregatedLongProperty(TABLE_READERS)

    // System.err.println(usage.get(MemoryUsageType.kMemTableTotal));
    // System.err.println(usage.get(MemoryUsageType.kMemTableUnFlushed));
    // System.err.println(usage.get(MemoryUsageType.kTableReadersTotal));
    // System.err.println(usage.get(MemoryUsageType.kCacheTotal));

    this.cfs.values().forEach(JAttachedColumnFamily::close);
    this.db.close();
    this.opts.close();
    this.cfo.close();
    if (this.sst != null) {
      this.sst.close();
    }
    if (this.cache != null) {
      this.cache.close();
    }

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

  public void accept(JRocksBatch batch) {
    this.add(batch.batch);
    batch.close();
  }

  public void accept(JRocksBatchWithIndex batch) {
    this.add(batch.batch);
    batch.close();
  }

  @SuppressWarnings("resource")
  public static JRocksEngine createInMemory() {

    try {

      DBOptions opts = new DBOptions();

      final List<ColumnFamilyHandle> cfh = new ArrayList<>();
      final RocksDB db;
      final ColumnFamilyOptions cfo = new ColumnFamilyOptions();

      List<ColumnFamilyDescriptor> cfds = new ArrayList<>();

      cfds.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfo));

      opts.setCreateIfMissing(true);
      opts.setCreateMissingColumnFamilies(true);

      Path tmp = Files.createTempDirectory("jrocksdb-mem-");

      db = RocksDB.open(opts, tmp.toString(), cfds, cfh);

      return new JRocksEngine(db, cfh, opts, cfo);

    }
    catch (final RocksDBException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  @SuppressWarnings("resource")
  public static JRocksEngine open(Path path) {

    try {

      DBOptions opts = new DBOptions();

      final List<ColumnFamilyHandle> cfh = new ArrayList<>();
      final RocksDB db;
      final ColumnFamilyOptions cfo = new ColumnFamilyOptions();

      List<ColumnFamilyDescriptor> cfds = new ArrayList<>();

      if (Files.exists(path)) {

        OptionsUtil.loadLatestOptions(path.toString(), Env.getDefault(), opts, cfds);

        db = RocksDB.open(opts, path.toString(), cfds, cfh);

      }
      else {

        cfds.add(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfo));

        opts.setCreateIfMissing(true);
        opts.setCreateMissingColumnFamilies(true);

        db = RocksDB.open(opts, path.toString(), cfds, cfh);

      }

      return new JRocksEngine(db, cfh, opts, cfo);

    }
    catch (final RocksDBException e) {
      throw new RuntimeException(e);
    }

  }

  //

  @Override
  public void put(JAttachedColumnFamily cf, byte[] key, byte[] value) {
    try {
      db.put(cf.h, key, value);
    }
    catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void delete(JAttachedColumnFamily cf, byte[] key) {
    try {
      db.delete(cf.h, key);
    }
    catch (IllegalArgumentException | RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * 
   */

  @Override
  public RocksIterator newIterator(JAttachedColumnFamily cf) {
    return db.newIterator(cf.h);
  }

  /**
   * 
   */

  @Override
  public int get(JAttachedColumnFamily cf, byte[] key, byte[] value) {
    try {
      return db.get(cf.h, key, value);
    }
    catch (IllegalArgumentException | RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

}
