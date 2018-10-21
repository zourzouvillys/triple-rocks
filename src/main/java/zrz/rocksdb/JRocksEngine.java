package zrz.rocksdb;

import static java.util.Objects.requireNonNull;

import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.rocksdb.Cache;
import org.rocksdb.Checkpoint;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.Env;
import org.rocksdb.FlushOptions;
import org.rocksdb.LRUCache;
import org.rocksdb.OptionsUtil;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;
import org.rocksdb.SstFileManager;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteBatchWithIndex;
import org.rocksdb.WriteOptions;
import org.rocksdb.util.SizeUnit;

import zrz.triplerocks.core.IndexKind;

public class JRocksEngine implements Closeable {

  static {
    RocksDB.loadLibrary();
  }

  private final RocksDB db;
  private final ColumnFamilyHandle defaultCF;
  private final ColumnFamilyHandle[] indexes;
  private DBOptions opts;
  private ColumnFamilyOptions cfo;
  private SstFileManager sst;
  private Cache cache;

  private JRocksEngine(
      RocksDB db,
      ColumnFamilyHandle defaultCF,
      ColumnFamilyHandle[] indexes,
      DBOptions opts,
      ColumnFamilyOptions cfo,
      SstFileManager sst,
      Cache cache) {

    this.db = db;
    this.defaultCF = defaultCF;
    this.indexes = indexes;
    this.opts = opts;
    this.cfo = cfo;
    this.sst = sst;
    this.cache = cache;

  }

  @SuppressWarnings("resource")
  public static JRocksEngine open(Path path) {

    try {

      DBOptions opts = new DBOptions();

      final List<ColumnFamilyHandle> cfh = new ArrayList<>();
      final SstFileManager sst = new SstFileManager(Env.getDefault());
      final RocksDB db;
      final ColumnFamilyOptions cfo = new ColumnFamilyOptions();

      // cfo.setTableFormatConfig(new PlainTableConfig().setHashTableRatio(0));
      Cache cache = new LRUCache(SizeUnit.MB * 32);

      // long bufferSizeBytes = SizeUnit.MB * 16;
      // WriteBufferManager wbm = new WriteBufferManager(bufferSizeBytes, cache);
      // opts.setWriteBufferManager(wbm);

      if (Files.exists(path)) {

        List<ColumnFamilyDescriptor> cfds = new ArrayList<>();

        OptionsUtil.loadLatestOptions(path.toString(), Env.getDefault(), opts, cfds);

        opts.setSstFileManager(sst);

        db = RocksDB.open(opts, path.toString(), cfds, cfh);

      }
      else {

        final ColumnFamilyDescriptor[] cfd = indexDescriptors(cfo);

        opts.setSstFileManager(sst);
        opts.setCreateIfMissing(true);
        opts.setCreateMissingColumnFamilies(true);

        db = RocksDB.open(opts, path.toString(), Arrays.asList(cfd), cfh);

      }

      ColumnFamilyHandle[] indexes = new ColumnFamilyHandle[IndexKind.values().length];

      for (final IndexKind kind : IndexKind.values()) {
        indexes[kind.ordinal()] = cfh.get(kind.ordinal() + 1);
      }

      return new JRocksEngine(db, cfh.get(0), indexes, opts, cfo, sst, cache);

    }
    catch (final RocksDBException e) {
      throw new RuntimeException(e);
    }

  }

  /**
   * 
   */

  public void backup() {

  }

  /**
   * 
   * @param cfo
   * @return
   */

  public static final ColumnFamilyDescriptor[] indexDescriptors(ColumnFamilyOptions cfo) {

    final ColumnFamilyDescriptor[] cfd = new ColumnFamilyDescriptor[1 + IndexKind.values().length];

    cfd[0] = new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, cfo);

    for (final IndexKind kind : IndexKind.values()) {
      cfd[1 + kind.ordinal()] = new ColumnFamilyDescriptor(kind.toString().getBytes(), cfo);
    }

    return cfd;

  }

  public void add(WriteBatch... batches) {
    try {
      try (WriteOptions opts = new WriteOptions()) {
        opts.setSync(false);
        opts.setDisableWAL(true);
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
        opts.setDisableWAL(true);
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

  public void compactRange() {

    try {
      db.compactRange(this.defaultCF);
      for (ColumnFamilyHandle ifx : this.indexes) {
        db.compactRange(ifx);
      }
    }
    catch (RocksDBException e) {
      // TODO Auto-generated catch block
      throw new RuntimeException(e);
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
   * return the column family for the specified index.
   * 
   * @param idx
   * @return
   */

  public ColumnFamilyHandle index(IndexKind idx) {
    return this.indexes[idx.ordinal()];
  }

  /**
   * return the column family for the specified dataset.
   * 
   * @return
   */

  public ColumnFamilyHandle keysHandle() {
    return this.defaultCF;
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

    this.defaultCF.close();
    Arrays.stream(this.indexes).forEach(ColumnFamilyHandle::close);
    this.db.close();
    this.opts.close();
    this.cfo.close();
    this.sst.close();

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
