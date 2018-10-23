package zrz.triplerocks.core;

import static org.junit.Assert.*;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.junit.Test;
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ColumnFamilyOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.FlushOptions;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import zrz.rocksdb.JRocksEngine;

public class IndexedRockStoreTest {

  @Test
  public void test() throws RocksDBException {
    final ColumnFamilyOptions cfo = new ColumnFamilyOptions();

    final ColumnFamilyDescriptor[] cfd = BaseRocksTripleStore.indexDescriptors(cfo);
    final List<ColumnFamilyHandle> cfh = new ArrayList<>();

    DBOptions opts = new DBOptions();

    opts.setCreateIfMissing(true);
    opts.setCreateMissingColumnFamilies(true);

    RocksDB db = RocksDB.open(opts, "/tmp/xxx", Arrays.asList(cfd), cfh);

    ColumnFamilyHandle[] indexes = new ColumnFamilyHandle[IndexKind.values().length];

    for (final IndexKind kind : IndexKind.values()) {
      indexes[kind.ordinal()] = cfh.get(kind.ordinal() + 1);
    }

    try (FlushOptions flush = new FlushOptions()) {
      flush.setWaitForFlush(true);
      try {
        db.flush(flush);
      }
      catch (RocksDBException e) {
        // TODO Auto-generated catch block
        throw new RuntimeException(e);
      }
    }

    // this.flush();
    cfh.get(0).close();
    Arrays.stream(indexes).forEach(ColumnFamilyHandle::close);
    db.close();
    opts.close();
    cfo.close();

  }

}
