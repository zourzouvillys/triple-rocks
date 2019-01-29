package zrz.rocksdb;

import static org.junit.Assert.*;

import org.junit.Test;
import org.rocksdb.RocksIterator;

public class JRocksSnapshotTest {

  @Test
  public void test() {

    JRocksEngine db = JRocksEngine.createInMemory();

    JAttachedColumnFamily testcf = db.columnFamily("test");

    try (JRocksSnapshot snap = db.createSnapshot()) {

      RocksIterator it = snap.newIterator(testcf);

    }

  }

}
