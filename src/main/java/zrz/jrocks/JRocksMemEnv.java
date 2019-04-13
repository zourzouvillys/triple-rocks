package zrz.jrocks;

import org.rocksdb.RocksDB;
import org.rocksdb.RocksMemEnv;

/**
 * in-memory environment for a rocks DB instance.
 * 
 * @author theo
 *
 */
public class JRocksMemEnv implements AutoCloseable {

  static {
    RocksDB.loadLibrary();
  }

  private RocksMemEnv env;

  public JRocksMemEnv() {
    this.env = new RocksMemEnv();
  }

  public void close() {
    this.env.close();
  }

}
