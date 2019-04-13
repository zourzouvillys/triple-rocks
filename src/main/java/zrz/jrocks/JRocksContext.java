package zrz.jrocks;

import org.rocksdb.RocksIterator;

/**
 * @author theo
 */

interface JRocksContext extends JRocksWriteContext {

  int get(byte[] key, byte[] value);

  RocksIterator newIterator();

}
