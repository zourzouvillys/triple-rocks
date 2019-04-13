package zrz.jrocks;

/**
 * execution with a context.
 * 
 * @author theo
 */

interface JRocksWriteContext {

  void put(byte[] key, byte[] value);

  void delete(byte[] key);

}
