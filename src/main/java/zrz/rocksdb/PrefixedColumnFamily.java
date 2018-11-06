package zrz.rocksdb;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;

import org.apache.jena.ext.com.google.common.base.Verify;

import com.google.common.primitives.Bytes;

/**
 * a column family which when used, prefixes all of it's keys with some predefined value and removes
 * the prefix when reading/iterating. iterator seekToFirst and seekForPrev are correctly managed,
 * too.
 * 
 * note: you're far better off using prefixed CFs.
 */

public class PrefixedColumnFamily implements JRocksColumnFamily {

  private final JRocksColumnFamily cf;
  private final byte[] prefix;

  public PrefixedColumnFamily(JRocksColumnFamily cf, String prefix) {
    this(cf, prefix.getBytes(UTF_8));
  }

  public PrefixedColumnFamily(JRocksColumnFamily cf, byte[] prefix) {
    this.cf = cf;
    this.prefix = prefix;
  }

  byte[] prefix(byte[] key) {
    return Bytes.concat(prefix, key);
  }

  @Override
  public int get(JRocksReadableWriter ctx, byte[] key, byte[] value) {
    return cf.get(ctx, prefix(key), value);
  }

  @Override
  public void delete(JRocksWriter ctx, byte[] key) {
    cf.delete(ctx, prefix(key));
  }

  @Override
  public void put(JRocksWriter ctx, byte[] key, byte[] value) {
    cf.put(ctx, prefix(key), value);
  }

  @Override
  public JRocksKeyValueIterator<byte[], byte[]> newIterator(JRocksReadableWriter ctx) {
    return newIterator(ctx, null);
  }

  @Override
  public JRocksKeyValueIterator<byte[], byte[]> newIterator(JRocksReadableWriter ctx, byte[] prefix) {

    Verify.verifyNotNull(ctx, "ctx");

    byte[] underlying = this.prefix;

    byte[] root =
      prefix == null ? this.prefix
                     : Bytes.concat(this.prefix, prefix);

    return new ForwardingJRocksKVIterator<byte[], byte[]>(cf.newIterator(ctx)) {

      @Override
      public byte[] key() {
        byte[] key = super.key();
        return Arrays.copyOfRange(key, underlying.length, key.length);
      }

      @Override
      public void seekToFirst() {
        super.seek(root);
      }

      @Override
      public void seek(byte[] target) {
        super.seek(prefix(target));
      }

      @Override
      public void seekForPrev(byte[] target) {
        super.seekForPrev(prefix(target));
      }

      @Override
      public boolean isValid() {

        if (!super.isValid())
          return false;

        byte[] key = super.key();

        if (key.length < root.length) {
          return false;
        }

        for (int i = 0; i < root.length; ++i) {
          if (key[i] != root[i]) {
            return false;
          }
        }

        return true;

      }

    };

  }

  public String toString() {
    return this.cf + " / " + new String(this.prefix);
  }

}
