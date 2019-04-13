package zrz.jrocks;

import static java.nio.charset.StandardCharsets.UTF_8;

import java.util.Arrays;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jena.ext.com.google.common.primitives.Longs;
import org.apache.jena.graph.Node;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.DBOptions;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatchWithIndex;

import com.google.common.base.Preconditions;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.primitives.Bytes;

import zrz.triplerocks.core.CodingUtils;

/**
 * allocates a short key to an IRI, returning the same key for the same value.
 * 
 * @author theo
 *
 */

public class KeyMap {

  private LoadingCache<Node, byte[]> ids = CacheBuilder.newBuilder()
      .maximumSize(5_000)
      .initialCapacity(5_000)
      .recordStats()
      .build(CacheLoader.from(this::loadKey));

  private final WriteBatchWithIndex idx;
  private AtomicLong alloc = new AtomicLong(0);

  private DBOptions opts;

  private ColumnFamilyHandle cfh;

  public KeyMap(WriteBatchWithIndex idx, ColumnFamilyHandle cfh) {
    this.idx = idx;
    this.cfh = cfh;
    this.opts = new DBOptions();
  }

  /**
   * returns a key to use for representing this node. if it is already in use, the same key will be returned.
   *
   * the returned key is shared, and must not be modified.
   * 
   * @param node
   * @return
   */

  public byte[] alloc(Node node) {
    return this.ids.getUnchecked(node);
  }

  public long lastAllocatedKey() {
    return this.alloc.get();
  }

  public String toString() {
    return "current keylen=" + CodingUtils.computeLength(this.lastAllocatedKey()) + ": " +
        this.ids.stats().toString();
  }

  byte[] loadKey(Node key) {

    String url = key.getURI();

    // get the IRI in byte form, which is what we use to index on.
    byte[] bytes = url.getBytes(UTF_8);

    try {

      byte[] found = idx.getFromBatch(cfh, opts, bytes);

      if (found != null) {
        return Arrays.copyOf(found, found.length);
      }

      long id = alloc.incrementAndGet();

      //
      int len = CodingUtils.computeLength(id);
      byte[] buffer = new byte[len];
      CodingUtils.writeLong(buffer, 0, id);

      idx.put(buffer, bytes);

      return buffer;

    }
    catch (RocksDBException e) {
      throw new RuntimeException(e);
    }

  }

}
