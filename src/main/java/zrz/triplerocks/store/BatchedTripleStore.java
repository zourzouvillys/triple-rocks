package zrz.triplerocks.store;

import java.util.stream.Stream;

import zrz.rocksdb.JRocksBatchWithIndex;
import zrz.rocksdb.JRocksBatchWriter;
import zrz.rocksdb.JRocksEngine;
import zrz.rocksdb.JRocksSet;

/**
 * a triple store which provides atomic change semantics but enforces consistency with reasonable performance by only
 * submitting the batch when flushed. this allows multiple changes to be batched.
 * 
 * this version is not reenteant. use {@link ConcurrentBatchedTripleStore} if you need it to be.
 * 
 * to use it, make changes then call {@link #flush(JRocksBatchWriter)} with the engine to commit all of the changes to
 * the backing store
 * 
 * @author theo
 *
 * @param <TripleT>
 */

public class BatchedTripleStore<TripleT> implements TripleStore<TripleT> {

  // the logical collection that contains the statements. may be sharded or prefix-mapped
  private JRocksSet<TripleT> collection;
  private JRocksBatchWithIndex batch;
  private JRocksEngine engine;

  /**
   * create a new instance using the given set.
   * 
   * @param engine
   * @param collection
   */

  public BatchedTripleStore(JRocksEngine engine, JRocksSet<TripleT> collection) {
    this.engine = engine;
    this.batch = new JRocksBatchWithIndex(engine);
    this.collection = collection;
  }

  /**
   * add a statement.
   */

  @Override
  public boolean add(TripleT triple) {
    if (collection.contains(this.batch, triple))
      return false;
    collection.put(this.batch, triple);
    return true;
  }

  /**
   * remove a statement.
   */

  @Override
  public boolean remove(TripleT triple) {
    if (!collection.contains(batch, triple))
      return false;
    collection.delete(batch, triple);
    return true;
  }

  /**
   * true if the given triple is already in the store (or in a pending batch).
   */

  @Override
  public boolean contains(TripleT triple) {
    return collection.contains(batch, triple);
  }

  /**
   * clear any pending changes, rolling back to the previous state.
   */

  public void clear() {
    this.batch.clear();
  }

  /**
   * write this batch out, making the changes permanent.
   * 
   * this will block any new writes until the flush is complete.
   * 
   */

  public void flush(JRocksBatchWriter w) {
    w.accept(batch);
    this.batch = new JRocksBatchWithIndex(engine);
  }

  /**
   * the number of updates which have yet to be flushed. this includes both deletes and adds.
   */

  public long pendingCount() {
    return this.batch.count();
  }

  /**
   * returns the total count of triples, in both the store and the pending set. note: this is slow to compute as it
   * counts each entry by iterating over it!
   */

  public long count() {
    try (Stream<TripleT> stream = this.collection.stream(batch)) {
      return stream.count();
    }
  }

  /**
   * stream of the triples in the store.
   */

  @Override
  public Stream<TripleT> stream() {
    return this.collection.stream(batch);
  }

}
