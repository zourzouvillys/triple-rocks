package zrz.triplerocks.store;

import java.util.concurrent.locks.ReentrantReadWriteLock;

import zrz.rocksdb.JRocksColumnFamily;
import zrz.rocksdb.JRocksBatchWriter;
import zrz.rocksdb.JRocksEngine;
import zrz.rocksdb.JRocksSet;

/**
 * a concurrent statement store which provides atomic change semantics but enforces consistency with reasonable
 * performance by only submitting the batch when flushed that can be used by multiple writers.
 * 
 * note that the performance of a non locked store is significantly better. it is far more performant to get a single
 * write lock and process a whole batch at once, before releasing and letting another.
 * 
 * @author theo
 *
 * @param <NodeT>
 */

public class ConcurrentBatchedTripleStore<TripleT> extends BatchedTripleStore<TripleT> {

  private ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

  public ConcurrentBatchedTripleStore(JRocksSet<TripleT> collection, JRocksEngine engine) {
    super(engine, collection);
  }

  /**
   * add a statement.
   */

  @Override
  public boolean add(TripleT triple) {
    lock.writeLock().lock();
    try {
      return super.add(triple);
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * remove a statement.
   */

  @Override
  public boolean remove(TripleT triple) {
    lock.writeLock().lock();
    try {
      return super.remove(triple);
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * true if the given triple is already in the store (or in a pending batch).
   */

  @Override
  public boolean contains(TripleT triple) {
    lock.readLock().lock();
    try {
      return super.contains(triple);
    }
    finally {
      lock.readLock().unlock();
    }
  }

  /**
   * clear any pending changes, rolling back to the previous state.
   */

  public void clear() {
    this.lock.writeLock().lock();
    try {
      super.clear();
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * write this batch out, making the changes permanent.
   * 
   * this will block any new writes until the flush is complete.
   * 
   */

  public void flush(JRocksBatchWriter writer) {
    lock.writeLock().lock();
    try {
      super.flush(writer);
    }
    finally {
      lock.writeLock().unlock();
    }
  }

  /**
   * the number of updates which have yet to be flushed. this includes both deletes and adds.
   */

  public long pendingCount() {
    lock.readLock().lock();
    try {
      return super.pendingCount();
    }
    finally {
      lock.readLock().unlock();
    }
  }

  /**
   * returns the total count of triples, in both the store and the pending set. note: this is slow to compute as it
   * counts each entry by iterating over it!
   */

  public long count() {
    lock.readLock().lock();
    try {
      return super.count();
    }
    finally {
      lock.readLock().unlock();
    }
  }

}
