package zrz.triplerocks.core;

import org.apache.jena.graph.Triple;

/**
 * logical store listener.
 * 
 * each change that has been written triggers the method on the interface, followed by a commit.
 * 
 * the commit is not transactional in the sense it has a rollback/abort or may be withdrawn - but instead allows for
 * batching of changes and making them visible atomically.
 * 
 * @author theo
 *
 */

public interface StoreChangeListener {

  /**
   * a statement was inserted into the store.
   * 
   * @param s
   * @param p
   * @param o
   */

  void insert(Triple triple);

  /**
   * a statement was deleted from the store.
   * 
   * @param s
   * @param p
   * @param o
   */

  void delete(Triple triple);

  /**
   * indicates a synchronization point, e.g a position at which all previous changes are visible.
   */

  void sync();

}
