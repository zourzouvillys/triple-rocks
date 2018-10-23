package zrz.triplerocks.store;

/**
 * a logical statement store build on top of triplerocks that is designed for a write-side gateway that hands off
 * statements to indexers to answer queries.
 * 
 * as such, this API does not provide a way to directly query the store other than concrete statements, e.g asking "does
 * this statement exist".
 * 
 * generally this interface would be bound to a writebatch rather than directly to the store. consumers are responsible
 * for the lifecycle mapping of such bindings.
 * 
 * @author theo
 *
 */

public interface TripleStream<TripleT> {

  /**
   * insert a statement into the store if it does not exist.
   * 
   * @return true if the statement was added, or false if it already existed.
   * 
   */

  boolean add(TripleT triple);

  /**
   * remove the specified statement from the store if it exists.
   * 
   * @return true if the statement existed in the store, else false.
   * 
   */

  boolean remove(TripleT triple);

}
