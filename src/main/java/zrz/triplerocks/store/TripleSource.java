package zrz.triplerocks.store;

import java.util.stream.Stream;

public interface TripleSource<TripleT> {

  /**
   * checks if this statement exists in the store.
   */

  boolean contains(TripleT triple);

  /**
   * a stream of triples in this store.
   */

  Stream<TripleT> stream();

}
