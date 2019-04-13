package zrz.jrocks;

/**
 * A logical index.
 * 
 * @author theo
 *
 */

public interface JRocksIndex {

  /**
   * called to generate an index entry for the given statement.
   */

  void index();

  /**
   * called to remove the index entry for the given statement.
   */

  void unindex();

  // void newCursor();

}
