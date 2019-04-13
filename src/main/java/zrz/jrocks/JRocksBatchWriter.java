package zrz.jrocks;

/**
 * interface for writing a batch out.
 * 
 * @author theo
 *
 */
public interface JRocksBatchWriter {

  /**
   * accepts this batch and commits it, taking ownership of it and responsibility for clearing it.
   * 
   * returns once it has been.
   * 
   * the batch must no longer be used
   * 
   * @param batch
   * 
   */

  void accept(JRocksBatchWithIndex batch);

  void accept(JRocksBatch batch);

}
