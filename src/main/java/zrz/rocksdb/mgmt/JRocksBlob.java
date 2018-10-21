package zrz.rocksdb.mgmt;

import java.nio.file.Path;

public interface JRocksBlob {

  /**
   * the blob ID for later retrieval.
   */
  
  String blobId();
  
  /**
   * the source file.
   */

  Path source();

}
