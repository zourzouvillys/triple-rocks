package zrz.rocksdb.mgmt;

import java.util.Collection;

/**
 * SPI for publishing a set of blobs from a staged directory.
 * 
 * @author theo
 *
 */

public interface BlobPublisher {

  void publish(Collection<JRocksBlob> blobs);

}
