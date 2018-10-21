package zrz.triplerocks.replication;

/**
 * SPI for delta publication.
 */

public interface DeltaPublisher {

  /**
   * publishes the given delta to consumers.
   */

  void announce(byte[] delta);

}
