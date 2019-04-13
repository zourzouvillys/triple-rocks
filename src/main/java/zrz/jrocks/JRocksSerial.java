package zrz.jrocks;

import java.util.concurrent.atomic.AtomicLong;

/**
 * a serial value which will increment for every value it issues. the engine will write out the
 * latest issued value, regardless of it actually being used.
 * 
 * @author theo
 *
 */

public class JRocksSerial {

  private AtomicLong lastValue;

  public JRocksSerial(JRocksColumnFamily cf, String serialKey, long initialValue) {
    this.lastValue = new AtomicLong(initialValue);
  }

  public long nextSerial() {
    return lastValue.incrementAndGet();
  }

  public long lastValue() {
    return this.lastValue.get();
  }

  public void close() {

  }

}
