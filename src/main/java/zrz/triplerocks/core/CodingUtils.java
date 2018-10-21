package zrz.triplerocks.core;

public class CodingUtils {

  public static int writeLong(byte[] buffer, int position, long value) {
    int totalBytesWritten = 0;
    while (true) {
      if ((value & ~0x7FL) == 0) {
        buffer[position++] = (byte) value;
        totalBytesWritten++;
        return totalBytesWritten;
      }
      else {
        buffer[position++] = (byte) (((int) value & 0x7F) | 0x80);
        totalBytesWritten++;
        value >>>= 7;
      }
    }
  }

  public static int computeLength(long value) {
    // handle two popular special cases up front ...
    if ((value & (~0L << 7)) == 0L) {
      return 1;
    }
    if (value < 0L) {
      return 10;
    }
    // ... leaving us with 8 remaining, which we can divide and conquer
    int n = 2;
    if ((value & (~0L << 35)) != 0L) {
      n += 4;
      value >>>= 28;
    }
    if ((value & (~0L << 21)) != 0L) {
      n += 2;
      value >>>= 14;
    }
    if ((value & (~0L << 14)) != 0L) {
      n += 1;
    }
    return n;
  }

}
