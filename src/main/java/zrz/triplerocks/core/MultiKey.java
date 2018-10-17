package zrz.triplerocks.core;

import com.google.common.primitives.Bytes;

public class MultiKey {

  public static final byte[] INDEX_SEPERATOR = { 0 };

  public static byte[] create(final byte[] k1) {
    return Bytes.concat((k1), INDEX_SEPERATOR);
  }

  public static byte[] create(final byte[] k1, final byte[] k2) {
    return Bytes.concat((k1), INDEX_SEPERATOR, (k2), INDEX_SEPERATOR);
  }

  public static byte[] create(final byte[] k1, final byte[] k2, final byte[] k3) {
    return Bytes.concat((k1), INDEX_SEPERATOR, (k2), INDEX_SEPERATOR, (k3));
  }

}
