package zrz.rocksdb;

public interface JRocksMapper<T> {

  /**
   * materialize a record from the given byte array, starting at the specified offset and consuming the given length of
   * bytes.
   * 
   * @param data
   * @return
   */

  T parseFrom(byte[] data, int offset, int length);

  /**
   * the size of buffer that will be needed to encode this value.
   */

  int serializedSize(T value);

  /**
   * write the record out to the given array, starting at offset. the length will be the same value as returned by
   * {@link JRocksMapper#serializedSize(Object)}.
   */

  void writeTo(T value, byte[] array, int offset, int length);

  default byte[] toByteArray(T value) {
    int len = serializedSize(value);
    byte[] buffer = new byte[len];
    writeTo(value, buffer, 0, len);
    return buffer;
  }

  default T parseFrom(byte[] data) {
    return parseFrom(data, 0, data.length);
  }

}
