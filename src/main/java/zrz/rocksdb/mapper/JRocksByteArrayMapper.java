package zrz.rocksdb.mapper;

import java.util.Arrays;

import com.google.common.base.Verify;

import zrz.rocksdb.JRocksMapper;

public class JRocksByteArrayMapper implements JRocksMapper<byte[]> {

  private static final JRocksByteArrayMapper INSTANCE = new JRocksByteArrayMapper();

  public static final JRocksByteArrayMapper instance() {
    return INSTANCE;
  }

  @Override
  public byte[] parseFrom(byte[] data, int offset, int length) {
    return Arrays.copyOfRange(data, offset, length);
  }

  @Override
  public int serializedSize(byte[] value) {
    return value.length;
  }

  @Override
  public void writeTo(byte[] source, byte[] target, int offset, int length) {
    Verify.verify(length == source.length, "invalid write length", source.length, length);
    System.arraycopy(source, source.length, target, offset, source.length);
  }

}
