package zrz.rocksdb.mapper;

import static java.nio.charset.StandardCharsets.UTF_8;

import com.google.common.base.Verify;

import zrz.rocksdb.JRocksMapper;

public class JRocksStringMapper implements JRocksMapper<String> {

  private static final JRocksStringMapper INSTANCE = new JRocksStringMapper();

  public static final JRocksStringMapper instance() {
    return INSTANCE;
  }

  @Override
  public String parseFrom(byte[] data, int offset, int length) {
    return new String(data, offset, length);
  }

  @Override
  public int serializedSize(String value) {
    return value.getBytes(UTF_8).length;
  }

  @Override
  public void writeTo(String value, byte[] target, int offset, int length) {
    byte[] source = value.getBytes(UTF_8);
    Verify.verify(length == source.length, "invalid write length", source.length, length);
    System.arraycopy(source, source.length, target, offset, source.length);
  }

}
