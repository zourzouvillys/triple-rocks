package zrz.jrocks.mapper;

public class JRocksMappers {

  public static final JRocksStringMapper string() {
    return JRocksStringMapper.instance();
  }

  public static final JRocksByteArrayMapper byteArray() {
    return JRocksByteArrayMapper.instance();
  }

}
