package zrz.rocksdb.protobuf;

import java.io.IOException;

import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.Parser;

import zrz.jrocks.JRocksMapper;

/**
 * mapper which uses the protobuf infrastructure. useful for records, not so much for indexes.
 * 
 * @author theo
 *
 * @param <T>
 */
public class ProtobufMessageMapper<T extends Message> implements JRocksMapper<T> {

  private final Parser<T> parser;

  public ProtobufMessageMapper(Parser<T> parser) {
    this.parser = parser;
  }

  @Override
  public T parseFrom(byte[] data, int offset, int length) {
    try {
      return parser.parseFrom(data, offset, length);
    }
    catch (InvalidProtocolBufferException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int serializedSize(T value) {
    return value.getSerializedSize();
  }

  @Override
  public void writeTo(T value, byte[] array, int offset, int length) {
    try {
      value.writeTo(CodedOutputStream.newInstance(array, offset, length));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public static <T extends Message> ProtobufMessageMapper<T> withParser(Parser<T> parser) {
    return new ProtobufMessageMapper<>(parser);
  }

}
