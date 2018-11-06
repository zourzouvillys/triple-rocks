package zrz.triplerocks.jena;

import java.io.IOException;

import org.apache.jena.graph.Node;
import org.apache.jena.sparql.core.Quad;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import zrz.rocksdb.JRocksMapper;

/**
 * maps Quad instances to on-disk bytes.
 * 
 * each slot is prefixed with a length value (byte compressed) followed by the encoded node.
 * 
 * @author theo
 *
 */

public class JenaQuadMapper implements JRocksMapper<Quad> {

  private static final JenaQuadMapper INSTANCE = new JenaQuadMapper();

  public static JenaQuadMapper instance() {
    return INSTANCE;
  }

  @Override
  public Quad parseFrom(byte[] data, int offset, int length) {

    CodedInputStream in = CodedInputStream.newInstance(data, offset, length);

    Node g = this.readNode(in);
    Node s = this.readNode(in);
    Node p = this.readNode(in);
    Node o = this.readNode(in);

    return Quad.create(g, s, p, o);

  }

  private Node readNode(CodedInputStream in) {
    try {
      int len = in.readInt32();
      return JenaNodeMapper.instance().parseFrom(in.readRawBytes(len));
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public int serializedSize(Quad value) {
    return componentSize(value.getGraph())
      + componentSize(value.getSubject())
      + componentSize(value.getPredicate())
      + componentSize(value.getObject());
  }

  private int componentSize(Node value) {
    int tlen = JenaNodeMapper.instance().serializedSize(value);
    return tlen + CodedOutputStream.computeUInt32SizeNoTag(tlen);
  }

  @Override
  public void writeTo(Quad value, byte[] array, int offset, int length) {

    CodedOutputStream out = CodedOutputStream.newInstance(array, offset, length);
    writeNode(out, value.getGraph());
    writeNode(out, value.getSubject());
    writeNode(out, value.getPredicate());
    writeNode(out, value.getObject());
  }

  private void writeNode(CodedOutputStream out, Node value) {
    byte[] data = JenaNodeMapper.instance().toByteArray(value);
    try {
      out.writeInt32NoTag(data.length);
      out.writeRawBytes(data);
    }
    catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

}
