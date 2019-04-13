package zrz.triplerocks.jena;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.ext.com.google.common.base.Verify;
import org.apache.jena.graph.BlankNodeId;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

import com.google.common.base.VerifyException;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import triplediff.protobuf.TripleDiffProto.Literal;
import zrz.jrocks.JRocksMapper;

public class JenaNodeMapper implements JRocksMapper<Node> {

  private static final JenaNodeMapper INSTANCE = new JenaNodeMapper();

  public static JenaNodeMapper instance() {
    return INSTANCE;
  }

  /**
   * 
   */

  @Override
  public Node parseFrom(byte[] data, int offset, int length) {
    byte header = data[offset];
    offset++;
    length--;
    switch (JenaNodeType.valueOf(header)) {
      case IRI:
        return NodeFactory.createURI(new String(data, offset, length));
      case BLANK_NODE:
        return NodeFactory.createBlankNode(BlankNodeId.create(new String(data, offset, length)));
      case LITERAL: {
        try {
          Literal lit = Literal.parseFrom(ByteString.copyFrom(data, offset, length));
          return NodeFactory.createLiteral(lit.getLexicalForm().toStringUtf8(), TypeMapper.getInstance().getSafeTypeByName(lit.getDataType()));
        }
        catch (InvalidProtocolBufferException e) {
          throw new RuntimeException(e);
        }
      }
      default:
        throw new IllegalArgumentException("unknown node");
    }
  }

  @Override
  public int serializedSize(Node value) {
    Verify.verify(value.isConcrete(), "node not concrete", value.getClass());
    int pfxlen = 1;
    return toBytes(value).length + pfxlen;
  }

  public byte[] toBytes(Node value) {

    if (value.isURI())
      return value.getURI().getBytes(UTF_8);
    else if (value.isBlank())
      return value.getBlankNodeLabel().getBytes(UTF_8);
    else if (value.isLiteral()) {
      return Literal.newBuilder()
        .setLexicalForm(ByteString.copyFromUtf8(value.getLiteralLexicalForm()))
        .setDataType(value.getLiteral().getDatatypeURI())
        .build()
        .toByteArray();
    }

    throw new VerifyException("unsupported type: " + value.getClass());

  }

  JenaNodeType toType(Node value) {
    if (value.isURI())
      return JenaNodeType.IRI;
    else if (value.isBlank())
      return JenaNodeType.BLANK_NODE;
    else if (value.isLiteral())
      return JenaNodeType.LITERAL;
    throw new VerifyException("unsupported type: " + value.getClass());
  }

  @Override
  public void writeTo(Node value, byte[] array, int offset, int length) {
    byte[] data = this.toBytes(value);
    Verify.verify((data.length + 1) == length, "%s != %s", data.length + 1, length);
    array[offset] = toType(value).code();
    System.arraycopy(data, 0, array, offset + 1, data.length);
  }

}
