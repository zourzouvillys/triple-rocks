package zrz.triplerocks.jena;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Node_Blank;
import org.apache.jena.graph.Node_URI;
import org.apache.jena.graph.Triple;

import com.google.common.base.Preconditions;
import com.google.common.primitives.Bytes;

import zrz.triplerocks.core.IndexKind;
import zrz.triplerocks.core.MultiKey;

class JenaMultiKey {

  private static final byte BN_TYPE = 1;
  private static final byte IRI_TYPE = 2;
  private static final byte VALUE_TYPE = 3;

  private static final byte[] BN_PFX = new byte[] { BN_TYPE };
  private static final byte[] IRI_PFX = new byte[] { IRI_TYPE };
  private static final byte[] VALUE_PFX = new byte[] { VALUE_TYPE };

  public static byte[] toKey(final Node s) {
    if (s.isBlank()) {
      return Bytes.concat(BN_PFX, ((Node_Blank) s).getBlankNodeLabel().getBytes());
    }
    else if (s.isLiteral()) {
      return Bytes.concat(VALUE_PFX, s.getLiteralLexicalForm().getBytes());
    }
    else if (s.isURI()) {
      return Bytes.concat(IRI_PFX, ((Node_URI) s).toString().getBytes(UTF_8));
    }
    throw new UnsupportedOperationException("invalid type: " + s.getClass());
  }

  public static Triple fromKey(final IndexKind idx, final byte[] key) {

    final int s1 = Bytes.indexOf(key, MultiKey.INDEX_SEPERATOR[0]);
    final int s2 = Bytes.lastIndexOf(key, MultiKey.INDEX_SEPERATOR[0]);

    Preconditions.checkArgument(s1 != -1);
    Preconditions.checkArgument(s2 != -1);
    Preconditions.checkArgument(s1 < s2);

    final Node a = toNode(key, 0, s1);
    final Node b = toNode(key, s1 + 1, s2 - s1 - 1);
    final Node c = toNode(key, s2 + 1, key.length - s2 - 1);

    return idx.toTriple(a, b, c);

  }

  private static Node toNode(final byte[] data, final int start, final int len) {
    switch (data[start]) {
      case BN_TYPE:
        return NodeFactory.createBlankNode(new String(data, start + 1, len - 1, UTF_8));
      case IRI_TYPE:
        return NodeFactory.createURI(new String(data, start + 1, len - 1, UTF_8));
      case VALUE_TYPE:
        return NodeFactory.createLiteral(new String(data, start + 1, len - 1, UTF_8));
      default:
        throw new UnsupportedOperationException("invalid type: " + Byte.toUnsignedInt(data[start]));
    }

  }

}
