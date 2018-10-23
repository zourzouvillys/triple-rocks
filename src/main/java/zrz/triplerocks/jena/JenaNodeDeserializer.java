package zrz.triplerocks.jena;

import static java.nio.charset.StandardCharsets.UTF_8;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.TypeMapper;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Node_Blank;
import org.apache.jena.graph.Node_URI;

import com.google.common.primitives.Bytes;

import zrz.triplerocks.api.TR_NodeFactory;
import zrz.triplerocks.api.TR_TermType;

/**
 * converts apache jena nodes into internal representation.
 * 
 * @author theo
 *
 */

public class JenaNodeDeserializer implements TR_NodeFactory<Node> {

  @Override
  public TR_TermType getType(Node s) {

    if (s.isBlank()) {
      return TR_TermType.BlankNode;
    }

    if (s.isLiteral()) {
      return TR_TermType.Literal;
    }

    if (s.isURI()) {
      return TR_TermType.IRI;
    }

    throw new UnsupportedOperationException("invalid type: " + s.getClass());
  }

  @Override
  public String getBlankNodeLabel(Node node) {
    return node.getBlankNodeLabel();
  }

  @Override
  public String getIRIString(Node node) {
    return node.getURI();
  }

  @Override
  public String getLexicalForm(Node node) {
    return node.getLiteralLexicalForm();
  }

  @Override
  public String getDataType(Node node) {
    return node.getLiteralDatatypeURI();
  }

  @Override
  public Node createLiteral(String lexcalForm, String dataType) {
    return NodeFactory.createLiteral(lexcalForm, TypeMapper.getInstance().getTypeByName(dataType));
  }

  @Override
  public Node createIRI(String iri) {
    return NodeFactory.createURI(iri);
  }

  @Override
  public Node createBlankNode(String label) {
    return NodeFactory.createBlankNode(label);
  }

}
