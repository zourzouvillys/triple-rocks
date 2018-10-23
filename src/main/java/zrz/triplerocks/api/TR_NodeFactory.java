package zrz.triplerocks.api;

/**
 * provides mapping between binary representation of nodes and a java type.
 * 
 * annoyingly we have a bunch of different implementations. apache commons RDF tries to fix this, but until all the
 * implementations use it ...
 * 
 * A converter will be given the type information, and should materialize a java node from it. TripleRocks may cache the
 * result.
 * 
 */

public interface TR_NodeFactory<NodeT> {

  /**
   * create a node representing a literal value.
   * 
   * @param lexcalForm
   *          the lexical form of this literal.
   * @param dataType
   *          the datatype of this literal.
   * @return
   */

  NodeT createLiteral(String lexcalForm, String dataType);

  /**
   * create a node representing given IRI.
   */

  NodeT createIRI(String iri);

  /**
   * create a node representing given blank node.
   */

  NodeT createBlankNode(String label);

  /**
   * provide the type of this node.
   */

  TR_TermType getType(NodeT type);

  /**
   * return the blank node label as a string.
   */

  String getBlankNodeLabel(NodeT node);

  /**
   * return the IRI itself as a string.
   */

  String getIRIString(NodeT node);

  /**
   * the node is a literal value, return the lexical form (not including the type or quotes).
   */

  String getLexicalForm(NodeT node);

  /**
   * the node is a literal. return the datatype identifier.
   */

  String getDataType(NodeT node);

}
