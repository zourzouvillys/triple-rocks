package zrz.triplerocks.commons;

import org.apache.commons.rdf.api.BlankNode;
import org.apache.commons.rdf.api.IRI;
import org.apache.commons.rdf.api.Literal;
import org.apache.commons.rdf.api.RDF;
import org.apache.commons.rdf.api.RDFTerm;

import zrz.triplerocks.api.TR_NodeFactory;
import zrz.triplerocks.api.TR_TermType;

/**
 * a mapper for RDF commons.
 * 
 * @author theo
 *
 */

public class RDFCommonsConverter implements TR_NodeFactory<RDFTerm> {

  private RDF rdf;

  public RDFCommonsConverter(RDF rdf) {
    this.rdf = rdf;
  }

  @Override
  public Literal createLiteral(String lexcalForm, String dataType) {
    return rdf.createLiteral(lexcalForm, dataType);
  }

  @Override
  public IRI createIRI(String iri) {
    return rdf.createIRI(iri);
  }

  @Override
  public BlankNode createBlankNode(String label) {
    return rdf.createBlankNode(label);
  }

  @Override
  public TR_TermType getType(RDFTerm type) {
    if (type instanceof IRI) {
      return TR_TermType.IRI;
    }
    else if (type instanceof Literal) {
      return TR_TermType.Literal;
    }
    else if (type instanceof BlankNode) {
      return TR_TermType.BlankNode;
    }
    throw new IllegalArgumentException(type.getClass().toString());
  }

  @Override
  public String getBlankNodeLabel(RDFTerm node) {
    return ((BlankNode) node).uniqueReference();
  }

  @Override
  public String getIRIString(RDFTerm node) {
    return ((IRI) node).getIRIString();
  }

  @Override
  public String getLexicalForm(RDFTerm node) {
    return ((Literal) node).getLexicalForm();
  }

  @Override
  public String getDataType(RDFTerm node) {
    return ((Literal) node).getDatatype().getIRIString();
  }

}
