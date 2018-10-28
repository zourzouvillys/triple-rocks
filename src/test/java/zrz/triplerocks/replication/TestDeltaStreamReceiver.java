package zrz.triplerocks.replication;

import org.apache.jena.rdf.model.AnonId;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.rdf.model.Resource;

import triplediff.protobuf.TripleDiffProto.Term;
import zrz.triplediff.DeltaStream;

public class TestDeltaStreamReceiver implements DeltaStream {

  private Model m;

  public TestDeltaStreamReceiver(Model m) {
    this.m = m;
  }

  @Override
  public void prefixAdd(String prefix, String iri) {
    m.setNsPrefix(prefix, iri);
  }

  @Override
  public void prefixRemove(String prefix, String iri) {
    m.removeNsPrefix(prefix);
  }

  @Override
  public void add(Term subject, Term predicate, Term object) {
    m.add(toResource(subject), toProperty(predicate), toNode(object));
  }

  @Override
  public void remove(Term subject, Term predicate, Term object) {
    m.remove(toResource(subject), toProperty(predicate), toNode(object));
  }

  public Resource toResource(Term term) {
    switch (term.getValueCase()) {
      case BLANK_NODE:
        return m.createResource(AnonId.create(term.getBlankNode()));
      case IRI:
        return m.createResource(term.getIri());
      case PREFIXED_NAME:
        return m.createResource(m.getNsPrefixURI(term.getPrefixedName().getPrefix()) + term.getPrefixedName().getLocalName());
      default:
        throw new IllegalArgumentException(term.getValueCase().toString());
    }
  }

  public Property toProperty(Term term) {
    switch (term.getValueCase()) {
      case IRI:
        return m.createProperty(term.getIri());
      case PREFIXED_NAME:
        return m.createProperty(term.getPrefixedName().getPrefix(), term.getPrefixedName().getLocalName());
      default:
        throw new IllegalArgumentException(term.getValueCase().toString());
    }
  }

  public RDFNode toNode(Term term) {
    switch (term.getValueCase()) {
      case BLANK_NODE:
        return m.createResource(AnonId.create(term.getBlankNode()));
      case IRI:
        return m.createResource(term.getIri());
      case PREFIXED_NAME:
        return m.createResource(m.getNsPrefixURI(term.getPrefixedName().getPrefix()) + term.getPrefixedName().getLocalName());
      case LITERAL:
        return m.createTypedLiteral(term.getLiteral().getLexicalForm().toStringUtf8(), term.getLiteral().getDataType());
      case BOOLEANLITERAL:
        return m.createTypedLiteral(term.getBooleanLiteral());
      case DOUBLELITERAL:
        return m.createTypedLiteral(term.getDoubleLiteral());
      case INTEGERLITERAL:
        return m.createTypedLiteral(term.getIntegerLiteral());
      case STRINGLITERAL:
        return m.createTypedLiteral(term.getStringLiteral());
      default:
        throw new IllegalArgumentException(term.getValueCase().toString());
    }
  }

}
