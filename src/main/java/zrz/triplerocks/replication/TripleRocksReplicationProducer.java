package zrz.triplerocks.replication;

import java.util.HashSet;
import java.util.Set;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import zrz.triplediff.DeltaBuilder;
import zrz.triplediff.DeltaTerms;
import zrz.triplediff.protobuf.TripleDiffProto.Delta;
import zrz.triplediff.protobuf.TripleDiffProto.Term;
import zrz.triplerocks.core.StoreChangeListener;

/**
 * reads store change events to generate an incremental diff which can then be sent to listeners.
 * 
 * @author theo
 *
 */

public class TripleRocksReplicationProducer implements StoreChangeListener {

  Set<Triple> tombstones = new HashSet<>();
  Set<Triple> added = new HashSet<>();
  private DeltaPublisher publisher;

  public TripleRocksReplicationProducer(DeltaPublisher publisher) {
    this.publisher = publisher;
  }

  @Override
  public void insert(Triple triple) {

    if (tombstones.contains(triple)) {
      // deleted, and then added again.
      tombstones.remove(triple);
      return;
    }

    if (added.contains(triple)) {
      // noop
      return;
    }

    added.add(triple);

  }

  private Term toDeltaTerm(Node node) {
    if (node.isURI())
      return DeltaTerms.iri(node.getURI());
    if (node.isBlank())
      return DeltaTerms.blankNode(node.getBlankNodeLabel());
    if (node.isLiteral()) {
      // urgh, need to sort out this datatype issue.
      if (node.getLiteralDatatype() == XSDDatatype.XSDstring)
        return DeltaTerms.literal(node.getLiteralLexicalForm());
      if (node.getLiteralDatatype() == XSDDatatype.XSDint)
        return DeltaTerms.literal(Long.parseLong(node.getLiteralLexicalForm()));
      if (node.getLiteralDatatype() == XSDDatatype.XSDlong)
        return DeltaTerms.literal(Long.parseLong(node.getLiteralLexicalForm()));
      if (node.getLiteralDatatype() == XSDDatatype.XSDinteger)
        return DeltaTerms.literal(Long.parseLong(node.getLiteralLexicalForm()));
      if (node.getLiteralDatatype() == XSDDatatype.XSDboolean)
        return DeltaTerms.literal(Boolean.parseBoolean(node.getLiteralLexicalForm()));
      if (node.getLiteralDatatype() == XSDDatatype.XSDdouble)
        return DeltaTerms.literal(Double.parseDouble(node.getLiteralLexicalForm()));
      //
      return DeltaTerms.literal(node.getLiteralLexicalForm(), node.getLiteralDatatypeURI());
    }
    throw new IllegalArgumentException(node.getClass().toGenericString());
  }

  @Override
  public void delete(Triple triple) {

    if (!added.remove(triple)) {

      tombstones.add(triple);

    }
    else {

      // was added then removed before we generated a delta. we can drop it.
      // note: triplerocks will only generate delete notifications for a triple which didn't already exist at the time
      // of a commit, so we don't need to worry about delete going away.

    }

  }

  @Override
  public void sync() {
    publisher.announce(createDelta().toByteArray());
  }

  /**
   * creates a deltapoint and returns it.
   */

  public Delta createDelta() {
    try {

      DeltaBuilder b = new DeltaBuilder();

      // ;
      tombstones.forEach(t -> {
        b.remove(toDeltaTerm(t.getSubject()), toDeltaTerm(t.getPredicate()), toDeltaTerm(t.getObject()));
      });

      added.forEach(t -> {
        b.add(toDeltaTerm(t.getSubject()), toDeltaTerm(t.getPredicate()), toDeltaTerm(t.getObject()));
      });

      return b.build();

    }
    finally {
      added.clear();
      tombstones.clear();
    }
  }

}
