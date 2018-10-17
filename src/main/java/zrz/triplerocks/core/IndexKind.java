package zrz.triplerocks.core;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

public enum IndexKind {

  SPO,
  SOP,
  PSO,
  POS,
  OPS,
  OSP,;

  public byte[] toKey(final byte[] s, final byte[] p, final byte[] o) {

    switch (this) {
      case SPO:
        return MultiKey.create(s, p, o);
      case PSO:
        return MultiKey.create(p, s, o);
      case SOP:
        return MultiKey.create(s, o, p);
      case POS:
        return MultiKey.create(p, o, s);
      case OPS:
        return MultiKey.create(o, p, s);
      case OSP:
        return MultiKey.create(o, s, p);
      default:
        throw new IllegalArgumentException();
    }

  }

  public Triple toTriple(final Node a, final Node b, final Node c) {
    switch (this) {
      case SPO:
        return new Triple(a, b, c);
      case PSO:
        return new Triple(b, a, c);
      case SOP:
        return new Triple(a, c, b);
      case POS:
        return new Triple(c, a, b);
      case OPS:
        return new Triple(c, b, a);
      case OSP:
        return new Triple(b, c, a);
      default:
        throw new IllegalArgumentException();
    }
  }

}
