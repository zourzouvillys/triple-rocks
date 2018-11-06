package zrz.triplerocks.jena;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;

public class JenaRocksWrappedGraph extends GraphBase implements Graph {

  private JenaRocksDatasetGraph dsg;
  private Node gn;

  public JenaRocksWrappedGraph(JenaRocksDatasetGraph dsg, Node gn) {
    this.dsg = dsg;
    this.gn = gn;
  }

  @Override
  protected ExtendedIterator<Triple> graphBaseFind(Triple t) {
    return WrappedIterator.createNoRemove(dsg.findNG(gn, t.getSubject(), t.getPredicate(), t.getObject()))
      .mapWith(e -> e.asTriple());
  }

  @Override
  public void performAdd(Triple t) {
    this.dsg.add(gn, t.getSubject(), t.getPredicate(), t.getObject());
  }

  @Override
  public void performDelete(Triple t) {
    this.dsg.delete(gn, t.getSubject(), t.getPredicate(), t.getObject());
  }

}
