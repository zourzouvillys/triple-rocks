package zrz.triplerocks.jena;

import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.util.iterator.ExtendedIterator;

/**
 * lightweight adapter which converts Jena calls to the core engine.
 *
 * @author theo
 *
 */
public class JenaRocksGraph extends GraphBase {

  private final JenaRocksStore store;

  public JenaRocksGraph(final JenaRocksStore store) {
    this.store = store;
  }

  @Override
  protected ExtendedIterator<Triple> graphBaseFind(final Triple triplePattern) {
    return this.store.query(triplePattern.getMatchSubject(), triplePattern.getMatchPredicate(), triplePattern.getMatchObject());
  }

  @Override
  public void performAdd(final Triple t) {
    this.store.performAdd(t.getSubject(), t.getPredicate(), t.getObject());
  }

  @Override
  public final void performDelete(final Triple t) {
    this.store.performDelete(t.getSubject(), t.getPredicate(), t.getObject());
  }

}
