package zrz.triplerocks.jena;

import org.apache.jena.graph.Capabilities;
import org.apache.jena.graph.TransactionHandler;
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

  @Override
  public TransactionHandler getTransactionHandler() {
    return new JenaRocksTransactionHandler(this);
  }

  @Override
  public Capabilities getCapabilities() {
    return CAPABILITIES;
  }

  /**
   * capabilities for JenaGraph.
   */
  private static final Capabilities CAPABILITIES = new Capabilities() {

    @Override
    public boolean sizeAccurate() {
      return true;
    }

    @Override
    public boolean addAllowed() {
      return true;
    }

    @SuppressWarnings("deprecation")
    @Override
    public boolean addAllowed(final boolean every) {
      return true;
    }

    @Override
    public boolean deleteAllowed() {
      return true;
    }

    @SuppressWarnings("deprecation")
    @Override
    public boolean deleteAllowed(final boolean every) {
      return true;
    }

    @SuppressWarnings("deprecation")
    @Override
    public boolean canBeEmpty() {
      return true;
    }

    @SuppressWarnings("deprecation")
    @Override
    public boolean iteratorRemoveAllowed() {
      return false;
    }

    @SuppressWarnings("deprecation")
    @Override
    public boolean findContractSafe() {
      return true;
    }

    @Override
    public boolean handlesLiteralTyping() {
      return false;
    }
  };

  public JenaRocksStore store() {
    return this.store;
  }

}
