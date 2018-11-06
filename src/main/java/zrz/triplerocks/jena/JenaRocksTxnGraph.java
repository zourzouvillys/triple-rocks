package zrz.triplerocks.jena;

import static com.google.common.base.Preconditions.checkState;

import java.util.function.Supplier;

import org.apache.jena.graph.Capabilities;
import org.apache.jena.graph.TransactionHandler;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.shared.Command;
import org.apache.jena.util.iterator.ExtendedIterator;

import zrz.triplerocks.core.TripleRocksTxn;

/**
 * lightweight adapter which converts Jena calls to the core engine.
 *
 * @author theo
 *
 */
public class JenaRocksTxnGraph extends GraphBase implements TransactionHandler {

  private final JenaRocksStore store;
  private TripleRocksTxn txn;

  public JenaRocksTxnGraph(final JenaRocksStore store) {
    this.store = store;
  }

  @Override
  protected ExtendedIterator<Triple> graphBaseFind(final Triple triplePattern) {
    return this.store.query(txn, triplePattern.getMatchSubject(), triplePattern.getMatchPredicate(), triplePattern.getMatchObject());
  }

  @Override
  public void performAdd(final Triple t) {
    this.store.performAdd(txn, t.getSubject(), t.getPredicate(), t.getObject());
  }

  @Override
  public final void performDelete(final Triple t) {
    this.store.performDelete(txn, t.getSubject(), t.getPredicate(), t.getObject());
  }

  @Override
  public TransactionHandler getTransactionHandler() {
    return this;
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

  @Override
  public boolean transactionsSupported() {
    return true;
  }

  @Override
  public void begin() {
    checkState(this.txn == null, "txn in progress");
    this.txn = this.store.createTransaction();
  }

  @Override
  public void abort() {
    checkState(this.txn != null, "txn not in progress");
    this.txn.abort();
    this.txn = null;
  }

  @Override
  public void commit() {
    checkState(this.txn != null, "txn not in progress");
    this.txn.commit();
    this.txn = null;
  }

  @Override
  public Object executeInTransaction(Command c) {
    return c.execute();
  }

  @Override
  public void execute(Runnable action) {
    action.run();
  }

  @Override
  public <T> T calculate(Supplier<T> action) {
    // ${todo} Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented Method: ${enclosing_type}.${enclosing_method} invoked.");
  }

}
