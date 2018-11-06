package zrz.triplerocks.jena.quadstore;

import org.apache.jena.graph.Capabilities;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.TransactionHandler;
import org.apache.jena.graph.Triple;
import org.apache.jena.graph.impl.GraphBase;
import org.apache.jena.graph.impl.TransactionHandlerBase;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.WrappedIterator;

import zrz.rocksdb.PrefixedColumnFamily;
import zrz.triplerocks.jena.JenaNodeMapper;

public class JenaQuadGraphView extends GraphBase implements Graph {

  // the backing store instance.
  private JenaQuadStore store;

  // the indexes over the triples.
  private TupleIndex indexes;

  public JenaQuadGraphView(JenaQuadStore store, PrefixedColumnFamily prefix) {
    this.store = store;
    this.indexes = new TupleIndex(new PrefixedColumnFamily(prefix, "/indexes"));
  }

  @Override
  protected ExtendedIterator<Triple> graphBaseFind(Triple triplePattern) {

    byte[] s =
      triplePattern.getMatchSubject() == null ? null
                                              : JenaNodeMapper.instance().toByteArray(triplePattern.getMatchSubject());
    byte[] p =
      triplePattern.getMatchPredicate() == null ? null
                                                : JenaNodeMapper.instance().toByteArray(triplePattern.getMatchPredicate());
    byte[] o =
      triplePattern.getMatchObject() == null ? null
                                             : JenaNodeMapper.instance().toByteArray(triplePattern.getMatchObject());

    return WrappedIterator.create(this.indexes.select(this.store.reader(), s, p, o));

  }

  @Override
  public void performAdd(final Triple t) {
    this.indexes.add(store.writer(), t);
  }

  @Override
  public final void performDelete(final Triple t) {
    this.indexes.remove(store.writer(), t);
  }

  @Override
  public TransactionHandler getTransactionHandler() {
    return new TransactionHandlerBase() {

      @Override
      public boolean transactionsSupported() {
        return true;
      }

      @Override
      public void commit() {
        store.commit();
      }

      @Override
      public void begin() {
        store.begin();
      }

      @Override
      public void abort() {
        store.abort();
      }
      
    };
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

    @Override
    public boolean addAllowed(final boolean every) {
      return true;
    }

    @Override
    public boolean deleteAllowed() {
      return true;
    }

    @Override
    public boolean deleteAllowed(final boolean every) {
      return true;
    }

    @Override
    public boolean canBeEmpty() {
      return true;
    }

    @Override
    public boolean iteratorRemoveAllowed() {
      return false;
    }

    @Override
    public boolean findContractSafe() {
      return true;
    }

    @Override
    public boolean handlesLiteralTyping() {
      return false;
    }
  };

}
