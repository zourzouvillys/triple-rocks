package zrz.triplerocks.jena;

import static com.google.common.base.Preconditions.checkState;
import static zrz.triplerocks.jena.JenaMultiKey.toKey;

import java.util.Arrays;
import java.util.Iterator;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphBase;
import org.apache.jena.sparql.core.Quad;

import zrz.triplerocks.core.TripleRocksTxn;

public class JenaRocksDatasetGraph extends DatasetGraphBase implements DatasetGraph {

  private JenaRocksStore store;
  private TripleRocksTxn txn;

  public JenaRocksDatasetGraph(JenaRocksStore store) {
    this.store = store;
  }

  /**
   * 
   */

  @Override
  public void begin(TxnType type) {
    checkState(this.txn == null);
    this.txn = store.createTransaction();
  }

  @Override
  public void begin(ReadWrite readWrite) {
    checkState(this.txn == null);
    this.txn = store.createTransaction();
  }

  @Override
  public boolean promote(Promote mode) {
    // TODO
    return true;
  }

  @Override
  public void commit() {
    checkState(this.txn != null);
    this.txn.commit();
    this.txn = null;
  }

  @Override
  public void abort() {
    checkState(this.txn != null);
    this.txn.abort();
    this.txn = null;
  }

  @Override
  public void end() {
    if (this.txn != null) {
      this.abort();
    }
  }

  @Override
  public ReadWrite transactionMode() {
    return ReadWrite.WRITE;
  }

  @Override
  public TxnType transactionType() {
    return TxnType.WRITE;
  }

  @Override
  public boolean isInTransaction() {
    return this.txn != null;
  }

  @Override
  public boolean supportsTransactions() {
    return true;
  }

  //// -------------------------------------------------------
  ////
  //// -------------------------------------------------------

  @Override
  public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
    if (this.txn != null) {
      return this.store.query(txn, s, p, o).mapWith(t -> Quad.create(g, t));
    }
    return this.store.query(s, p, o).mapWith(t -> Quad.create(g, t));
  }

  @Override
  public Iterator<Quad> findNG(Node g, Node s, Node p, Node o) {
    if (this.txn != null) {
      return this.store.query(txn, s, p, o).mapWith(t -> Quad.create(g, t));
    }
    return this.store.query(s, p, o).mapWith(t -> Quad.create(g, t));
  }

  //// -------------------------------------------------------
  ////
  //// -------------------------------------------------------

  @Override
  public void add(Quad q) {
    if (this.txn != null) {
      this.txn.insert(toKey(q.getSubject()), toKey(q.getPredicate()), toKey(q.getObject()));
    }
    else {
      this.store.performAdd(q.getSubject(), q.getPredicate(), q.getObject());
    }
  }

  @Override
  public void delete(Quad q) {
    if (this.txn != null) {
      this.txn.delete(toKey(q.getSubject()), toKey(q.getPredicate()), toKey(q.getObject()));
    }
    else {
      this.store.performDelete(q.getSubject(), q.getPredicate(), q.getObject());
    }
  }

  //// -------------------------------------------------------
  ////
  //// -------------------------------------------------------

  @Override
  public Iterator<Node> listGraphNodes() {
    return Arrays.asList(NodeFactory.createBlankNode())
      .iterator();
  }

  @Override
  public Graph getDefaultGraph() {
    return new JenaRocksWrappedGraph(this, null);
  }

  @Override
  public Graph getGraph(Node graphNode) {
    return new JenaRocksWrappedGraph(this, graphNode);
  }

  @Override
  public void addGraph(Node graphName, Graph graph) {
    throw new UnsupportedOperationException("addgraph");
  }

  @Override
  public void removeGraph(Node graphName) {
    // ${todo} Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented Method: ${enclosing_type}.${enclosing_method} invoked.");
  }

}
