package zrz.triplerocks.jena.quadstore;

import java.util.Iterator;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.TxnType;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphBase;
import org.apache.jena.sparql.core.Quad;

import com.google.common.base.Preconditions;
import com.google.common.base.Verify;

import zrz.rocksdb.JRocksBatchWithIndex;
import zrz.rocksdb.JRocksColumnFamily;
import zrz.rocksdb.JRocksDirectSet;
import zrz.rocksdb.JRocksEngine;
import zrz.rocksdb.JRocksReadableWriter;
import zrz.rocksdb.JRocksWriter;
import zrz.rocksdb.PrefixedColumnFamily;
import zrz.triplerocks.jena.JenaNodeMapper;

/**
 * a single backing rocksDB instance can store multiple graphs.
 * 
 * there are multiple potential configurations; there could be a CF per graph dataset, using
 * prefixes to seperate the data and the index. or there could be a CF per dataset, and a CF per
 * index. or a CF could contain a bunch of datasets and seperate CF for all of the indexes, prefixed
 * by a graph identifier.
 * 
 * we chose the most simple implementation. all data is stored in a single CF, and a prefix is used
 * to identify the data vs the indexes. this makes management super easy, but has some
 * scalability/peformance limitations.
 * 
 * the triples are stored with a graph context prefix which indicates the graph it is contained it.
 * all indexes have the context appended. this allows us to scan for values across all contexts.
 * 
 * A "union" view is also available.
 * 
 * 
 * @author theo
 *
 */

public class JenaQuadStore extends DatasetGraphBase implements DatasetGraph {

  // the underlying datastore. currently can't host across multiple stores - but
  // multiple CFs are ok.
  private JRocksEngine engine;

  private JRocksBatchWithIndex batch;

  // all of the graphs in this set.
  private JRocksDirectSet<Node> meta;

  // all of the quads.
  private JRocksDirectSet<Quad> quads;

  // the prefix for named graph datasets.
  private PrefixedColumnFamily namedGraphPrefix;

  public JenaQuadStore(JRocksEngine engine) {
    this.engine = engine;
    this.meta = new JRocksDirectSet<>(JenaNodeMapper.instance(), new PrefixedColumnFamily(engine.columnFamily("default"), "meta"));
    this.namedGraphPrefix = new PrefixedColumnFamily(engine.columnFamily("default"), "graphs/");
  }

  @Override
  public void begin(TxnType type) {
    Preconditions.checkState(this.batch == null);
    this.batch = new JRocksBatchWithIndex(engine);
  }

  @Override
  public void begin(ReadWrite readWrite) {
    Preconditions.checkState(this.batch == null);
    this.batch = new JRocksBatchWithIndex(engine);
  }

  @Override
  public boolean promote(Promote mode) {
    Preconditions.checkState(this.batch != null);
    return true;
  }

  @Override
  public void commit() {
    Preconditions.checkState(this.batch != null);
    this.engine.accept(this.batch);
    this.batch = null;
  }

  @Override
  public void abort() {
    Preconditions.checkState(this.batch != null);
    this.batch.close();
    this.batch = null;
  }

  @Override
  public void end() {
    if (this.batch != null) {
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
    return this.batch != null;
  }

  @Override
  public boolean supportsTransactions() {
    return true;
  }

  @Override
  public void add(Quad quad) {
    this.quads.put(this.writer(), quad);
  }

  @Override
  public void delete(Quad quad) {
    this.quads.delete(this.writer(), quad);
  }

  void checkGraphNode(Node namedGraph) {
    Preconditions.checkArgument(namedGraph.isConcrete());
    Preconditions.checkArgument(namedGraph.isURI() || namedGraph.isBlank());
  }

  /**
   * Find matching quads in the entire dataset (including default graph) - may include wildcards,
   * Node.ANY or null.
   */

  @Override
  public Iterator<Quad> find(Node g, Node s, Node p, Node o) {
    throw new UnsupportedOperationException("JenaQuadStore.find");
  }

  /**
   * Find matching quads in the dataset in named graphs only - may include wildcards, Node.ANY or
   * null.
   * 
   * we map to the indexes that are created for each of the store types.
   * 
   */

  @Override
  public Iterator<Quad> findNG(Node g, Node s, Node p, Node o) {
    throw new UnsupportedOperationException("JenaQuadStore.findNG");
  }

  JRocksReadableWriter reader() {
    return this.batch == null ? this.engine
                              : this.batch;
  }

  JRocksWriter writer() {
    return Verify.verifyNotNull(batch, "batch");
  }

  /**
   * list each graph in this store.
   */

  @Override
  public Iterator<Node> listGraphNodes() {
    return this.meta.createIterator(this.reader()).toList().iterator();
  }

  @Override
  public Graph getDefaultGraph() {
    throw new UnsupportedOperationException("no default graph");
  }

  /**
   * return a handle which represents a named Graph.
   * 
   * the returned graph will use the underlying transaction that this instance uses, and can not be
   * changed directly.
   * 
   */

  @Override
  public Graph getGraph(Node graphNode) {
    checkGraphNode(graphNode);
    PrefixedColumnFamily cf =
      new PrefixedColumnFamily(
        this.namedGraphPrefix,
        JenaNodeMapper.instance().toByteArray(graphNode));
    return new JenaQuadGraphView(this, cf);
  }

  @Override
  public void addGraph(Node graphNode, Graph graph) {
    Verify.verifyNotNull(graphNode, "graphName");
    checkGraphNode(graphNode);
    Verify.verifyNotNull(graph, "graph");
    this.meta.put(this.writer(), graphNode);
    // add all the triples into this graph.

    Graph g = this.getGraph(graphNode);
    graph.find()
      .forEachRemaining(t -> g.add(t));
    g.close();

  }

  /**
   * Remove all data associated with the named graph. This will include prefixes associated with the
   * graph.
   */

  @Override
  public void removeGraph(Node graphName) {
    throw new UnsupportedOperationException("JenaQuadStore.removeGraph");
  }

}
