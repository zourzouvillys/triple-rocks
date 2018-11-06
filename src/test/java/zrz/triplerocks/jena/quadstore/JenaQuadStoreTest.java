package zrz.triplerocks.jena.quadstore;

import static org.junit.Assert.*;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Node_ANY;
import org.apache.jena.graph.Triple;
import org.apache.jena.sparql.graph.GraphFactory;
import org.junit.Test;

import zrz.rocksdb.JRocksEngine;

public class JenaQuadStoreTest {

  @Test
  public void test() {

    JenaQuadStore store = new JenaQuadStore(JRocksEngine.createInMemory());

    store.begin();

    store.listGraphNodes()
      .forEachRemaining(System.err::println);

    Graph initial = GraphFactory.createGraphMem();
    initial
      .add(
        Triple.create(
          NodeFactory.createURI("alice"),
          NodeFactory.createURI("name"),
          NodeFactory.createURI("Alice")));

    store.addGraph(
      NodeFactory.createURI("test-graph"),
      initial);

    store.listGraphNodes()
      .forEachRemaining(System.err::println);

    Graph g = store.getGraph(NodeFactory.createURI("test-graph"));

    g
      .add(
        Triple.create(
          NodeFactory.createURI("bob"),
          NodeFactory.createURI("name"),
          NodeFactory.createURI("Bob")));

    g.find()
      .forEachRemaining(System.err::println);

    System.err.println("----");
    g.find(Node_ANY.ANY, null, NodeFactory.createURI("Bob"))
      .forEachRemaining(System.err::println);
    System.err.println("----");

    // store.find()
    // .forEachRemaining(System.err::println);

    store.commit();

    store.getGraph(NodeFactory.createURI("test-graph"))
      .find()
      .forEachRemaining(System.err::println);

  }

}
