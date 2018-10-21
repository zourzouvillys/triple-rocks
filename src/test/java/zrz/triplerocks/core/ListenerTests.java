package zrz.triplerocks.core;

import static org.junit.Assert.assertEquals;

import org.apache.jena.graph.NodeFactory;
import org.junit.Test;

import zrz.triplerocks.jena.JenaRocksStore;

public class ListenerTests {

  @Test
  public void test() {

    JenaRocksStore store = new JenaRocksStore();

    long id = store.latestSequenceNumber();

    store.performAdd(NodeFactory.createURI("alice"), NodeFactory.createURI("firstName"), NodeFactory.createLiteral("Alice"));
    store.performAdd(NodeFactory.createURI("bob"), NodeFactory.createURI("firstName"), NodeFactory.createLiteral("Bob"));

    // add the listener.
    MockListener listener = new MockListener();
    store.addListener(listener, id);

    assertEquals(2, listener.model().size());

    store.performAdd(NodeFactory.createURI("charlie"), NodeFactory.createURI("firstName"), NodeFactory.createLiteral("Charlie"));

    assertEquals(3, listener.model().size());

    store.performDelete(NodeFactory.createURI("charlie"), NodeFactory.createURI("firstName"), NodeFactory.createLiteral("Charlie"));

    assertEquals(2, listener.model().size());

  }

}
