package zrz.triplerocks.replication;

import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.NodeFactory;
import org.junit.Ignore;
import org.junit.Test;

import triplediff.protobuf.TripleDiffProto.Delta;
import zrz.triplerocks.jena.JenaRocksStore;

public class TripleRocksReplicationProduceTest {

  @Ignore
  @Test
  public void test() throws IOException {

    Path base = Files.createTempDirectory("tests");

    JenaRocksStore store = new JenaRocksStore(base.resolve("database"));

    long id = store.latestSequenceNumber();

    store.performAdd(NodeFactory.createURI("alice"), NodeFactory.createURI("firstName"), NodeFactory.createLiteral("Alice"));
    store.performAdd(NodeFactory.createURI("bob"), NodeFactory.createURI("firstName"), NodeFactory.createLiteral("Bob"));
    store.performAdd(NodeFactory.createURI("bob"), NodeFactory.createURI("age"), NodeFactory.createLiteralByValue(12, XSDDatatype.XSDinteger));
    store.performAdd(NodeFactory.createURI("charlie"), NodeFactory.createURI("firstName"), NodeFactory.createLiteral("Charlie"));

    // add the listener.
    TripleRocksReplicationProducer listener = new TripleRocksReplicationProducer(new MockPublisher());

    store.addListener(listener, id);

    store.performDelete(NodeFactory.createURI("charlie"), NodeFactory.createURI("firstName"), NodeFactory.createLiteral("Charlie"));
    store.performAdd(NodeFactory.createURI("charlie"), NodeFactory.createURI("firstName"), NodeFactory.createLiteral("Charlie"));
    
    Delta delta = listener.createDelta();

    assertEquals(3, delta.getDiffsCount());

  }

}
