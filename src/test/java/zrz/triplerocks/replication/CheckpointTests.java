package zrz.triplerocks.replication;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.NodeFactory;
import org.junit.Test;

import zrz.triplerocks.jena.JenaRocksStore;

public class CheckpointTests {

  @Test
  public void test() throws IOException {

    Path base = Files.createTempDirectory("tests");

    JenaRocksStore store = new JenaRocksStore(base.resolve("database"));

    store.performAdd(NodeFactory.createURI("alice"), NodeFactory.createURI("firstName"), NodeFactory.createLiteral("Alice"));
    store.performAdd(NodeFactory.createURI("bob"), NodeFactory.createURI("firstName"), NodeFactory.createLiteral("Bob"));
    store.performAdd(NodeFactory.createURI("bob"), NodeFactory.createURI("age"), NodeFactory.createLiteralByValue(12, XSDDatatype.XSDinteger));
    store.performAdd(NodeFactory.createURI("charlie"), NodeFactory.createURI("firstName"), NodeFactory.createLiteral("Charlie"));
    store.performDelete(NodeFactory.createURI("charlie"), NodeFactory.createURI("firstName"), NodeFactory.createLiteral("Charlie"));
    store.performAdd(NodeFactory.createURI("charlie"), NodeFactory.createURI("firstName"), NodeFactory.createLiteral("Charlie"));

    Path checkpointDir = Files.createDirectories(base.resolve("checkpoints"));

    Path result = store.checkpoint(checkpointDir);

    System.err.println(result);

  }

}
