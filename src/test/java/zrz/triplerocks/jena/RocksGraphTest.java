package zrz.triplerocks.jena;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.util.Optional;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.junit.Test;

import com.google.common.io.Files;

public class RocksGraphTest {

  @Test
  public void test() {

    final File dir = Files.createTempDir();

    final JenaRocksStore store = new JenaRocksStore(dir.toPath());

    final JenaRocksGraph g = new JenaRocksGraph(store);

    final Triple t1 = new Triple(
        NodeFactory.createURI("theo"),
        NodeFactory.createURI("urn:firstName"),
        NodeFactory.createLiteral("Theo"));

    g.add(t1);

    assertEquals(Optional.of(t1), g.find(t1).nextOptional());

    final ExtendedIterator<Triple> it = store.all();

    assertTrue(it.hasNext());

    final Triple val = it.next();

    assertEquals(NodeFactory.createURI("theo"), val.getSubject());
    assertEquals(NodeFactory.createURI("urn:firstName"), val.getPredicate());
    assertEquals(NodeFactory.createLiteral("Theo"), val.getObject());

    assertFalse(it.hasNext());

  }

}
