package zrz.triplerocks.jena;

import static org.apache.jena.rdf.model.ModelFactory.createModelForGraph;
import static org.junit.Assert.assertEquals;

import org.apache.jena.rdf.model.Model;
import org.junit.Test;

import zrz.triplerocks.jena.JenaRocksDatasetGraph;
import zrz.triplerocks.jena.JenaRocksStore;

public class JenaRocksDatasetGraphTest {

  @Test
  public void testCommit() {
    JenaRocksStore store = new JenaRocksStore();
    JenaRocksDatasetGraph g = new JenaRocksDatasetGraph(store);
    g.begin();
    Model m = createModelForGraph(g.getDefaultGraph());
    m.add(m.createResource("ID"), m.createProperty("xyz"), m.createLiteral("theo"));
    assertEquals(1, m.listStatements().toList().size());
    assertEquals(1, m.listResourcesWithProperty(m.createProperty("xyz")).toList().size());
    g.commit();
    assertEquals(1, store.all().toList().size());
    assertEquals(1, m.listResourcesWithProperty(m.createProperty("xyz")).toList().size());
  }

  @Test
  public void testAbort() {
    JenaRocksStore store = new JenaRocksStore();
    JenaRocksDatasetGraph g = new JenaRocksDatasetGraph(store);
    g.begin();
    Model m = createModelForGraph(g.getDefaultGraph());
    m.add(m.createResource("ID"), m.createProperty("xyz"), m.createLiteral("theo"));
    g.abort();
    assertEquals(0, store.all().toList().size());
  }

}
