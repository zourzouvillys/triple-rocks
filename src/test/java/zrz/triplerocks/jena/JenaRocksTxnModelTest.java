package zrz.triplerocks.jena;

import static org.apache.jena.rdf.model.ModelFactory.createModelForGraph;
import static org.junit.Assert.assertEquals;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.junit.Test;

import zrz.triplerocks.jena.JenaRocksDatasetGraph;
import zrz.triplerocks.jena.JenaRocksStore;

public class JenaRocksTxnModelTest {

  @Test
  public void testCommit() {
    
    JenaRocksStore store = new JenaRocksStore();
    
    Model m = ModelFactory.createModelForGraph(new JenaRocksTxnGraph(store));
    m.begin();
    m.add(m.createResource("ID"), m.createProperty("xyz"), m.createLiteral("theo"));
    assertEquals(1, m.listStatements().toList().size());
    assertEquals(1, m.listResourcesWithProperty(m.createProperty("xyz")).toList().size());
    m.commit();
    assertEquals(1, store.all().toList().size());
    assertEquals(1, m.listResourcesWithProperty(m.createProperty("xyz")).toList().size());
  }

  @Test
  public void testAbort() {
    JenaRocksStore store = new JenaRocksStore();
    Model m = ModelFactory.createModelForGraph(new JenaRocksTxnGraph(store));
    m.begin();
    m.add(m.createResource("ID"), m.createProperty("xyz"), m.createLiteral("theo"));
    m.abort();
    assertEquals(0, store.all().toList().size());
  }

}
