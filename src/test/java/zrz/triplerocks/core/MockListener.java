package zrz.triplerocks.core;

import org.apache.jena.graph.Triple;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.impl.StatementImpl;

public class MockListener implements StoreChangeListener {

  private Model model;

  MockListener() {
    this.model = ModelFactory.createDefaultModel();
  }

  @Override
  public void insert(Triple t) {
    System.err.println("INSERT: " + t);
    model.add(this.model.asStatement(t));
  }

  @Override
  public void delete(Triple t) {
    System.err.println("DELETE: " + t);
    model.remove(this.model.asStatement(t));
  }

  @Override
  public void sync() {
    System.err.println("SYNC");
  }

  public Model model() {
    return this.model;
  }

}
