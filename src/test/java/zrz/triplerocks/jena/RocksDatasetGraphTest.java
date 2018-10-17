package zrz.triplerocks.jena;

import org.apache.jena.atlas.io.IndentedWriter;
import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.algebra.Algebra;
import org.apache.jena.sparql.algebra.Op;
import org.apache.jena.sparql.algebra.op.OpBGP;
import org.apache.jena.sparql.core.BasicPattern;
import org.apache.jena.sparql.core.DatasetGraph;
import org.apache.jena.sparql.core.DatasetGraphFactory;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.core.Var;
import org.apache.jena.sparql.engine.Plan;
import org.apache.jena.sparql.engine.QueryEngineFactory;
import org.apache.jena.sparql.engine.QueryEngineRegistry;
import org.apache.jena.sparql.engine.QueryIterator;
import org.apache.jena.sparql.engine.binding.Binding;
import org.apache.jena.sparql.engine.binding.BindingHashMap;
import org.apache.jena.sparql.sse.WriterSSE;
import org.junit.Test;

import com.google.common.io.Files;

public class RocksDatasetGraphTest {

  @Test
  public void test() {

    final Var sVar = Var.alloc("s");
    final Var pVar = Var.alloc("p");
    final Var oVar = Var.alloc("o");

    final Node subject = sVar;
    final Node predicate = pVar;
    final Node object = NodeFactory.createLiteral("Zourzouvillys");

    final BasicPattern bgp = new BasicPattern();

    bgp.add(new Triple(subject, predicate, object));

    Op op = new OpBGP(bgp);

    // limit to just one.

    // op = new OpSlice(op, 0, 1);

    // perform optimization pass.
    op = Algebra.optimize(op);

    // to debug ...
    final IndentedWriter out = IndentedWriter.stderr;
    WriterSSE.out(out, op, new Prologue());
    out.flush();

    //
    final DatasetGraph ds = DatasetGraphFactory.wrap(createGraph());

    final Binding binding = new BindingHashMap();

    final QueryEngineFactory f = QueryEngineRegistry.findFactory(op, ds, null);

    final Plan plan = f.create(op, ds, binding, null);

    plan.output(IndentedWriter.stdout);

    ds.begin(ReadWrite.READ);

    try {

      final QueryIterator it = plan.iterator();

      final boolean found = it.hasNext();

      if (found) {
        while (it.hasNext()) {
          final Binding bindings = it.nextBinding();
          System.err.println(bindings.get(sVar) + " " + bindings.get(pVar) + " " + bindings.get(oVar));
        }
      }
      else {
        System.err.println("no match");
      }

      it.close();

    }
    finally {

      ds.end();

    }

  }

  private static Graph createGraph() {

    final JenaRocksStore store = new JenaRocksStore(Files.createTempDir().toPath());
    final JenaRocksGraph graph = new JenaRocksGraph(store);

    final Model m = ModelFactory.createModelForGraph(graph);

    m.begin();

    graph.add(new Triple(
        NodeFactory.createURI("theo"),
        NodeFactory.createURI("firstName"),
        NodeFactory.createLiteral("Ttheo")));
    graph.delete(new Triple(
        NodeFactory.createURI("theo"),
        NodeFactory.createURI("firstName"),
        NodeFactory.createLiteral("Ttheo")));
    graph.add(new Triple(
        NodeFactory.createURI("theo"),
        NodeFactory.createURI("firstName"),
        NodeFactory.createLiteral("Theo")));
    graph.add(new Triple(
        NodeFactory.createURI("theo"),
        NodeFactory.createURI("lastName"),
        NodeFactory.createLiteral("Zzzourzouvillys")));
    graph.delete(new Triple(
        NodeFactory.createURI("theo"),
        NodeFactory.createURI("lastName"),
        NodeFactory.createLiteral("Zzzourzouvillys")));
    graph.add(new Triple(
        NodeFactory.createURI("theo"),
        NodeFactory.createURI("lastName"),
        NodeFactory.createLiteral("Zourzouvillys")));
    graph.add(new Triple(
        NodeFactory.createURI("alice"),
        NodeFactory.createURI("firstName"),
        NodeFactory.createLiteral("Alice")));

    m.commit();

    return graph;
  }

}
