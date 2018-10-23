package zrz.triplerocks.benchmarks;

import java.nio.file.Path;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.rocksdb.HistogramType;

import com.google.common.collect.Iterators;
import com.google.common.io.Files;

import zrz.triplerocks.jena.JenaRocksStore;

public class WriteBenchmarks {

  public void testWrites1() throws InterruptedException {

    Path path = Files.createTempDir().toPath();

    JenaRocksStore store = new JenaRocksStore(path);

    System.err.println(path);

    AtomicBoolean writting = new AtomicBoolean(true);
    AtomicInteger pos = new AtomicInteger();

    Thread reader = new Thread(() -> {

      int hits = 0;
      int misses = 0;

      while (writting.get()) {

        int rand = ThreadLocalRandom.current().nextInt(1, 1_000_000);

        ExtendedIterator<Triple> it = store.query(NodeFactory.createURI("subject-" + rand), null, null);

        int size = Iterators.size(it);

        if (size > 0) {
          hits++;
        }
        else {
          misses++;
        }

        it.close();

      }

      System.err.println("hits: " + hits);
      System.err.println("misses: " + misses);

    });

    reader.start();

    try {

      for (int i = 0; i < 1_000_000; ++i) {

        long id = pos.getAndIncrement();

        store.performAdd(
            NodeFactory.createURI("subject-" + id),
            NodeFactory.createURI("predicate-" + id),
            NodeFactory.createURI("object-" + id));

      }
    }
    finally {
      writting.set(false);
    }

    reader.join();

    System.err.println(store.stats(HistogramType.DB_WRITE).getAverage());
    System.err.println(store.property("rocksdb.stats"));

  }

}
