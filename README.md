# RocksDB Triplestore

a simple triplestore backed by RocksDB, with an adapter for Apache Jena.

```

// create the store.
final JenaRocksStore store = new JenaRocksStore(Paths.get("/my/store"));

// create a graph wrapper.
final JenaRocksGraph graph = new JenaRocksGraph(store);

// use as a DatasetGraph if you want
final DatasetGraph ds = DatasetGraphFactory.wrap(graph);


```
