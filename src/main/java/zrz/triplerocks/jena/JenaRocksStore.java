package zrz.triplerocks.jena;

import java.nio.file.Path;

import org.apache.jena.graph.Graph;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NullIterator;
import org.apache.jena.util.iterator.SingletonIterator;
import org.rocksdb.HistogramData;
import org.rocksdb.HistogramType;
import org.rocksdb.RocksDBException;

import zrz.triplerocks.core.BaseRocksTripleStore;
import zrz.triplerocks.core.IndexKind;
import zrz.triplerocks.core.MultiKey;
import zrz.triplerocks.core.TripleRocksAPI;

public class JenaRocksStore extends BaseRocksTripleStore {

  public JenaRocksStore() {
    super();
  }

  public JenaRocksStore(final Path path) {
    super(path);
  }

  public ExtendedIterator<Triple> all(TripleRocksAPI api) {
    return new JenaRocksIterator(api.createIterator(IndexKind.SPO), null, IndexKind.SPO);
  }

  public ExtendedIterator<Triple> all() {
    return all(JenaRocksTransactionHandler.currentTxn(this));
  }

  public ExtendedIterator<Triple> query(final Node sn, final Node pn, final Node on) {
    return query(JenaRocksTransactionHandler.currentTxn(this), sn, pn, on);
  }

  public ExtendedIterator<Triple> query(TripleRocksAPI api, Node sn, final Node pn, final Node on) {

    final byte[] s = toKey(sn);
    final byte[] p = toKey(pn);
    final byte[] o = toKey(on);

    if ((s != null) && (p != null) && (o != null)) {

      // best case, direct single hit.
      if (api.contains(s, p, o)) {
        return new SingletonIterator<>(new Triple(sn, pn, on));
      }

      return NullIterator.instance();

    }
    else if ((s != null) && (p != null)) {

      final byte[] key = MultiKey.create(s, p);
      return this.query(api, IndexKind.SPO, key);

    }
    else if ((s != null) && (o != null)) {

      final byte[] key = MultiKey.create(s, o);
      return this.query(api, IndexKind.SOP, key);

    }
    else if ((p != null) && (o != null)) {

      final byte[] key = MultiKey.create(p, o);
      return this.query(api, IndexKind.POS, key);

    }
    else if (s != null) {

      return this.query(api, IndexKind.SPO, MultiKey.create(s));

    }
    else if (p != null) {

      return this.query(api, IndexKind.PSO, MultiKey.create(p));

    }
    else if (o != null) {

      return this.query(api, IndexKind.OPS, MultiKey.create(o));

    }
    else {

      return this.all(api);

    }
  }

  public JenaRocksIterator query(TripleRocksAPI api, final IndexKind index, final byte[] key) {
    if (api == null) {
      return query(JenaRocksTransactionHandler.currentTxn(this), index, key);
    }
    return new JenaRocksIterator(api.createIterator(index), key, index);
  }

  public JenaRocksIterator query(final IndexKind index, final byte[] key) {
    return query(JenaRocksTransactionHandler.currentTxn(this), index, key);
  }

  private static byte[] toKey(final Node n) {
    if ((n == null) || (n == Node.ANY)) {
      return null;
    }
    return JenaMultiKey.toKey(n);
  }

  public void performAdd(final Node s, final Node p, final Node o) {
    performAdd(JenaRocksTransactionHandler.currentTxn(this), s, p, o);
  }

  public void performAdd(TripleRocksAPI api, final Node s, final Node p, final Node o) {

    final byte[] sk = toKey(s);
    final byte[] pk = toKey(p);
    final byte[] ok = toKey(o);

    api.insert(sk, pk, ok);

  }

  public final void performDelete(final Node s, final Node p, final Node o) {
    performDelete(JenaRocksTransactionHandler.currentTxn(this), s, p, o);
  }

  public final void performDelete(TripleRocksAPI api, final Node s, final Node p, final Node o) {

    final byte[] sk = toKey(s);
    final byte[] pk = toKey(p);
    final byte[] ok = toKey(o);

    api.delete(sk, pk, ok);

  }

  public HistogramData stats(HistogramType type) {
    return this.stats.getHistogramData(type);
  }

  public String property(String property) {
    try {
      return this.db().getProperty(property);
    }
    catch (RocksDBException e) {
      // TODO Auto-generated catch block
      throw new RuntimeException(e);
    }
  }

  /**
   * add a named graph to the store.
   * 
   * @param iri
   * @param content
   */

  public void addGraph(String iri, Graph content) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented Method: JenaRocksStore.addGraph invoked.");
  }

}
