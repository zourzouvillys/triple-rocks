package zrz.triplerocks.jena;

import java.nio.file.Path;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NullIterator;
import org.apache.jena.util.iterator.SingletonIterator;

import zrz.triplerocks.core.BaseRocksTripleStore;
import zrz.triplerocks.core.IndexKind;
import zrz.triplerocks.core.MultiKey;

public class JenaRocksStore extends BaseRocksTripleStore {

  public JenaRocksStore() {
    super();
  }

  public JenaRocksStore(final Path path) {
    super(path);
  }

  public ExtendedIterator<Triple> all() {
    return new JenaRocksIterator(this.createIterator(IndexKind.SPO), null, IndexKind.SPO);
  }

  public ExtendedIterator<Triple> query(final Node sn, final Node pn, final Node on) {

    final byte[] s = toKey(sn);
    final byte[] p = toKey(pn);
    final byte[] o = toKey(on);

    if ((s != null) && (p != null) && (o != null)) {

      // best case, direct single hit.
      if (JenaRocksTransactionHandler.currentTxn(this).contains(s, p, o)) {
        return new SingletonIterator<>(new Triple(sn, pn, on));
      }

      return NullIterator.instance();

    }
    else if ((s != null) && (p != null)) {

      final byte[] key = MultiKey.create(s, p);
      return this.query(IndexKind.SPO, key);

    }
    else if ((s != null) && (o != null)) {

      final byte[] key = MultiKey.create(s, o);
      return this.query(IndexKind.SOP, key);

    }
    else if ((p != null) && (o != null)) {

      final byte[] key = MultiKey.create(p, o);
      return this.query(IndexKind.POS, key);

    }
    else if (s != null) {

      return this.query(IndexKind.SPO, MultiKey.create(s));

    }
    else if (p != null) {

      return this.query(IndexKind.PSO, MultiKey.create(p));

    }
    else if (o != null) {

      return this.query(IndexKind.OPS, MultiKey.create(o));

    }
    else {

      return this.all();

    }
  }

  public JenaRocksIterator query(final IndexKind index, final byte[] key) {
    return new JenaRocksIterator(JenaRocksTransactionHandler.currentTxn(this).createIterator(index), key, index);
  }

  private static byte[] toKey(final Node n) {
    if (n == null) {
      return null;
    }
    return JenaMultiKey.toKey(n);
  }

  public void performAdd(final Node s, final Node p, final Node o) {

    final byte[] sk = toKey(s);
    final byte[] pk = toKey(p);
    final byte[] ok = toKey(o);

    JenaRocksTransactionHandler
        .currentTxn(this)
        .insert(sk, pk, ok);

  }

  public final void performDelete(final Node s, final Node p, final Node o) {

    final byte[] sk = toKey(s);
    final byte[] pk = toKey(p);
    final byte[] ok = toKey(o);

    JenaRocksTransactionHandler
        .currentTxn(this)
        .delete(sk, pk, ok);

  }

}
