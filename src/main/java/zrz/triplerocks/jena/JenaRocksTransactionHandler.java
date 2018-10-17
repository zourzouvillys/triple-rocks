package zrz.triplerocks.jena;

import org.apache.jena.graph.TransactionHandler;
import org.apache.jena.graph.impl.TransactionHandlerBase;

import com.google.common.base.Preconditions;

import zrz.triplerocks.core.TripleRocksAPI;
import zrz.triplerocks.core.TripleRocksTxn;

/**
 * we use RocksDB snapshot for read view snapshots, and a write batch for write transactions.
 *
 * @author theo
 *
 */

public class JenaRocksTransactionHandler extends TransactionHandlerBase implements TransactionHandler {

  private final JenaRocksGraph graph;
  private static final ThreadLocal<TripleRocksTxn> currentTransaction = new ThreadLocal<>();

  public JenaRocksTransactionHandler(final JenaRocksGraph graph) {
    this.graph = graph;
  }

  public static TripleRocksAPI currentTxn(final TripleRocksAPI fallback) {
    final TripleRocksTxn txn = currentTransaction.get();
    if (txn != null) {
      return txn;
    }
    return fallback;
  }

  @Override
  public boolean transactionsSupported() {
    return true;
  }

  @Override
  public void begin() {
    Preconditions.checkState(currentTransaction.get() == null);
    currentTransaction.set(this.graph.store().createTransaction());
  }

  @Override
  public void abort() {
    final TripleRocksTxn txn = currentTransaction.get();
    txn.abort();
    currentTransaction.set(null);
  }

  @Override
  public void commit() {
    final TripleRocksTxn txn = currentTransaction.get();
    Preconditions.checkState(txn != null);
    txn.commit();
    currentTransaction.set(null);
  }

}
