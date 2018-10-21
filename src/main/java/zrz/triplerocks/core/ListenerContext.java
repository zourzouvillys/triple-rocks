package zrz.triplerocks.core;

import org.apache.jena.graph.Triple;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.RocksDBException;
import org.rocksdb.TransactionLogIterator;
import org.rocksdb.TransactionLogIterator.BatchResult;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteBatchInterface;
import org.rocksdb.WriteBatchWithIndex;

import com.google.common.base.Preconditions;

import zrz.triplerocks.jena.JenaMultiKey;

class ListenerContext extends WriteBatch.Handler {

  private StoreChangeListener listener;
  private BaseRocksTripleStore store;
  private boolean changed;
  private ColumnFamilyHandle idx;

  ListenerContext(BaseRocksTripleStore store, StoreChangeListener listener) {
    this.store = store;
    this.listener = listener;
    this.idx = store.indexes[IndexKind.SPO.ordinal()];
  }

  /**
   * @param id
   * 
   */

  void start(long id) {

    long head = store.db.getLatestSequenceNumber();

    Preconditions.checkState(id <= head);

    if (head == id) {
      return;
    }

    try (TransactionLogIterator it = store.db.getUpdatesSince(id)) {
      while (it.isValid()) {
        BatchResult batch = it.getBatch();
        accept(batch.writeBatch());
        it.next();
      }
      listener.sync();
    }
    catch (RocksDBException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * 
   * @param wb
   */

  public void accept(WriteBatchWithIndex wb) {
    accept(wb.getWriteBatch());
    if (this.changed) {
      listener.sync();
      this.changed = false;
    }
  }

  void notify(WriteBatch batch) {
    accept(batch);
    if (this.changed) {
      listener.sync();
      this.changed = false;
    }
  }

  /**
   * note: this method does not notify of a commit.
   * 
   * @param batch
   */

  private void accept(WriteBatch batch) {
    try {
      batch.iterate(this);
    }
    catch (RocksDBException e) {
      // TODO Auto-generated catch block
      throw new RuntimeException(e);
    }
  }

  @Override
  public void put(int arg0, byte[] key, byte[] value) throws RocksDBException {
    if (arg0 != idx.getID())
      return;
    Triple triple = JenaMultiKey.fromKey(IndexKind.SPO, key);
    listener.insert(triple);
    this.changed = true;
  }

  @Override
  public void delete(int arg0, byte[] key) throws RocksDBException {
    if (arg0 != store.indexes[IndexKind.SPO.ordinal()].getID())
      return;
    listener.delete(JenaMultiKey.fromKey(IndexKind.SPO, key));
    this.changed = true;
  }

  @Override
  public void singleDelete(int arg0, byte[] key) throws RocksDBException {
    if (arg0 != store.indexes[IndexKind.SPO.ordinal()].getID())
      return;
    listener.delete(JenaMultiKey.fromKey(IndexKind.SPO, key));
    this.changed = true;
  }

  @Override
  public void deleteRange(int arg0, byte[] arg1, byte[] arg2) throws RocksDBException {
  }

  // all the rest are ignored, we don't use them.

  @Override
  public void delete(byte[] arg0) {
  }

  @Override
  public void deleteRange(byte[] arg0, byte[] arg1) {
  }

  @Override
  public void logData(byte[] arg0) {
  }

  @Override
  public void markBeginPrepare() throws RocksDBException {
  }

  @Override
  public void markCommit(byte[] arg0) throws RocksDBException {
  }

  @Override
  public void markEndPrepare(byte[] arg0) throws RocksDBException {
  }

  @Override
  public void markNoop(boolean arg0) throws RocksDBException {
  }

  @Override
  public void markRollback(byte[] arg0) throws RocksDBException {
  }

  @Override
  public void merge(byte[] arg0, byte[] arg1) {
  }

  @Override
  public void merge(int arg0, byte[] arg1, byte[] arg2) throws RocksDBException {
  }

  @Override
  public void put(byte[] arg0, byte[] arg1) {
  }

  @Override
  public void putBlobIndex(int arg0, byte[] arg1, byte[] arg2) throws RocksDBException {
  }

  @Override
  public void singleDelete(byte[] arg0) {
  }

}
