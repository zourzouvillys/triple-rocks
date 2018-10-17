package zrz.triplerocks.jena;

import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;
import org.apache.jena.util.iterator.NiceIterator;
import org.rocksdb.RocksIterator;

import zrz.triplerocks.core.IndexKind;

/**
 * iterator wrapper for rockstriple entries.
 *
 * @author theo
 *
 */
class JenaRocksIterator extends NiceIterator<Triple> implements ExtendedIterator<Triple> {

  // the underlying iterator
  private final RocksIterator it;

  // the prefix to constrain results to.
  private final byte[] pfx;

  // the index we're scanning
  private final IndexKind idx;

  // if we've read an entry but not returned it yet, stored here.
  private Triple next;

  // once we've reached the logical end and closed the backing iterator, set to true.
  private boolean done;

  public JenaRocksIterator(final RocksIterator it, final byte[] key, final IndexKind idx) {
    this.it = it;
    this.pfx = key;
    this.idx = idx;
    if ((key == null) || (key.length == 1)) {
      it.seekToFirst();
    }
    else {
      it.seek(key);
    }
  }

  @Override
  public void close() {
    if (!this.done) {
      this.it.close();
    }
  }

  @Override
  public boolean hasNext() {

    if (this.done) {
      return false;
    }

    if (this.next != null) {
      return true;
    }

    this.next = this.next();

    return this.next != null;

  }

  @Override
  public Triple next() {

    try {

      if (this.done) {
        throw new IllegalArgumentException("tried to read past end of iterator");
      }

      if (this.next != null) {
        return this.next;
      }

      if (!this.it.isValid()) {
        // no next. we're done.
        this.done = true;
        this.it.close();
        return null;
      }

      final byte[] key = this.it.key();

      if (this.pfx != null) {
        for (int i = 0; i < this.pfx.length; ++i) {
          if (key[i] != this.pfx[i]) {
            // prefixes don't match, end of scan.
            this.done = true;
            this.it.close();
            return null;
          }
        }
      }

      // parse the key.
      final Triple triple = JenaMultiKey.fromKey(this.idx, key);

      // now move to the next entry.
      this.it.next();

      return triple;

    }
    finally {

      this.next = null;

    }

  }

}
