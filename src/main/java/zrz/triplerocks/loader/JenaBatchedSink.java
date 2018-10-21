package zrz.triplerocks.loader;

import org.apache.jena.graph.Triple;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.rocksdb.RocksDBException;
import org.rocksdb.WriteBatch;
import org.rocksdb.WriteBatchWithIndex;
import org.rocksdb.util.SizeUnit;

import zrz.rocksdb.JRocksEngine;
import zrz.rocksdb.KeyMap;
import zrz.triplerocks.core.IndexKind;

/**
 * writes incoming triples to a write batch.
 * 
 * not really meant for online usage. use case is offline SST generation.
 * 
 * @author theo
 *
 */
public class JenaBatchedSink implements StreamRDF {

  private static final byte[] EMPTY_BYTES = new byte[] {};
  private WriteBatch wb;
  private WriteBatchWithIndex idx;

  private KeyMap keys;
  private int objlen;
  private int refcount;
  private int triples;
  private JRocksEngine db;

  public JenaBatchedSink(JRocksEngine db) {
    this.db = db;
  }

  @Override
  public void start() {
    this.wb = new WriteBatch();
    this.idx = new WriteBatchWithIndex();
    this.keys = new KeyMap(idx, db.keysHandle());
  }

  @Override
  public void triple(Triple triple) {

    byte[] sb = keys.alloc(triple.getSubject());
    byte[] pb = keys.alloc(triple.getPredicate());

    this.triples++;

    if (triple.getObject().isURI()) {

      byte[] ob = keys.alloc(triple.getObject());

      try {
        for (IndexKind idx : IndexKind.values()) {
          byte[] key = idx.toKey(sb, pb, ob);
          wb.put(db.index(idx), key, EMPTY_BYTES);
        }
      }
      catch (RocksDBException e) {
        // TODO Auto-generated catch block
        throw new RuntimeException(e);
      }

      this.refcount++;

    }
    else if (triple.getObject().isLiteral()) {

      try {
        for (IndexKind idx : IndexKind.values()) {
          byte[] key = idx.toKey(sb, pb, sb);
          wb.put(db.index(idx), key, EMPTY_BYTES);
        }
      }
      catch (RocksDBException e) {
        // TODO Auto-generated catch block
        throw new RuntimeException(e);
      }

      this.objlen += triple.getObject().getLiteralLexicalForm().length();

    }
    else {
      throw new IllegalArgumentException(triple.getObject().getClass().toString());
    }

  }

  @Override
  public void quad(Quad quad) {
    // TODO Auto-generated method stub
    throw new UnsupportedOperationException("Unimplemented Method: StreamRDF.quad invoked.");
  }

  @Override
  public void base(String base) {
    // TODO Auto-generated method stub
    // throw new UnsupportedOperationException("Unimplemented Method: StreamRDF.base invoked.");
  }

  @Override
  public void prefix(String prefix, String iri) {
    // TODO Auto-generated method stub
    // throw new UnsupportedOperationException("Unimplemented Method: StreamRDF.prefix invoked.");
  }

  @Override
  public void finish() {

    if (this.idx != null) {
      System.err.println(this.triples + " triples");
      System.err.println("IDX entries: " + idx.count());
      System.err.println((idx.getWriteBatch().getDataSize() / SizeUnit.MB) + " MB IRI indexes");
      System.err.println(this.keys.lastAllocatedKey());
      System.err.println(((this.triples * 16) + (this.refcount * 8) + (this.objlen)) / SizeUnit.MB + " MB data");
      System.err.println(this.keys);
    }

    db.add(this.wb);
    db.add(this.idx);

    this.wb.close();
    this.idx.close();

  }

}
