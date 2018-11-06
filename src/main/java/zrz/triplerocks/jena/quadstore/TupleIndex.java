package zrz.triplerocks.jena.quadstore;

import static zrz.triplerocks.api.TripleSlot.OBJECT;
import static zrz.triplerocks.api.TripleSlot.PREDICATE;
import static zrz.triplerocks.api.TripleSlot.SUBJECT;

import java.util.Iterator;

import org.apache.jena.graph.Triple;

import zrz.rocksdb.JRocksColumnFamily;
import zrz.rocksdb.JRocksReadableWriter;
import zrz.rocksdb.JRocksWriter;
import zrz.rocksdb.PrefixedColumnFamily;
import zrz.triplerocks.jena.JenaNodeMapper;

/**
 * the 6 indexes on the triples.
 * 
 * @author theo
 *
 */

public class TupleIndex {

  private IndexTable spo;
  private IndexTable sop;
  private IndexTable pos;

  private IndexTable pso;
  private IndexTable ops;
  private IndexTable osp;

  public TupleIndex(JRocksColumnFamily cf) {

    this.spo = new IndexTable(new PrefixedColumnFamily(cf, "/spo"), SUBJECT, PREDICATE, OBJECT);
    this.sop = new IndexTable(new PrefixedColumnFamily(cf, "/sop"), SUBJECT, OBJECT, PREDICATE);
    this.pos = new IndexTable(new PrefixedColumnFamily(cf, "/pos"), PREDICATE, OBJECT, SUBJECT);
    this.ops = new IndexTable(new PrefixedColumnFamily(cf, "/ops"), OBJECT, PREDICATE, SUBJECT);
    this.pso = new IndexTable(new PrefixedColumnFamily(cf, "/pso"), PREDICATE, SUBJECT, OBJECT);
    this.osp = new IndexTable(new PrefixedColumnFamily(cf, "/osp"), OBJECT, SUBJECT, PREDICATE);

  }

  void add(JRocksWriter writer, Triple t) {
    byte[] s = JenaNodeMapper.instance().toByteArray(t.getSubject());
    byte[] p = JenaNodeMapper.instance().toByteArray(t.getPredicate());
    byte[] o = JenaNodeMapper.instance().toByteArray(t.getObject());
    this.spo.add(writer, s, p, o);
    this.sop.add(writer, s, o, p);
    this.pos.add(writer, p, o, s);
    this.ops.add(writer, o, p, s);
    this.pso.add(writer, p, s, o);
    this.osp.add(writer, o, s, p);
  }

  void remove(JRocksWriter writer, Triple t) {
    byte[] s = JenaNodeMapper.instance().toByteArray(t.getSubject());
    byte[] p = JenaNodeMapper.instance().toByteArray(t.getPredicate());
    byte[] o = JenaNodeMapper.instance().toByteArray(t.getObject());
    this.spo.remove(writer, s, p, o);
    this.sop.remove(writer, s, o, p);
    this.pos.remove(writer, p, o, s);
    this.ops.remove(writer, o, p, s);
    this.pso.remove(writer, p, s, o);
    this.osp.remove(writer, o, s, p);
  }

  public Iterator<Triple> select(JRocksReadableWriter reader, byte[] s, byte[] p, byte[] o) {

    // all or nothing.
    if ((s == null) && (p == null) && (o == null)) {
      return spo.find(reader);
    }
    else if ((s != null) && (p != null) && (o != null)) {
      return spo.find(reader, s, p, o);
    }

    // the single wildcard...
    if ((s != null) && (p != null)) {
      return spo.find(reader, s, p);
    }
    if ((o != null) && (p != null)) {
      return ops.find(reader, o, p);
    }
    if ((s != null) && (o != null)) {
      return sop.find(reader, s, o);
    }

    // the double any ...
    if (s != null) {
      return spo.find(reader, s);
    }
    else if (p != null) {
      return pso.find(reader, p);
    }
    else if (o != null) {
      return ops.find(reader, o);
    }

    throw new IllegalArgumentException();

  }

}
