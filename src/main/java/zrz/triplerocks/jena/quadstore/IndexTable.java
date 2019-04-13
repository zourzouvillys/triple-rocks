package zrz.triplerocks.jena.quadstore;

import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;

import org.apache.jena.ext.com.google.common.base.Verify;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.Triple;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import zrz.jrocks.JRocksColumnFamily;
import zrz.jrocks.JRocksKeyValueIterator;
import zrz.jrocks.JRocksReadableWriter;
import zrz.jrocks.JRocksWriter;
import zrz.jrocks.PrefixedColumnFamily;
import zrz.triplerocks.api.TripleSlot;
import zrz.triplerocks.jena.JenaNodeMapper;
import zrz.triplerocks.jena.JenaTripleMapper;

public class IndexTable {

  private JRocksColumnFamily cf;
  private TripleSlot[] slots;

  public IndexTable(JRocksColumnFamily cf, TripleSlot... slots) {
    this.cf = cf;
    this.slots = Arrays.copyOf(slots, slots.length);
  }

  byte[] toBuffer(byte[] s, byte[] p, byte[] o) {
    int len =
      CodedOutputStream.computeByteArraySizeNoTag(s)
        + CodedOutputStream.computeByteArraySizeNoTag(p)
        + CodedOutputStream.computeByteArraySizeNoTag(o);

    byte[] buffer = new byte[len];

    CodedOutputStream os = CodedOutputStream.newInstance(buffer);
    try {
      os.writeByteArrayNoTag(s);
      os.writeByteArrayNoTag(p);
      os.writeByteArrayNoTag(o);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    os.checkNoSpaceLeft();

    return buffer;

  }

  byte[] toBuffer(byte[] s, byte[] p) {
    int len =
      CodedOutputStream.computeByteArraySizeNoTag(s)
        + CodedOutputStream.computeByteArraySizeNoTag(p);

    byte[] buffer = new byte[len];

    CodedOutputStream os = CodedOutputStream.newInstance(buffer);
    try {
      os.writeByteArrayNoTag(s);
      os.writeByteArrayNoTag(p);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    os.checkNoSpaceLeft();

    return buffer;

  }

  byte[] toBuffer(byte[] s) {
    int len =
      CodedOutputStream.computeByteArraySizeNoTag(s);

    byte[] buffer = new byte[len];

    CodedOutputStream os = CodedOutputStream.newInstance(buffer);
    try {
      os.writeByteArrayNoTag(s);
    }
    catch (IOException ex) {
      throw new RuntimeException(ex);
    }

    os.checkNoSpaceLeft();

    return buffer;

  }

  public void add(JRocksWriter writer, byte[] s, byte[] p, byte[] o) {
    cf.put(writer, toBuffer(s, p, o), "".getBytes());
  }

  public void remove(JRocksWriter writer, byte[] s, byte[] p, byte[] o) {
    cf.delete(writer, toBuffer(s, p, o));
  }

  public Iterator<Triple> find(JRocksReadableWriter reader) {
    JRocksKeyValueIterator<byte[], byte[]> it = this.cf.newIterator(reader);
    return it.toKeyIterator()
      .map(JenaTripleMapper.instance()::parseFrom)
      .toIterator();
  }

  public Iterator<Triple> find(JRocksReadableWriter reader, byte[] a) {
    Verify.verifyNotNull(a, "a");
    JRocksKeyValueIterator<byte[], byte[]> it = this.cf.newIterator(reader, toBuffer(a));
    it.seek(toBuffer(a));
    return it.toKeyIterator()
      .map(JenaTripleMapper.instance()::parseFrom)
      .map(this::toTriple)
      .toIterator();
  }

  public Iterator<Triple> find(JRocksReadableWriter reader, byte[] a, byte[] b) {
    Verify.verifyNotNull(a, "a");
    Verify.verifyNotNull(b, "b");
    JRocksKeyValueIterator<byte[], byte[]> it = this.cf.newIterator(reader, toBuffer(a, b));
    return it
      .toKeyIterator()
      .map(JenaTripleMapper.instance()::parseFrom)
      .map(this::toTriple)
      .toIterator();
  }

  public Iterator<Triple> find(JRocksReadableWriter reader, byte[] a, byte[] b, byte[] c) {
    Verify.verifyNotNull(a, "a");
    Verify.verifyNotNull(b, "b");
    Verify.verifyNotNull(c, "c");
    JRocksKeyValueIterator<byte[], byte[]> it = this.cf.newIterator(reader, toBuffer(a, b, c));
    return it
      .toKeyIterator()
      .map(JenaTripleMapper.instance()::parseFrom)
      .map(this::toTriple)
      .toIterator();
  }

  Triple toTriple(Triple in) {
    return Triple.create(
      getSlot(in, TripleSlot.SUBJECT),
      getSlot(in, TripleSlot.PREDICATE),
      getSlot(in, TripleSlot.OBJECT));
  }

  private Node getSlot(Triple in, TripleSlot slot) {

    int idx = slotIndex(slot);

    switch (idx) {
      case 0:
        return in.getSubject();
      case 1:
        return in.getPredicate();
      case 2:
        return in.getObject();
    }

    throw new IllegalArgumentException("invalid index " + idx + ": " + slot.name() + " in " + Arrays.toString(slots));

  }

  private int slotIndex(TripleSlot slot) {
    if (this.slots[0] == slot)
      return 0;
    if (this.slots[1] == slot)
      return 1;
    if (this.slots[2] == slot)
      return 2;
    throw new IllegalArgumentException(slot.name());
  }

}
