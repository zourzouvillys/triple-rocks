package zrz.rocksdb;

import static org.junit.Assert.*;

import java.util.stream.Stream;

import org.junit.Test;

import zrz.rocksdb.protobuf.ProtobufMessageMapper;
import zrz.triplediff.protobuf.TripleDiffProto.Term;
import zrz.triplediff.protobuf.TripleDiffProto.Triple;

public class CFSetTest {

  @Test
  public void testInBatch() {

    try (JRocksEngine db = JRocksEngine.createInMemory()) {

      JRocksSet<Triple> triples = new JRocksDirectSet<>(ProtobufMessageMapper.withParser(Triple.parser()), db.columnFamily("default"));

      JRocksBatchWithIndex batch = new JRocksBatchWithIndex(db);

      Triple triple = Triple
          .newBuilder()
          .setSubject(Term.newBuilder().setIri("mysubject"))
          .setPredicate(Term.newBuilder().setIri("mypred"))
          .setObject(Term.newBuilder().setIri("myobj"))
          .build();

      triples.put(batch, triple);

      assertTrue(triples.contains(batch, triple));

      assertFalse(triples.contains(batch, triple.toBuilder().setSubject(Term.newBuilder().setIri("subject2")).build()));

      try (Stream<Triple> strm = triples.stream(batch)) {
        assertEquals(1L, strm.count());
      }

      // putting the same triple should result in no extra ...

      triples.put(batch, triple);

      try (Stream<Triple> strm = triples.stream(batch)) {
        assertEquals(1L, strm.count());
      }

    }

  }

  @Test
  public void testDirect() {

    try (JRocksEngine db = JRocksEngine.createInMemory()) {

      JRocksSet<Triple> triples = new JRocksDirectSet<>(ProtobufMessageMapper.withParser(Triple.parser()), db.columnFamily("default"));

      Triple triple = Triple
          .newBuilder()
          .setSubject(Term.newBuilder().setIri("mysubject"))
          .setPredicate(Term.newBuilder().setIri("mypred"))
          .setObject(Term.newBuilder().setIri("myobj"))
          .build();

      triples.put(db, triple);

      assertTrue(triples.contains(db, triple));

      assertFalse(triples.contains(db, triple.toBuilder().setSubject(Term.newBuilder().setIri("subject2")).build()));

      try (Stream<Triple> strm = triples.stream(db)) {
        assertEquals(1L, strm.count());
      }

      // putting the same triple should result in no extra ...

      triples.put(db, triple);

      try (Stream<Triple> strm = triples.stream(db)) {
        assertEquals(1L, strm.count());
      }

    }

  }

}
