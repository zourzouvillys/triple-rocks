package zrz.triplerocks.store;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.junit.Test;

import triplediff.protobuf.TripleDiffProto.Term;
import triplediff.protobuf.TripleDiffProto.Triple;
import zrz.rocksdb.JRocksColumnFamily;
import zrz.rocksdb.JRocksDirectSet;
import zrz.rocksdb.JRocksEngine;
import zrz.rocksdb.PrefixedColumnFamily;
import zrz.rocksdb.protobuf.ProtobufMessageMapper;

public class BatchedTripleStoreTest {

  @Test
  public void test() {

    try (JRocksEngine db = JRocksEngine.createInMemory()) {

      JRocksColumnFamily acf = db.columnFamily("default");
      PrefixedColumnFamily cf = new PrefixedColumnFamily(acf, "test/".getBytes());

      BatchedTripleStore<Triple> batched =
        new BatchedTripleStore<>(
          db,
          new JRocksDirectSet<>(ProtobufMessageMapper.withParser(Triple.parser()), cf));

      assertTrue(batched.add(Triple.newBuilder().setSubject(Term.newBuilder().setIri("my-subject")).build()));

      assertTrue(batched.contains(Triple.newBuilder().setSubject(Term.newBuilder().setIri("my-subject")).build()));

      // second attempt at adding same triple should be rejected.
      assertFalse(batched.add(Triple.newBuilder().setSubject(Term.newBuilder().setIri("my-subject")).build()));

      assertTrue(batched.remove(Triple.newBuilder().setSubject(Term.newBuilder().setIri("my-subject")).build()));

      // already removed, so should be rejected.
      assertFalse(batched.remove(Triple.newBuilder().setSubject(Term.newBuilder().setIri("my-subject")).build()));

      assertTrue(batched.add(Triple.newBuilder().build()));

      assertEquals(1L, batched.count());

      batched.clear();

      assertEquals(0L, batched.count());

      assertTrue(batched.add(Triple.newBuilder().setSubject(Term.newBuilder().setIri("my-second-subject")).build()));

      assertEquals(1L, batched.count());

      batched.flush(db);

      assertEquals(1L, batched.count());

      List<Triple> found = batched.stream().collect(Collectors.toList());
      assertEquals(found, Arrays.asList(Triple.newBuilder().setSubject(Term.newBuilder().setIri("my-second-subject")).build()));

      assertTrue(batched.remove(Triple.newBuilder().setSubject(Term.newBuilder().setIri("my-second-subject")).build()));

      batched.flush(db);

      assertFalse(batched.remove(Triple.newBuilder().build()));
      // assertEquals(0L, db.columnFamily("default").count());
      assertEquals(0L, batched.count());

    }

  }

}
