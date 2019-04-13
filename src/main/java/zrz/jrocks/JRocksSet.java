package zrz.jrocks;

import java.util.stream.Stream;

import com.google.common.collect.Streams;

/**
 * logical grouping of a set of records without a value.
 * 
 * the underlying implementation may not map directly to a single CF. it may add a prefix to perform other logic to
 * materialize. however the exposed API will keep the same semantics.
 * 
 * @author theo
 *
 */
public interface JRocksSet<TypeT> extends JRocksCollection {

  boolean contains(JRocksReadableWriter ctx, TypeT key);

  void put(JRocksWriter ctx, TypeT value);

  void delete(JRocksWriter ctx, TypeT key);

  JRocksIterator<TypeT> createIterator(JRocksReadableWriter ctx);

  /**
   * note: use {@link Stream#close()}!
   */

  default Stream<TypeT> stream(JRocksReadableWriter ctx) {
    return Streams.stream(new JavaIteratorAdapter<>(createIterator(ctx)));
  }

}
