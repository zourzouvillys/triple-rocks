package zrz.jrocks;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.function.Function;

import org.apache.jena.graph.Triple;
import org.apache.jena.util.iterator.ExtendedIterator;

import zrz.triplerocks.jena.JenaTripleMapper;

public interface JRocksIterator<TypeT> extends JRocksBaseIterator {

  TypeT entry();

  default List<TypeT> toList() {
    final LinkedList<TypeT> result = new LinkedList<>();
    this.seekToFirst();
    while (this.isValid()) {
      result.add(this.entry());
      this.next();
    }
    return result;
  }

  default Iterator<TypeT> toIterator() {
    this.seekToFirst();
    JRocksIterator<TypeT> it = this;
    return new Iterator<TypeT>() {

      @Override
      public boolean hasNext() {
        return isValid();
      }

      @Override
      public TypeT next() {
        try {
          return it.entry();
        }
        finally {
          it.next();
        }
      }

      public void close() {
        it.close();
      }

    };
  }

  default <T> JRocksIterator<T> map(Function<TypeT, T> mapper) {

    JRocksIterator<TypeT> it = this;

    return new JRocksIterator<T>() {

      @Override
      public void seekToFirst() {
        it.seekToFirst();
      }

      @Override
      public void close() {
        it.close();
      }

      @Override
      public boolean isValid() {
        return it.isValid();
      }

      @Override
      public void next() {
        it.next();
      }

      @Override
      public T entry() {
        return mapper.apply(it.entry());
      }
    };

  }

}
