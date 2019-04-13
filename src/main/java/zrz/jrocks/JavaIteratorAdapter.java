package zrz.jrocks;

import java.util.Iterator;

public class JavaIteratorAdapter<TypeT> implements Iterator<TypeT> {

  private JRocksIterator<TypeT> it;

  public JavaIteratorAdapter(JRocksIterator<TypeT> it) {

    this.it = it;
    this.it.seekToFirst();

    if (!it.isValid()) {
      it.close();
      it = null;
    }

  }

  @Override
  public boolean hasNext() {
    if (it == null) {
      return false;
    }
    return this.it.isValid();
  }

  @Override
  public TypeT next() {

    if (it == null) {
      throw new IllegalArgumentException("read past end");
    }

    TypeT res = it.entry();

    if (!it.isValid()) {
      it.close();
      it = null;
    }

    it.next();

    return res;

  }

}
