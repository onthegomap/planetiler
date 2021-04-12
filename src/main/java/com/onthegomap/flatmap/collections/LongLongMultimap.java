package com.onthegomap.flatmap.collections;

public interface LongLongMultimap {

  void put(long key, long value);

  class FewUnorderedBinarySearchMultimap implements LongLongMultimap {

    @Override
    public void put(long key, long value) {

    }
  }

  class ManyOrderedBinarySearchMultimap implements LongLongMultimap {

    @Override
    public void put(long key, long value) {

    }
  }
}
