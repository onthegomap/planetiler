package com.onthegomap.flatmap.worker;

public interface Source<T> {

  T getNext();
}
