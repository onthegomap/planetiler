package com.onthegomap.flatmap.worker;

public interface Sink<T> {

  void process(T item);
}
