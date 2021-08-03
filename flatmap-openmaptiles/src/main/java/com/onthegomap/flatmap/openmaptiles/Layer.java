package com.onthegomap.flatmap.openmaptiles;

public interface Layer {

  default void release() {
  }

  String name();
}
