package com.onthegomap.flatmap.collections;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class CommonStringEncoder {

  private final ConcurrentMap<String, Byte> stringToId = new ConcurrentHashMap<>(255);
  private final String[] idToLayer = new String[255];
  private final AtomicInteger layerId = new AtomicInteger(0);

  public String decode(byte id) {
    String str = idToLayer[id];
    if (str == null) {
      throw new IllegalStateException("No string for " + id);
    }
    return str;
  }

  public byte encode(String string) {
    return stringToId.computeIfAbsent(string, s -> {
      int id = layerId.getAndIncrement();
      if (id > 250) {
        throw new IllegalStateException("Too many string keys when inserting " + string);
      }
      idToLayer[id] = string;
      return (byte) id;
    });
  }
}
