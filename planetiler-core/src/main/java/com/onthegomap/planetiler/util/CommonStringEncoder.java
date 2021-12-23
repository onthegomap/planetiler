package com.onthegomap.planetiler.util;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A utility for compressing up to 250 commonly-used strings (i.e. layer name, tag attributes) into a single byte.
 */
@ThreadSafe
public class CommonStringEncoder {

  private final ConcurrentMap<String, Byte> stringToId = new ConcurrentHashMap<>(255);
  private final String[] idToString = new String[255];
  private final AtomicInteger layerId = new AtomicInteger(0);

  /**
   * Returns the string for {@code id}.
   *
   * @throws IllegalArgumentException if there is no value for {@code id}.
   */
  public String decode(byte id) {
    String str = idToString[id & 0xff];
    if (str == null) {
      throw new IllegalArgumentException("No string for " + id);
    }
    return str;
  }

  /**
   * Returns a byte value to each unique string passed in.
   *
   * @param string the string to store
   * @return a byte that can be converted back to a string by {@link #decode(byte)}.
   * @throws IllegalArgumentException if called for too many values
   */
  public byte encode(String string) {
    // optimization to avoid more expensive computeIfAbsent call for the majority case when concurrent hash map already
    // contains the value.
    Byte result = stringToId.get(string);
    if (result == null) {
      result = stringToId.computeIfAbsent(string, s -> {
        int id = layerId.getAndIncrement();
        if (id > 250) {
          throw new IllegalArgumentException("Too many string keys when inserting " + string);
        }
        idToString[id] = string;
        return (byte) id;
      });
    }
    return result;
  }
}
