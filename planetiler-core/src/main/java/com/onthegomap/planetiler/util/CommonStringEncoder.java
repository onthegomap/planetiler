package com.onthegomap.planetiler.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import javax.annotation.concurrent.ThreadSafe;

/**
 * A utility for compressing commonly-used strings (i.e. layer name, tag attributes).
 */
@ThreadSafe
public class CommonStringEncoder {

  private final Map<String, Byte> stringToId = new ConcurrentHashMap<>(255);
  private final String[] idToString = new String[255];
  private final AtomicInteger layerId = new AtomicInteger(0);

  private final Map<String, Integer> stringToIdMap = new ConcurrentHashMap<>(255);
  private final Map<Integer, String> idToStringMap = new ConcurrentHashMap<>();
  private final AtomicInteger intStringId = new AtomicInteger(0);

  /**
   * Returns the string for {@code id}.
   *
   * @throws IllegalArgumentException if there is no value for {@code id}.
   */
  public String decodeByte(byte id) {
    String str = idToString[id & 0xff];
    if (str == null) {
      throw new IllegalArgumentException("No string for " + id);
    }
    return str;
  }

  /**
   * Returns the string for {@code id}.
   *
   * @throws IllegalArgumentException if there is no value for {@code id}.
   */
  public String decodeInt(int id) {
    String str = idToStringMap.get(id);
    if (str == null) {
      throw new IllegalArgumentException("No string for " + id);
    }
    return str;
  }

  /**
   * Returns a byte value to each unique string passed in.
   *
   * @param string the string to store
   * @return a byte that can be converted back to a string by {@link #decodeByte(byte)}.
   * @throws IllegalArgumentException if called for too many values
   */
  public byte encodeByte(String string) {
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

  /**
   * Returns a int value to each unique string passed in.
   *
   * @param string the string to store
   * @return an int that can be converted back to a string by {@link #decodeInt(int)}.
   * @throws IllegalArgumentException if called for too many values
   */
  public int encodeInt(String string) {
    // optimization to avoid more expensive computeIfAbsent call for the majority case when concurrent hash map already
    // contains the value.
    return stringToIdMap.computeIfAbsent(string, s -> {
      int id = intStringId.getAndIncrement();
      idToStringMap.put(id, string);
      return id;
    });
  }
}
