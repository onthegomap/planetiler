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

  private final int maxStrings;

  private final Map<String, Integer> stringToId;
  private final String[] idToString;
  private final AtomicInteger stringId = new AtomicInteger(0);

  public CommonStringEncoder(int maxStrings) {
    this.maxStrings = maxStrings;
    stringToId = new ConcurrentHashMap<>(maxStrings);
    idToString = new String[maxStrings];
  }

  /**
   * Returns the string for {@code id}.
   *
   * @throws IllegalArgumentException if there is no value for {@code id}.
   */
  public String decode(int id) {
    String str = idToString[id];
    if (str == null) {
      throw new IllegalArgumentException("No string for " + id);
    }
    return str;
  }

  /**
   * Returns a int value to each unique string passed in.
   *
   * @param string the string to store
   * @return an int that can be converted back to a string by {@link #decode(int)}.
   * @throws IllegalArgumentException if called for too many values
   */
  public int encode(String string) {
    // optimization to avoid more expensive computeIfAbsent call for the majority case when concurrent hash map already
    // contains the value.
    Integer result = stringToId.get(string);
    if (result == null) {
      result = stringToId.computeIfAbsent(string, s -> {
        int id = stringId.getAndIncrement();
        if (id >= maxStrings) {
          throw new IllegalArgumentException("Too many strings");
        }
        idToString[id] = string;
        return id;
      });
    }
    return result;
  }

  /**
   * Variant of CommonStringEncoder based on byte rather than int for string indexing.
   */
  public static class AsByte {
    private final CommonStringEncoder encoder = new CommonStringEncoder(256);

    public String decode(byte id) {
      return encoder.decode(id & 0xff);
    }

    public byte encode(String string) {
      return (byte) encoder.encode(string);
    }
  }
}
