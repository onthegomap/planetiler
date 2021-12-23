package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.util.Imposm3Parsers;
import com.onthegomap.planetiler.util.Parse;
import java.util.Map;

/** An input element with a set of string key/object value pairs. */
public interface WithTags {

  /** The key/value pairs on this element. */
  Map<String, Object> tags();

  default Object getTag(String key) {
    return tags().get(key);
  }

  default Object getTag(String key, Object defaultValue) {
    Object val = getTag(key);
    if (val == null) {
      return defaultValue;
    }
    return val;
  }

  default boolean hasTag(String key) {
    return tags().containsKey(key);
  }

  default boolean hasTag(String key, Object value) {
    return value.equals(getTag(key));
  }

  /** Returns true if the value for {@code key} is {@code value1} or {@code value2}. */
  default boolean hasTag(String key, Object value1, Object value2) {
    Object actual = getTag(key);
    return value1.equals(actual) || value2.equals(actual);
  }

  /** Returns the {@link Object#toString()} value for {@code key} or {@code null} if not present. */
  default String getString(String key) {
    Object value = getTag(key);
    return value == null ? null : value.toString();
  }

  /** Returns the {@link Object#toString()} value for {@code key} or {@code defaultValue} if not present. */
  default String getString(String key, String defaultValue) {
    Object value = getTag(key, defaultValue);
    return value == null ? null : value.toString();
  }

  /**
   * Returns {@code false} if {@code tag}'s {@link Object#toString()} value is empty, "0", "false", or "no" and {@code
   * true} otherwise.
   */
  default boolean getBoolean(String key) {
    return Parse.bool(getTag(key));
  }

  /** Returns the value for {@code key}, parsed with {@link Long#parseLong(String)} - or 0 if missing or invalid. */
  default long getLong(String key) {
    return Parse.parseLong(getTag(key));
  }

  /**
   * Returns the value for {@code key} interpreted as a direction, where -1 is reverse, 1 is forward, and 0 is other.
   *
   * @see <a href="https://wiki.openstreetmap.org/wiki/Key:oneway">OSM one-way</a>
   */
  default int getDirection(String key) {
    return Parse.direction(getTag(key));
  }

  /**
   * Returns a z-order for an OSM road based on the tags that are present. Bridges are above roads appear above tunnels
   * and major roads appear above minor.
   *
   * @see Imposm3Parsers#wayzorder(Map)
   */
  default int getWayZorder() {
    return Parse.wayzorder(tags());
  }

  default void setTag(String key, Object value) {
    tags().put(key, value);
  }
}
