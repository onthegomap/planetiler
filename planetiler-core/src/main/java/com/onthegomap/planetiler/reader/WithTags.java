package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.util.Imposm3Parsers;
import com.onthegomap.planetiler.util.Parse;
import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.Objects;

/** An input element with a set of string key/object value pairs. */
public interface WithTags {
  static WithTags from(Map<String, Object> tags) {
    return new OfMap(tags);
  }

  /** The key/value pairs on this element. */
  Map<String, Object> tags();

  default Object getTag(String key) {
    var result = tags().get(key);
    if (result != null) {
      return result;
    } else if (key.contains(".")) {
      return getDotted(key).rawValue();
    }
    return null;
  }

  private Struct getDotted(String key) {
    String[] parts = key.split("(\\[])?\\.", 2);
    if (parts.length == 2) {
      return getStruct(parts[0]).get(parts[1]);
    }
    return getStruct(parts[0]);
  }

  default Object getTag(String key, Object defaultValue) {
    Object val = getTag(key);
    if (val == null) {
      return defaultValue;
    }
    return val;
  }

  default boolean hasTag(String key) {
    var contains = tags().containsKey(key);
    return contains || (key.contains(".") && !getDotted(key).isNull());
  }

  private static boolean contains(Object actual, Object expected) {
    if (actual instanceof Collection<?> actualList) {
      if (expected instanceof Collection<?> expectedList) {
        for (var elem : expectedList) {
          if (actualList.contains(elem)) {
            return true;
          }
        }
      } else {
        return actualList.contains(expected);
      }
    } else if (expected instanceof Collection<?> expectedList) {
      return expectedList.contains(actual);
    }
    return expected.equals(actual);
  }

  default boolean hasTag(String key, Object value) {
    return contains(getTag(key), value);
  }

  /**
   * Returns true if the value for {@code key} is {@code value1} or {@code value2}.
   * <p>
   * Specialized version of {@link #hasTag(String, Object, Object, Object...)} for the most common use-case of small
   * number of values to test against that avoids allocating an array.
   */
  default boolean hasTag(String key, Object value1, Object value2) {
    Object actual = getTag(key);
    if (actual == null) {
      return false;
    } else {
      return contains(actual, value1) || contains(actual, value2);
    }
  }

  /** Returns true if the value for {@code key} is equal to any one of the values. */
  default boolean hasTag(String key, Object value1, Object value2, Object... others) {
    Object actual = getTag(key);
    if (actual != null) {
      if (contains(actual, value1) || contains(actual, value2)) {
        return true;
      }
      for (Object value : others) {
        if (contains(actual, value)) {
          return true;
        }
      }
    }
    return false;
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
   * Returns {@code false} if {@code tag}'s {@link Object#toString()} value is empty, "0", "false", or "no" and
   * {@code true} otherwise.
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

  /** Returns a {@link Struct} wrapper for a field, which can be a primitive or nested list/map. */
  default Struct getStruct(String key) {
    return Struct.of(getTag(key));
  }

  /**
   * Shortcut for calling {@link Struct#get(Object)} multiple times to get a deeply nested value.
   * <p>
   * Arguments can be strings to get values out of maps, or integers to get an element at a certain index out of a list.
   */
  default Struct getStruct(Object key, Object... others) {
    Struct struct = getStruct(Objects.toString(key));
    return struct.get(others[0], Arrays.copyOfRange(others, 1, others.length));
  }

  /**
   * Attempts to marshal the properties on this feature into a typed java class or record using
   * <a href="https://github.com/FasterXML/jackson-databind">jackson-databind</a>.
   */
  default <T> T as(Class<T> clazz) {
    return JsonConversion.convertValue(tags(), clazz);
  }

  /**
   * Serializes the properties on this feature as a JSON object.
   */
  default String tagsAsJson() {
    return JsonConversion.writeValueAsString(tags());
  }

  record OfMap(@Override Map<String, Object> tags) implements WithTags {}
}
