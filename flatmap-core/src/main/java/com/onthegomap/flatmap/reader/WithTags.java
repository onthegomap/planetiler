package com.onthegomap.flatmap.reader;

import com.onthegomap.flatmap.util.Parse;
import java.util.Map;

public interface WithTags {

  Map<String, Object> tags();

  default Object getTag(String key) {
    return tags().get(key);
  }

  default Object getTag(String key, Object defaultValue) {
    Object val = tags().get(key);
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

  default boolean hasTag(String key, Object value, Object value2) {
    Object actual = getTag(key);
    return value.equals(actual) || value2.equals(actual);
  }

  default String getString(String key) {
    Object value = getTag(key);
    return value == null ? null : value.toString();
  }

  default String getString(String key, String defaultValue) {
    Object value = getTag(key, defaultValue);
    return value == null ? null : value.toString();
  }

  default boolean getBoolean(String key) {
    return Parse.bool(getTag(key));
  }

  default long getLong(String key) {
    return Parse.parseLong(getTag(key));
  }

  default int getDirection(String key) {
    return Parse.direction(getTag(key));
  }

  default int getWayZorder() {
    return Parse.wayzorder(tags());
  }

  default void setTag(String key, Object value) {
    tags().put(key, value);
  }
}
