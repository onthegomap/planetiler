package com.onthegomap.planetiler.custommap;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class YamlParser {

  public static String getString(Map<String, Object> map, String key) {
    return getString(map, key, "");
  }

  public static String getString(Map<String, Object> profileDef, String key, String defaultValue) {
    if (profileDef.containsKey(key)) {
      Object value = profileDef.get(key);
      if (value != null) {
        return value.toString();
      }
    }
    return defaultValue;
  }

  public static Double getDouble(Map<String, Object> def, String key) {
    if (def == null) {
      return null;
    }
    if (def.containsKey(key)) {
      Object value = def.get(key);
      if (value != null && Number.class.isAssignableFrom(value.getClass())) {
        return ((Number) value).doubleValue();
      }
    }
    return null;
  }

  public static Long getLong(Map<String, Object> def, String key) {
    if (def.containsKey(key)) {
      Object value = def.get(key);
      if (value != null && Number.class.isAssignableFrom(value.getClass())) {
        return ((Number) value).longValue();
      }
    }
    return null;
  }

  public static Set<String> extractStringSet(Object obj) {
    if (!(obj instanceof Collection)) {
      return Collections.emptySet();
    }
    Collection<String> c = (Collection<String>) obj;
    return new HashSet<>(c);
  }
}
