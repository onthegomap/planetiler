package com.onthegomap.flatmap;

import com.carrotsearch.hppc.ObjectIntMap;
import com.graphhopper.coll.GHObjectIntHashMap;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;

public class Parse {

  public static Long parseLongOrNull(Object tag) {
    try {
      return tag == null ? null : tag instanceof Number number ? number.longValue() : Long.parseLong(tag.toString());
    } catch (NumberFormatException e) {
      return null;
    }
  }

  public static long parseLong(Object tag) {
    try {
      return tag == null ? 0 : tag instanceof Number number ? number.longValue() : Long.parseLong(tag.toString());
    } catch (NumberFormatException e) {
      return 0;
    }
  }

  private static final Pattern INT_SUBSTRING_PATTERN = Pattern.compile("^(-?\\d+)(\\D|$)");

  public static Integer parseIntSubstring(String tag) {
    if (tag == null) {
      return null;
    }
    try {
      var matcher = INT_SUBSTRING_PATTERN.matcher(tag);
      return matcher.find() ? Integer.parseInt(matcher.group(1)) : null;
    } catch (NumberFormatException e) {
      return null;
    }
  }

  private static final Set<String> booleanFalseValues = Set.of("", "0", "false", "no");

  public static boolean bool(Object tag) {
    return !(tag == null || booleanFalseValues.contains(tag.toString()));
  }

  public static int boolInt(Object tag) {
    return bool(tag) ? 1 : 0;
  }

  private static final Set<String> forwardDirections = Set.of("1", "yes", "true");

  public static int direction(Object string) {
    if (string == null) {
      return 0;
    } else if (forwardDirections.contains(string(string))) {
      return 1;
    } else if ("-1".equals(string)) {
      return -1;
    } else {
      return 0;
    }
  }

  private static final ObjectIntMap<String> defaultRank = new GHObjectIntHashMap<>();

  static {
    defaultRank.put("minor", 3);
    defaultRank.put("road", 3);
    defaultRank.put("unclassified", 3);
    defaultRank.put("residential", 3);
    defaultRank.put("tertiary_link", 3);
    defaultRank.put("tertiary", 4);
    defaultRank.put("secondary_link", 3);
    defaultRank.put("secondary", 5);
    defaultRank.put("primary_link", 3);
    defaultRank.put("primary", 6);
    defaultRank.put("trunk_link", 3);
    defaultRank.put("trunk", 8);
    defaultRank.put("motorway_link", 3);
    defaultRank.put("motorway", 9);
  }

  private static String string(Object object) {
    return object == null ? null : object.toString();
  }

  public static int wayzorder(Map<String, Object> tags) {
    long z = Parse.parseLong(tags.get("layer")) * 10 +
      defaultRank.getOrDefault(
        string(tags.get("highway")),
        tags.containsKey("railway") ? 7 : 0
      ) +
      (Parse.boolInt(tags.get("tunnel")) * -10L) +
      (Parse.boolInt(tags.get("bridge")) * 10L);
    return Math.abs(z) < 10_000 ? (int) z : 0;
  }
}
