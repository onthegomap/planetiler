package com.onthegomap.planetiler.examples;

import java.util.HashMap;
import java.util.Map;

public class DirectionParser {
  private static final Map<String, Double> DIRECTIONS = new HashMap<>();

  static {
    DIRECTIONS.put("N", 0.0);
    DIRECTIONS.put("NE", 45.0);
    DIRECTIONS.put("E", 90.0);
    DIRECTIONS.put("SE", 135.0);
    DIRECTIONS.put("S", 180.0);
    DIRECTIONS.put("SW", 225.0);
    DIRECTIONS.put("W", 270.0);
    DIRECTIONS.put("NW", 315.0);
    DIRECTIONS.put("NNW", 337.5);
    DIRECTIONS.put("NNE", 22.5);
    DIRECTIONS.put("ENE", 67.5);
    DIRECTIONS.put("ESE", 112.5);
    DIRECTIONS.put("SSE", 157.5);
    DIRECTIONS.put("SSW", 202.5);
    DIRECTIONS.put("WSW", 247.5);
    DIRECTIONS.put("WNW", 292.5);
  }

  public static Double parse(String str) {
    if (str == null) {
      return null;
    }

    Double direction = DIRECTIONS.get(str.toUpperCase());

    if (direction != null) {
      return direction;
    }

    try {
      return Double.parseDouble(str);
    } catch (NumberFormatException e) {
      return null;
    }
  }
}
