package com.onthegomap.planetiler.stats;

public class DefaultStats {
  private static Stats defaultValue = null;

  public static Stats get() {
    return defaultValue;
  }

  public static void set(Stats stats) {
    defaultValue = stats;
  }
}
