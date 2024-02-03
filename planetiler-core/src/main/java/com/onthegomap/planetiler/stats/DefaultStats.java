package com.onthegomap.planetiler.stats;

/**
 * Holder for default {@link Stats} implementation to use for this process.
 */
public class DefaultStats {
  private DefaultStats() {}

  private static Stats defaultValue = null;

  public static Stats get() {
    return defaultValue;
  }

  public static void set(Stats stats) {
    defaultValue = stats;
  }
}
