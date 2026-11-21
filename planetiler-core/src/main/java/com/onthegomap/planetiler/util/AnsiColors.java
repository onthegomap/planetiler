package com.onthegomap.planetiler.util;

import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

/** Utilities for styling terminal output. */
public class AnsiColors {
  // Support NO_COLOR env var (https://no-color.org/)
  private static final AtomicBoolean useColors =
    new AtomicBoolean(System.getenv("NO_COLOR") == null || "\0".equals(System.getenv("NO_COLOR")));

  public static void setUseColors(boolean colors) {
    useColors.set(colors);
  }

  private AnsiColors() {}

  private static final String COLOR_RESET = "\u001B[0m";
  private static final String FG_RED = "\u001B[31m";
  private static final String FG_GREEN = "\u001B[32m";
  private static final String FG_YELLOW = "\u001B[33m";
  private static final String FG_BLUE = "\u001B[34m";
  private static final String REVERSE = "\u001B[7m";
  private static final String BOLD = "\u001B[1m";
  // Warnings and errors in the log are colored with these in log4j2.properties using:
  // %X{ansi_yellow} ... %X{ansi_reset}
  private static final Map<String, String> LOG_PATTERN_COLORS =
    Map.of("ansi_red", FG_RED, "ansi_yellow", FG_YELLOW, "ansi_reset", COLOR_RESET);

  /**
   * Returns the ANSI codes that log patterns read with {@code %X{ansi_red}}, {@code %X{ansi_yellow}} and
   * {@code %X{ansi_reset}}, or an empty map when colors are off, so those patterns then write no escape codes.
   */
  public static Map<String, String> logPatternColors() {
    return useColors.get() ? LOG_PATTERN_COLORS : Map.of();
  }

  private static String color(String fg, String string) {
    return useColors.get() ? (fg + string + COLOR_RESET) : string;
  }

  public static String red(String string) {
    return color(FG_RED, string);
  }

  public static String green(String string) {
    return color(FG_GREEN, string);
  }

  public static String yellow(String string) {
    return color(FG_YELLOW, string);
  }

  public static String blue(String string) {
    return color(FG_BLUE, string);
  }

  public static String redBackground(String string) {
    return color(REVERSE + BOLD + FG_RED, string);
  }

  public static String greenBackground(String string) {
    return color(REVERSE + BOLD + FG_GREEN, string);
  }

  public static String redBold(String string) {
    return color(BOLD + FG_RED, string);
  }

  public static String greenBold(String string) {
    return color(BOLD + FG_GREEN, string);
  }
}
