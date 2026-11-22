package com.onthegomap.planetiler.util;

import java.io.Console;
import java.util.OptionalInt;

/** Information about the terminal that planetiler runs in. */
public final class Terminal {

  private Terminal() {}

  /**
   * Returns the width of the terminal in columns from the {@code COLUMNS} environment variable, which the planetiler
   * launcher sets. Empty when input or output isn't a terminal, or {@code COLUMNS} isn't set.
   */
  public static OptionalInt width() {
    // Shells don't export COLUMNS, and the JDK has no API for the terminal size (asking the OS would need a native
    // call), so plain `java -jar` runs get the caller's fixed width.
    Console console = System.console();
    return width(console != null && console.isTerminal(), System.getenv("COLUMNS"));
  }

  static OptionalInt width(boolean isTerminal, String columns) {
    if (!isTerminal || columns == null) {
      return OptionalInt.empty();
    }
    try {
      int parsed = Integer.parseInt(columns.strip());
      return parsed > 0 ? OptionalInt.of(parsed) : OptionalInt.empty();
    } catch (NumberFormatException e) {
      return OptionalInt.empty();
    }
  }
}
