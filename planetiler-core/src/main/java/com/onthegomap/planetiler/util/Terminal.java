package com.onthegomap.planetiler.util;

import static java.lang.foreign.ValueLayout.ADDRESS;
import static java.lang.foreign.ValueLayout.JAVA_INT;
import static java.lang.foreign.ValueLayout.JAVA_LONG;
import static java.lang.foreign.ValueLayout.JAVA_SHORT;

import java.io.Console;
import java.lang.foreign.Arena;
import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.util.Optional;

/** Information about the terminal that planetiler runs in. */
public final class Terminal {

  private static final int STDOUT = 1;

  private Terminal() {}

  /**
   * Returns the width in columns of the terminal that planetiler runs in: the {@code COLUMNS} environment variable when
   * set, otherwise the width the OS reports. Empty when output isn't a terminal or the width can't be found (e.g. on
   * Windows without {@code COLUMNS}).
   */
  public static Optional<Integer> width() {
    Console console = System.console();
    if (console == null || !console.isTerminal()) {
      return Optional.empty();
    }
    // Shells don't export COLUMNS, so when it is set someone chose that width on purpose.
    return parseColumns(System.getenv("COLUMNS")).or(Terminal::stdoutWidthFromOs);
  }

  static Optional<Integer> parseColumns(String columns) {
    try {
      int parsed = Integer.parseInt(columns == null ? "" : columns.strip());
      return Optional.of(parsed).filter(width -> width > 0);
    } catch (NumberFormatException _) {
      return Optional.empty();
    }
  }

  /** Returns the width of the terminal standard output goes to from {@code ioctl(TIOCGWINSZ)}, like {@code stty}. */
  private static Optional<Integer> stdoutWidthFromOs() {
    long request = switch (System.getProperty("os.name")) {
      case "Linux" -> 0x5413L;
      case "Mac OS X" -> 0x40087468L;
      default -> 0;
    };
    // Without native access, the JDK would print a warning about the call instead.
    if (request == 0 || !Terminal.class.getModule().isNativeAccessEnabled()) {
      return Optional.empty();
    }
    try (var arena = Arena.ofConfined()) {
      var linker = Linker.nativeLinker();
      var ioctl = linker.downcallHandle(
        linker.defaultLookup().findOrThrow("ioctl"),
        FunctionDescriptor.of(JAVA_INT, JAVA_INT, JAVA_LONG, ADDRESS),
        Linker.Option.firstVariadicArg(2));
      // struct winsize is 4 unsigned shorts: rows, columns, then 2 pixel sizes
      var winsize = arena.allocate(8);
      int result = (int) ioctl.invokeExact(STDOUT, request, winsize);
      int columns = winsize.get(JAVA_SHORT, 2);
      return result == 0 && columns > 0 ? Optional.of(columns) : Optional.empty();
    } catch (Throwable _) {
      return Optional.empty();
    }
  }
}
