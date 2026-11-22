package com.onthegomap.planetiler.config;

import java.util.List;

/**
 * A section of {@code --help} that options read through {@link Arguments#withGroup(ArgumentGroup)} belong to.
 *
 * @param name     the section title
 * @param advanced true to show the section only with {@code --help-all}
 */
public record ArgumentGroup(String name, boolean advanced) {

  public static final ArgumentGroup INPUT = basic("Input");
  public static final ArgumentGroup SOURCES = advanced("Sources");
  public static final ArgumentGroup DOWNLOADS = advanced("Downloads");
  public static final ArgumentGroup OUTPUT = basic("Output");
  public static final ArgumentGroup OUTPUT_ADVANCED = advanced("Output (advanced)");
  public static final ArgumentGroup PROCESSING = basic("Processing");
  public static final ArgumentGroup PROCESSING_ADVANCED = advanced("Processing (advanced)");
  static final ArgumentGroup LAUNCHER = basic("Launcher");
  public static final ArgumentGroup LOGGING = basic("Logging");
  public static final ArgumentGroup DEBUGGING = advanced("Debugging");
  public static final ArgumentGroup WIKIDATA = advanced("Wikidata translations");
  /** Options that don't fit any other section. */
  public static final ArgumentGroup OTHER = advanced("Other");

  /**
   * The order core's sections appear in, after the profile's own options. Each advanced section follows the regular one
   * it adds to, so they sit together in {@code --help-all}.
   */
  static final List<ArgumentGroup> CORE_ORDER = List.of(
    INPUT,
    SOURCES,
    DOWNLOADS,
    OUTPUT,
    OUTPUT_ADVANCED,
    PROCESSING,
    PROCESSING_ADVANCED,
    LAUNCHER,
    LOGGING,
    DEBUGGING,
    WIKIDATA,
    OTHER
  );

  /** Returns a section that {@code --help} shows. */
  public static ArgumentGroup basic(String name) {
    return new ArgumentGroup(name, false);
  }

  /** Returns a section that only {@code --help-all} shows. */
  public static ArgumentGroup advanced(String name) {
    return new ArgumentGroup(name, true);
  }
}
