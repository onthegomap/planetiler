package com.onthegomap.planetiler.reader.remote;

import java.util.Locale;

/** How to get a source that is not on local disk yet, from the {@code download} and {@code download_<source>} args. */
public enum DownloadMode {
  /** Only read local files. */
  NONE,
  /**
   * Read the parts the map bounds need from cloud-optimized sources with range requests and keep them in a sparse
   * local file for later runs; download other sources whole.
   */
  CACHED,
  /**
   * Read the parts the map bounds need from cloud-optimized sources with range requests without storing them on disk;
   * download other sources whole.
   */
  STREAMING,
  /** Download every source whole. */
  FULL;

  /**
   * Parses {@code false}, {@code cached}, {@code streaming} or {@code full}, where {@code true} means {@code cached}.
   *
   * @throws IllegalArgumentException if {@code value} is none of those
   */
  public static DownloadMode parse(String value) {
    return switch (value.toLowerCase(Locale.ROOT)) {
      case "false", "none" -> NONE;
      case "true", "cached" -> CACHED;
      case "streaming" -> STREAMING;
      case "full" -> FULL;
      default -> throw new IllegalArgumentException(
        "Unrecognized download mode: " + value + ", expected false, cached, streaming or full");
    };
  }

  /** Returns true if sources missing from local disk should be fetched. */
  public boolean enabled() {
    return this != NONE;
  }

  /** Returns true if cloud-optimized sources should be read with range requests instead of downloaded whole. */
  public boolean fetchWhileProcessing() {
    return this == CACHED || this == STREAMING;
  }
}
