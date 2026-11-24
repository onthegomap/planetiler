package com.onthegomap.planetiler.util;

import java.io.IOException;
import java.time.Instant;
import java.util.Properties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/** Accessor for the build info inserted by maven into the {@code buildinfo.properties} file. */
public record BuildInfo(String githash, String version, Long buildTime) {

  private static final Logger LOGGER = LoggerFactory.getLogger(BuildInfo.class);

  private static final BuildInfo instance;
  static {
    BuildInfo result = null;
    try (var properties = BuildInfo.class.getResourceAsStream("/buildinfo.properties")) {
      var parsed = new Properties();
      parsed.load(properties);
      String githash = parsed.getProperty("githash");
      String version = parsed.getProperty("version");
      String epochMs = parsed.getProperty("timestamp");
      Long buildTime = null;

      if (epochMs != null && !epochMs.isBlank() && epochMs.matches("^\\d+$")) {
        buildTime = Long.parseLong(epochMs);
      }
      result = new BuildInfo(githash, version, buildTime);
    } catch (IOException e) {
      LOGGER.error("Error getting build properties");
    }
    instance = result;
  }

  public String buildTimeString() {
    return buildTime == null ? null : Instant.ofEpochMilli(buildTime).toString();
  }

  /** Returns the version, git hash and build time on one line, like {@code planetiler 0.11.0 (<git hash>, <time>)}. */
  public String versionLine() {
    String buildTimeString = buildTimeString();
    return "planetiler " + version + " (" + githash + (buildTimeString == null ? "" : ", " + buildTimeString) + ")";
  }

  /** Returns this build's {@link #versionLine()}, or {@code planetiler (unknown version)} without build info. */
  public static String currentVersionLine() {
    return instance == null ? "planetiler (unknown version)" : instance.versionLine();
  }

  /**
   * Returns {@code Planetiler <version>}, or just {@code Planetiler} when the version is unknown, for the first line of
   * help pages and runs.
   */
  public static String planetilerVersion() {
    return instance == null || instance.version == null ? "Planetiler" : ("Planetiler " + instance.version);
  }

  /** Returns info inserted by maven at build-time into the {@code buildinfo.properties} file. */
  public static BuildInfo get() {
    return instance;
  }
}
