package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.Planetiler;
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
    try (var properties = Planetiler.class.getResourceAsStream("/buildinfo.properties")) {
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

  /** Returns info inserted by maven at build-time into the {@code buildinfo.properties} file. */
  public static BuildInfo get() {
    return instance;
  }
}
