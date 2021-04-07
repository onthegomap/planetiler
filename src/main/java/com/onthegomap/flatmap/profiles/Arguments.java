package com.onthegomap.flatmap.profiles;

import com.onthegomap.flatmap.GeoUtils;
import com.onthegomap.flatmap.OsmInputFile;
import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Arguments {

  private static final Logger LOGGER = LoggerFactory.getLogger(Arguments.class);

  public Arguments(String[] args) {
  }

  private String getArg(String key, String defaultValue) {
    return System.getProperty(key, defaultValue).trim();
  }

  public double[] bounds(String arg, String description, OsmInputFile osmInputFile) {
    String input = System.getProperty(arg, null);
    double[] result;
    if (input == null) {
      // get from osm.pbf
      result = osmInputFile.getBounds();
    } else if ("world".equalsIgnoreCase(input)) {
      result = GeoUtils.WORLD_LAT_LON_BOUNDS;
    } else {
      result = Stream.of(input.split("[\\s,]+")).mapToDouble(Double::parseDouble).toArray();
    }
    LOGGER.info(description + ": " + Arrays.toString(result));
    return result;
  }

  public String get(String arg, String description, String defaultValue) {
    String value = getArg(arg, defaultValue);
    LOGGER.info(description + ": " + value);
    return value;
  }

  public File file(String arg, String description, String defaultValue) {
    String value = getArg(arg, defaultValue);
    File file = new File(value);
    LOGGER.info(description + ": " + value);
    return file;
  }

  public File inputFile(String arg, String description, String defaultValue) {
    File file = file(arg, description, defaultValue);
    if (!file.exists()) {
      throw new IllegalArgumentException(file + " does not exist");
    }
    return file;
  }

  public boolean get(String arg, String description, boolean defaultValue) {
    boolean value = "true".equalsIgnoreCase(getArg(arg, Boolean.toString(defaultValue)));
    LOGGER.info(description + ": " + value);
    return value;
  }

  public List<String> get(String arg, String description, String[] defaultValue) {
    String value = getArg(arg, String.join(",", defaultValue));
    List<String> results = List.of(value.split("[\\s,]+"));
    LOGGER.info(description + ": " + value);
    return results;
  }

  public int threads() {
    String value = getArg("threads", Integer.toString(Runtime.getRuntime().availableProcessors()));
    int threads = Math.max(2, Integer.parseInt(value));
    LOGGER.info("num threads: " + threads);
    return threads;
  }
}
