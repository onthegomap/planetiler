package com.onthegomap.flatmap.config;

import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.reader.osm.OsmInputFile;
import com.onthegomap.flatmap.stats.Stats;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Stream;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lightweight abstraction over ways to provide key/value pair arguments to a program like jvm properties, environmental
 * variables, or a config file.
 */
public class Arguments {

  private static final Logger LOGGER = LoggerFactory.getLogger(Arguments.class);

  private final Function<String, String> provider;

  private Arguments(Function<String, String> provider) {
    this.provider = provider;
  }

  /**
   * Parses arguments from JVM system properties prefixed with {@code flatmap.}
   * <p>
   * For example to set {@code key=value}: {@code java -Dflatmap.key=value -jar ...}
   *
   * @return arguments parsed from JVM system properties
   */
  public static Arguments fromJvmProperties() {
    return new Arguments(key -> System.getProperty("flatmap." + key));
  }

  /**
   * Parses arguments from environmental variables prefixed with {@code FLATMAP_}
   * <p>
   * For example to set {@code key=value}: {@code FLATMAP_KEY=value java -jar ...}
   *
   * @return arguments parsed from environmental variables
   */
  public static Arguments fromEnvironment() {
    return new Arguments(key -> System.getenv("FLATMAP_" + key.toUpperCase(Locale.ROOT)));
  }

  /**
   * Parses command-line arguments.
   * <p>
   * For example to set {@code key=value}: {@code java -jar ... key=value}
   *
   * @param args arguments provided to main method
   * @return arguments parsed from command-line arguments
   */
  public static Arguments fromArgs(String... args) {
    Map<String, String> parsed = new HashMap<>();
    for (String arg : args) {
      String[] kv = arg.split("=", 2);
      if (kv.length == 2) {
        String key = kv[0].replaceAll("^[\\s-]+", "");
        String value = kv[1];
        parsed.put(key, value);
      }
    }
    return of(parsed);
  }

  /**
   * Parses arguments from a properties file.
   *
   * @param path path to the properties file
   * @return arguments parsed from a properties file
   * @see <a href="https://en.wikipedia.org/wiki/.properties">.properties format explanation</a>
   */
  public static Arguments fromConfigFile(Path path) {
    Properties properties = new Properties();
    try (var reader = Files.newBufferedReader(path)) {
      properties.load(reader);
      return new Arguments(properties::getProperty);
    } catch (IOException e) {
      throw new IllegalArgumentException("Unable to load config file: " + path, e);
    }
  }

  /**
   * Look for arguments in the following priority order:
   * <ol>
   *   <li>command-line arguments: {@code java ... key=value}</li>
   *   <li>jvm properties: {@code java -Dflatmap.key=value ...}</li>
   *   <li>environmental variables: {@code FLATMAP_KEY=value java ...}</li>
   *   <li>in a config file from "config" argument from any of the above</li>
   * </ol>
   *
   * @param args command-line args provide to main entrypoint method
   * @return arguments parsed from those sources
   */
  public static Arguments fromArgsOrConfigFile(String... args) {
    Arguments fromArgsOrEnv = fromArgs(args)
      .orElse(fromJvmProperties())
      .orElse(fromEnvironment());
    Path configFile = fromArgsOrEnv.file("config", "path to config file", null);
    if (configFile != null) {
      return fromArgsOrEnv.orElse(fromConfigFile(configFile));
    } else {
      return fromArgsOrEnv;
    }
  }

  /**
   * @param map map that provides the key/value pairs
   * @return arguments provided by map
   */
  public static Arguments of(Map<String, String> map) {
    return new Arguments(map::get);
  }

  /**
   * Shorthand for {@link #of(Map)} which constructs the map from a list of key/value pairs
   *
   * @param args list of key/value pairs
   * @return arguments provided by that list of key/value pairs
   */
  public static Arguments of(Object... args) {
    Map<String, String> map = new TreeMap<>();
    for (int i = 0; i < args.length; i += 2) {
      map.put(args[i].toString(), args[i + 1].toString());
    }
    return of(map);
  }

  /**
   * Chain two argument providers so that {@code other} is used as a fallback to {@code this}.
   *
   * @param other another arguments provider
   * @return arguments instance that checks {@code this} first and if a match is not found then {@code other}
   */
  public Arguments orElse(Arguments other) {
    return new Arguments(key -> {
      String ourResult = provider.apply(key);
      return ourResult != null ? ourResult : other.provider.apply(key);
    });
  }

  private String getArg(String key, String defaultValue) {
    String value = getArg(key);
    return value == null ? defaultValue : value;
  }

  private String getArg(String key) {
    String value = provider.apply(key);
    return value == null ? null : value.trim();
  }

  /**
   * Parse an argument as {@link Envelope}, or use bounds from a {@link BoundsProvider} instead.
   * <p>
   * Format: {@code westLng,southLat,eastLng,northLat}
   *
   * @param key           argument name
   * @param description   argument description
   * @param defaultBounds fallback provider if argument missing (ie. an {@link OsmInputFile} that contains bounds in
   *                      it's metadata)
   * @return An envelope parsed from {@code key} or provided by {@code defaultBounds}
   */
  public Envelope bounds(String key, String description, BoundsProvider defaultBounds) {
    String input = getArg(key);
    Envelope result;
    if (input == null && defaultBounds != null) {
      // get from input file
      result = defaultBounds.getBounds();
    } else if ("world".equalsIgnoreCase(input)) {
      result = GeoUtils.WORLD_LAT_LON_BOUNDS;
    } else {
      double[] bounds = Stream.of(input.split("[\\s,]+")).mapToDouble(Double::parseDouble).toArray();
      if (bounds.length != 4) {
        throw new IllegalArgumentException("bounds must have 4 coordinates, got: " + input);
      }
      result = new Envelope(bounds[0], bounds[2], bounds[1], bounds[3]);
    }
    logArgValue(key, description, result);
    return result;
  }

  private void logArgValue(String key, String description, Object result) {
    LOGGER.debug("argument: " + key + "=" + result + " (" + description + ")");
  }

  /**
   * Return an argument as a string.
   *
   * @param key          argument name
   * @param description  argument description
   * @param defaultValue fallback value if missing
   * @return the value for {@code key} otherwise {@code defaultValue}
   */
  public String get(String key, String description, String defaultValue) {
    String value = getArg(key, defaultValue);
    logArgValue(key, description, value);
    return value;
  }

  /**
   * Parse an argument as a {@link Path} to a file.
   *
   * @param key          argument name
   * @param description  argument description
   * @param defaultValue fallback path if missing
   * @return a path parsed from {@code key} otherwise {@code defaultValue}
   */
  public Path file(String key, String description, Path defaultValue) {
    String value = getArg(key);
    Path file = value == null ? defaultValue : Path.of(value);
    logArgValue(key, description, file);
    return file;
  }

  /**
   * Parse an argument as a {@link Path} to a file that must exist for the program to work.
   *
   * @param key          argument name
   * @param description  argument description
   * @param defaultValue fallback path if missing
   * @return a path parsed from {@code key} otherwise {@code defaultValue}
   * @throws IllegalArgumentException if the file does not exist
   */
  public Path inputFile(String key, String description, Path defaultValue) {
    Path path = file(key, description, defaultValue);
    if (!Files.exists(path)) {
      throw new IllegalArgumentException(path + " does not exist");
    }
    return path;
  }

  /**
   * Parse an argument as a boolean.
   * <p>
   * {@code true} is considered true, and anything else will be handled as false.
   *
   * @param key          argument name
   * @param description  argument description
   * @param defaultValue fallback value if missing
   * @return a boolean parsed from {@code key} otherwise {@code defaultValue}
   */
  public boolean get(String key, String description, boolean defaultValue) {
    boolean value = "true".equalsIgnoreCase(getArg(key, Boolean.toString(defaultValue)));
    logArgValue(key, description, value);
    return value;
  }

  /**
   * Parse an argument as a list of strings.
   *
   * @param key          argument name
   * @param description  argument description
   * @param defaultValue fallback list if missing
   * @return a list of strings parsed from {@code key} otherwise {@code defaultValue}
   */
  public List<String> get(String key, String description, List<String> defaultValue) {
    String value = getArg(key, String.join(",", defaultValue));
    List<String> results = Stream.of(value.split("[\\s,]+"))
      .filter(c -> !c.isBlank()).toList();
    logArgValue(key, description, value);
    return results;
  }

  /**
   * Get the number of threads from {@link Runtime#availableProcessors()} but allow the user to override it by setting
   * the {@code threads} argument.
   *
   * @return number of threads the program should use
   * @throws NumberFormatException if {@code threads} can't be parsed as an integer
   */
  public int threads() {
    String value = getArg("threads", Integer.toString(Runtime.getRuntime().availableProcessors()));
    int threads = Math.max(2, Integer.parseInt(value));
    logArgValue("threads", "num threads", threads);
    return threads;
  }

  /**
   * Return a {@link Stats} implementation based on the arguments provided.
   * <p>
   * If {@code pushgateway} is set then it uses a stats implementation that pushes to prometheus through a <a
   * href="https://github.com/prometheus/pushgateway">push gateway</a> every {@code pushgateway.interval} seconds.
   * Otherwise uses an in-memory stats implementation.
   *
   * @return the stats implementation to use
   */
  public Stats getStats() {
    String prometheus = getArg("pushgateway");
    if (prometheus != null && !prometheus.isBlank()) {
      LOGGER.info("Using prometheus push gateway stats");
      String job = get("pushgateway.job", "prometheus pushgateway job ID", "flatmap");
      Duration interval = duration("pushgateway.interval", "how often to send stats to prometheus push gateway", "15s");
      return Stats.prometheusPushGateway(prometheus, job, interval);
    } else {
      LOGGER.info("Using in-memory stats");
      return Stats.inMemory();
    }
  }

  /**
   * Parse an argument as integer
   *
   * @param key          argument name
   * @param description  argument description
   * @param defaultValue fallback value if missing
   * @return an integer parsed from {@code key} otherwise {@code defaultValue}
   * @throws NumberFormatException if the argument cannot be parsed as an integer
   */
  public int integer(String key, String description, int defaultValue) {
    String value = getArg(key, Integer.toString(defaultValue));
    int parsed = Integer.parseInt(value);
    logArgValue(key, description, parsed);
    return parsed;
  }

  /**
   * Parse an argument as {@link Duration} (ie. "10s", "90m", "1h30m")
   *
   * @param key          argument name
   * @param description  argument description
   * @param defaultValue fallback value if missing
   * @return the parsed duration value
   * @throws DateTimeParseException if the argument cannot be parsed as a duration
   */
  public Duration duration(String key, String description, String defaultValue) {
    String value = getArg(key, defaultValue);
    Duration parsed = Duration.parse("PT" + value);
    logArgValue(key, description, parsed.get(ChronoUnit.SECONDS) + " seconds");
    return parsed;
  }
}
