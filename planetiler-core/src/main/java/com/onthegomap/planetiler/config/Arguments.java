package com.onthegomap.planetiler.config;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.stats.Stats;
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
   * Returns arguments from JVM system properties prefixed with {@code planetiler.}
   * <p>
   * For example to set {@code key=value}: {@code java -Dplanetiler.key=value -jar ...}
   */
  public static Arguments fromJvmProperties() {
    return new Arguments(key -> System.getProperty("planetiler." + key));
  }

  /**
   * Returns arguments parsed from environmental variables prefixed with {@code PLANETILER_}
   * <p>
   * For example to set {@code key=value}: {@code PLANETILER_KEY=value java -jar ...}
   */
  public static Arguments fromEnvironment() {
    return new Arguments(key -> System.getenv("PLANETILER_" + key.toUpperCase(Locale.ROOT)));
  }

  /**
   * Returns arguments parsed from command-line arguments.
   * <p>
   * For example to set {@code key=value}: {@code java -jar ... key=value}
   * <p>
   * Or to set {@code key=true}: {@code java -jar ... --key}
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
      } else if (kv.length == 1) {
        parsed.put(kv[0].replaceAll("^[\\s-]+", ""), "true");
      }
    }
    return of(parsed);
  }

  /**
   * Returns arguments provided from a properties file.
   *
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
   * Returns arguments parsed from command-line arguments, JVM properties, environmental variables, or a config file.
   * <p>
   * Priority order:
   * <ol>
   * <li>command-line arguments: {@code java ... key=value}</li>
   * <li>jvm properties: {@code java -Dplanetiler.key=value ...}</li>
   * <li>environmental variables: {@code PLANETILER_KEY=value java ...}</li>
   * <li>in a config file from "config" argument from any of the above</li>
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

  public static Arguments of(Map<String, String> map) {
    return new Arguments(map::get);
  }

  /** Shorthand for {@link #of(Map)} which constructs the map from a list of key/value pairs. */
  public static Arguments of(Object... args) {
    Map<String, String> map = new TreeMap<>();
    for (int i = 0; i < args.length; i += 2) {
      map.put(args[i].toString(), args[i + 1].toString());
    }
    return of(map);
  }

  private String get(String key) {
    String value = provider.apply(key);
    if (value == null) {
      value = provider.apply(key.replace('-', '_'));
      if (value == null) {
        value = provider.apply(key.replace('_', '-'));
      }
    }
    return value;
  }

  /**
   * Chain two argument providers so that {@code other} is used as a fallback to {@code this}.
   *
   * @param other another arguments provider
   * @return arguments instance that checks {@code this} first and if a match is not found then {@code other}
   */
  public Arguments orElse(Arguments other) {
    return new Arguments(key -> {
      String ourResult = get(key);
      return ourResult != null ? ourResult : other.get(key);
    });
  }

  String getArg(String key) {
    String value = get(key);
    return value == null ? null : value.trim();
  }

  String getArg(String key, String defaultValue) {
    String value = getArg(key);
    return value == null ? defaultValue : value;
  }

  /**
   * Returns an {@link Envelope} parsed from {@code key} argument, or null if missing.
   * <p>
   * Format: {@code westLng,southLat,eastLng,northLat}
   *
   * @param key         argument name
   * @param description argument description
   * @return An envelope parsed from {@code key} or null if missing
   */
  public Envelope bounds(String key, String description) {
    String input = getArg(key);
    Envelope result = null;
    if ("world".equalsIgnoreCase(input) || "planet".equalsIgnoreCase(input)) {
      result = GeoUtils.WORLD_LAT_LON_BOUNDS;
    } else if (input != null) {
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

  public String getString(String key, String description, String defaultValue) {
    String value = getArg(key, defaultValue);
    logArgValue(key, description, value);
    return value;
  }

  /** Returns a {@link Path} parsed from {@code key} argument which may or may not exist. */
  public Path file(String key, String description, Path defaultValue) {
    String value = getArg(key);
    Path file = value == null ? defaultValue : Path.of(value);
    logArgValue(key, description, file);
    return file;
  }

  /**
   * Returns a {@link Path} parsed from {@code key} argument which must exist for the program to function.
   *
   * @throws IllegalArgumentException if the file does not exist
   */
  public Path inputFile(String key, String description, Path defaultValue) {
    Path path = file(key, description, defaultValue);
    if (!Files.exists(path)) {
      throw new IllegalArgumentException(path + " does not exist");
    }
    return path;
  }

  /** Returns a boolean parsed from {@code key} argument where {@code "true"} is true and anything else is false. */
  public boolean getBoolean(String key, String description, boolean defaultValue) {
    boolean value = "true".equalsIgnoreCase(getArg(key, Boolean.toString(defaultValue)));
    logArgValue(key, description, value);
    return value;
  }

  /** Returns a {@link List} parsed from {@code key} argument where values are separated by commas. */
  public List<String> getList(String key, String description, List<String> defaultValue) {
    String value = getArg(key, String.join(",", defaultValue));
    List<String> results = Stream.of(value.split("\\s*,[,\\s]*"))
      .filter(c -> !c.isBlank()).toList();
    logArgValue(key, description, value);
    return results;
  }

  /**
   * Returns the number of threads from {@link Runtime#availableProcessors()} but allow the user to override it by
   * setting the {@code threads} argument.
   *
   * @throws NumberFormatException if {@code threads} can't be parsed as an integer
   */
  public int threads() {
    String value = getArg("threads", Integer.toString(Runtime.getRuntime().availableProcessors()));
    int threads = Math.max(2, Integer.parseInt(value));
    logArgValue("threads", "num threads", threads);
    return threads;
  }

  /**
   * Returns a {@link Stats} implementation based on the arguments provided.
   * <p>
   * If {@code pushgateway} is set then it uses a stats implementation that pushes to prometheus through a
   * <a href="https://github.com/prometheus/pushgateway">push gateway</a> every {@code pushgateway.interval} seconds.
   * Otherwise, uses an in-memory stats implementation.
   */
  public Stats getStats() {
    String prometheus = getArg("pushgateway");
    if (prometheus != null && !prometheus.isBlank()) {
      LOGGER.info("Using prometheus push gateway stats");
      String job = getString("pushgateway.job", "prometheus pushgateway job ID", "planetiler");
      Duration interval = getDuration("pushgateway.interval", "how often to send stats to prometheus push gateway",
        "15s");
      return Stats.prometheusPushGateway(prometheus, job, interval);
    } else {
      LOGGER.info("Using in-memory stats");
      return Stats.inMemory();
    }
  }

  /**
   * Returns an argument as integer.
   *
   * @throws NumberFormatException if the argument cannot be parsed as an integer
   */
  public int getInteger(String key, String description, int defaultValue) {
    String value = getArg(key, Integer.toString(defaultValue));
    int parsed = Integer.parseInt(value);
    logArgValue(key, description, parsed);
    return parsed;
  }

  /**
   * Returns an argument as double.
   *
   * @throws NumberFormatException if the argument cannot be parsed as a double
   */
  public double getDouble(String key, String description, double defaultValue) {
    String value = getArg(key, Double.toString(defaultValue));
    double parsed = Double.parseDouble(value);
    logArgValue(key, description, parsed);
    return parsed;
  }

  /**
   * Returns an argument as a {@link Duration} (i.e. "10s", "90m", "1h30m").
   *
   * @throws DateTimeParseException if the argument cannot be parsed as a duration
   */
  public Duration getDuration(String key, String description, String defaultValue) {
    String value = getArg(key, defaultValue);
    Duration parsed = Duration.parse("PT" + value);
    logArgValue(key, description, parsed.get(ChronoUnit.SECONDS) + " seconds");
    return parsed;
  }

  /**
   * Returns an argument as long.
   *
   * @throws NumberFormatException if the argument cannot be parsed as an long
   */
  public long getLong(String key, String description, long defaultValue) {
    String value = getArg(key, Long.toString(defaultValue));
    long parsed = Long.parseLong(value);
    logArgValue(key, description, parsed);
    return parsed;
  }
}
