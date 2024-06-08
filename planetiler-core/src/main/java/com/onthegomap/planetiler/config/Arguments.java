package com.onthegomap.planetiler.config;

import com.google.common.collect.HashMultiset;
import com.google.common.collect.Multiset;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.stats.Stats;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.function.UnaryOperator;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Lightweight abstraction over ways to provide key/value pair arguments to a program like jvm properties, environmental
 * variables, or a config file.
 * <p>
 * When looking up a key, tries to find a case-and-separator-insensitive match, for example {@code "CONFIG_OPTION"} will
 * match {@code "config-option"} and {@code "config_option"}.
 * <p>
 * If you replace an option with a new value, you can read a value from the new option and fall back to old one by using
 * {@code "new_flag|old_flag"} as the key.
 */
public class Arguments {

  private static final Logger LOGGER = LoggerFactory.getLogger(Arguments.class);

  private final UnaryOperator<String> provider;
  private final Supplier<? extends Collection<String>> keys;
  private boolean silent = false;

  private Arguments(UnaryOperator<String> provider, Supplier<? extends Collection<String>> keys) {
    this.provider = provider;
    this.keys = keys;
  }

  /**
   * Returns arguments from JVM system properties prefixed with {@code planetiler.}
   * <p>
   * For example to set {@code key=value}: {@code java -Dplanetiler.key=value -jar ...}
   */
  public static Arguments fromJvmProperties() {
    return fromJvmProperties(
      System::getProperty,
      () -> System.getProperties().stringPropertyNames()
    );
  }

  static Arguments fromJvmProperties(UnaryOperator<String> getter, Supplier<? extends Collection<String>> keys) {
    return fromPrefixed(getter, keys, "planetiler", ".", false);
  }

  /**
   * Returns arguments parsed from environmental variables prefixed with {@code PLANETILER_}
   * <p>
   * For example to set {@code key=value}: {@code PLANETILER_KEY=value java -jar ...}
   */
  public static Arguments fromEnvironment() {
    return fromEnvironment(
      System::getenv,
      () -> System.getenv().keySet()
    );
  }

  static Arguments fromEnvironment(UnaryOperator<String> getter, Supplier<Set<String>> keys) {
    return fromPrefixed(getter, keys, "PLANETILER", "_", true);
  }

  /**
   * Returns arguments parsed from a {@link Properties} object.
   */
  public static Arguments from(Properties properties) {
    return new Arguments(
      properties::getProperty,
      properties::stringPropertyNames
    );
  }

  /**
   * Returns arguments parsed from command-line arguments.
   * <p>
   * For example to set {@code key=value}: {@code java -jar ... key=value} or {@code java -jar ... --key value}
   * <p>
   * Or to set {@code key=true}: {@code java -jar ... --key}
   *
   * @param args arguments provided to main method
   * @return arguments parsed from command-line arguments
   */
  public static Arguments fromArgs(String... args) {
    Map<String, String> parsed = new HashMap<>();
    for (int i = 0; i < args.length; i++) {
      String arg = args[i].strip();
      String[] kv = arg.split("=", 2);
      String key = kv[0].replaceAll("^[\\s-]+", "");
      if (kv.length == 2) {
        String value = kv[1];
        parsed.put(key, value);
      } else if (kv.length == 1) {
        if (arg.startsWith("-")) {
          if (i >= args.length - 1 || args[i + 1].strip().startsWith("-")) {
            parsed.put(key, "true");
          } else {
            parsed.put(key, args[++i].strip());
          }
        } else {
          parsed.put(key, "true");
        }
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
      return from(properties);
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
    Arguments fromArgsOrEnv = fromEnvOrArgs(args);
    Path configFile = fromArgsOrEnv.file("config", "path to config file", null);
    if (configFile != null) {
      return fromArgsOrEnv.orElse(fromConfigFile(configFile));
    } else {
      return fromArgsOrEnv;
    }
  }

  /**
   * Returns arguments parsed from command-line arguments, JVM properties, environmental variables.
   * <p>
   * Priority order:
   * <ol>
   * <li>command-line arguments: {@code java ... key=value}</li>
   * <li>jvm properties: {@code java -Dplanetiler.key=value ...}</li>
   * <li>environmental variables: {@code PLANETILER_KEY=value java ...}</li>
   * </ol>
   *
   * @param args command-line args provide to main entrypoint method
   * @return arguments parsed from those sources
   */
  public static Arguments fromEnvOrArgs(String... args) {
    return fromArgs(args)
      .orElse(fromJvmProperties())
      .orElse(fromEnvironment());
  }

  private static String normalize(String key, String separator, boolean upperCase) {
    String result = key.replaceAll("[._-]", separator);
    return upperCase ? result.toUpperCase(Locale.ROOT) : result.toLowerCase(Locale.ROOT);
  }

  private static String normalize(String key) {
    return normalize(key, "_", false);
  }

  public static Arguments of(Map<String, String> map) {
    Map<String, String> updated = new LinkedHashMap<>();
    for (var entry : map.entrySet()) {
      updated.put(normalize(entry.getKey()), entry.getValue());
    }
    return new Arguments(updated::get, updated::keySet);
  }

  /** Shorthand for {@link #of(Map)} which constructs the map from a list of key/value pairs. */
  public static Arguments of(Object... args) {
    Map<String, String> map = new TreeMap<>();
    for (int i = 0; i < args.length; i += 2) {
      map.put(args[i].toString(), args[i + 1].toString());
    }
    return of(map);
  }

  private static Arguments from(UnaryOperator<String> provider, Supplier<? extends Collection<String>> rawKeys,
    UnaryOperator<String> forward, UnaryOperator<String> reverse) {
    Supplier<List<String>> keys = () -> rawKeys.get().stream().flatMap(key -> {
      String reversed = reverse.apply(key);
      return normalize(key).equals(normalize(reversed)) ? Stream.empty() : Stream.of(reversed);
    }).toList();
    return new Arguments(key -> provider.apply(forward.apply(key)), keys);
  }

  private static Arguments fromPrefixed(UnaryOperator<String> provider, Supplier<? extends Collection<String>> keys,
    String prefix, String separator, boolean uppperCase) {
    var prefixRegex = Pattern.compile("^" + Pattern.quote(normalize(prefix + separator, separator, uppperCase)),
      Pattern.CASE_INSENSITIVE);
    return from(provider, keys,
      key -> normalize(prefix + separator + key, separator, uppperCase),
      key -> normalize(prefixRegex.matcher(key).replaceFirst(""))
    );
  }

  private String get(String key) {
    String[] options = key.split("\\|");
    String value = null;
    for (int i = 0; i < options.length; i++) {
      String option = options[i].strip();
      value = provider.apply(normalize(option));
      if (value != null) {
        if (i != 0) {
          LOGGER.warn("Argument '{}' is deprecated", option);
        }
        break;
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
    var result = new Arguments(
      key -> {
        String ourResult = get(key);
        return ourResult != null ? ourResult : other.get(key);
      },
      () -> Stream.concat(
        other.keys.get().stream(),
        keys.get().stream()
      ).distinct().toList()
    );
    if (silent) {
      result.silence();
    }
    return result;
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

  protected void logArgValue(String key, String description, Object result) {
    if (!silent && LOGGER.isDebugEnabled()) {
      LOGGER.debug("argument: {}={} ({})", key.replaceFirst("\\|.*$", ""), result, description);
    }
  }

  /** Stop logging argument values when they are read and return this instance. */
  public Arguments silence() {
    this.silent = true;
    return this;
  }

  public String getString(String key, String description, String defaultValue) {
    String value = getArg(key, defaultValue);
    logArgValue(key, description, value);
    return value;
  }

  public String getString(String key, String description) {
    String value = getRequiredArg(key, description);
    logArgValue(key, description, value);
    return value;
  }

  /** Returns a {@link Path} parsed from {@code key} argument, or fall back to a default if the argument is not set. */
  public Path file(String key, String description, Path defaultValue) {
    String value = getArg(key);
    Path file = value == null ? defaultValue : Path.of(value);
    logArgValue(key, description, file);
    return file;
  }

  /** Returns a {@link Path} parsed from {@code key} argument which may or may not exist. */
  public Path file(String key, String description) {
    String value = getRequiredArg(key, description);
    Path file = Path.of(value);
    logArgValue(key, description, file);
    return file;
  }

  private String getRequiredArg(String key, String description) {
    String value = getArg(key);
    if (value == null) {
      throw new IllegalArgumentException("Missing required parameter: " + key + " (" + description + ")");
    }
    return value;
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

  /**
   * Returns a {@link Path} parsed from a required {@code key} argument which must exist for the program to function.
   *
   * @throws IllegalArgumentException if the file does not exist or if the parameter is not provided.
   */
  public Path inputFile(String key, String description) {
    Path path = file(key, description);
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

  /** Returns a boolean parsed from {@code key} or {@code null} if not specified. */
  public Boolean getBooleanObject(String key, String description) {
    var arg = getArg(key);
    Boolean value = arg == null ? null : "true".equalsIgnoreCase(arg);
    logArgValue(key, description, value);
    return value;
  }

  /** Returns a {@link List} parsed from {@code key} argument where values are separated by commas. */
  public List<String> getList(String key, String description, List<String> defaultValue) {
    String value = getArg(key, String.join(",", defaultValue));
    List<String> results = Stream.of(value.split(","))
      .map(String::trim)
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
      LOGGER.info("argument: stats=use prometheus push gateway stats");
      String job = getString("pushgateway.job", "prometheus pushgateway job ID", "planetiler");
      Duration interval = getDuration("pushgateway.interval", "how often to send stats to prometheus push gateway",
        "15s");
      return Stats.prometheusPushGateway(prometheus, job, interval);
    } else {
      LOGGER.info("argument: stats=use in-memory stats");
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
   * @throws NumberFormatException if the argument cannot be parsed as a long
   */
  public long getLong(String key, String description, long defaultValue) {
    String value = getArg(key, Long.toString(defaultValue));
    long parsed = Long.parseLong(value);
    logArgValue(key, description, parsed);
    return parsed;
  }

  public <T> T getObject(String key, String description, T defaultValue, Function<String, T> converter) {
    final String serializedValue = getArg(key);
    final T value = serializedValue == null ? defaultValue : converter.apply(serializedValue);
    logArgValue(key, description, value);
    return value;
  }

  /**
   * Returns a map from all the arguments provided to their values.
   */
  public Map<String, String> toMap() {
    Map<String, String> result = new HashMap<>();
    for (var key : keys.get()) {
      result.put(normalize(key), get(key));
    }
    return result;
  }

  /** Returns a copy of this {@code Arguments} instance that logs each extracted argument value exactly once. */
  public Arguments withExactlyOnceLogging() {
    Multiset<String> logged = HashMultiset.create();
    return new Arguments(this.provider, this.keys) {
      @Override
      protected void logArgValue(String key, String description, Object result) {
        int count = logged.add(key, 1);
        if (count == 0) {
          super.logArgValue(key, description, result);
        } else if (count == 3000) {
          LOGGER.warn("Too many requests for argument '{}', result should be cached", key);
        }
      }
    };
  }

  public boolean silenced() {
    return silent;
  }

  public Arguments copy() {
    return new Arguments(provider, keys);
  }

  /**
   * Returns a new arguments instance that translates requests for a {@code "key"} to {@code "prefix_key"}.
   */
  public Arguments withPrefix(String prefix) {
    return fromPrefixed(provider, keys, prefix, "_", false);
  }

  /** Returns a view of this instance, that only supports requests for {@code allowedKeys}. */
  public Arguments subset(String... allowedKeys) {
    Set<String> allowed = new HashSet<>();
    for (String key : allowedKeys) {
      allowed.add(normalize(key));
    }
    return new Arguments(
      key -> allowed.contains(normalize(key)) ? provider.apply(key) : null,
      () -> keys.get().stream().filter(key -> allowed.contains(normalize(key))).toList()
    );
  }

  /** Returns a new arguments instance where the value for {@code key} defaults to {@code value}. */
  public Arguments withDefault(Object key, Object value) {
    return orElse(Arguments.of(key.toString().replaceFirst("^-*", ""), value));
  }
}
