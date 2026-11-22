package com.onthegomap.planetiler.config;

import static com.onthegomap.planetiler.geo.GeoUtils.envelopeToString;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.EditDistance;
import io.soabase.recordbuilder.core.RecordBuilder;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
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
  private static final String COMMAND_LINE = "command line";
  private static final String CONFIG_FILE = "config file";

  private final Function<String, Value> provider;
  private final Supplier<? extends Collection<String>> keys;
  private final Registry registry;
  private final ArgumentGroup group;
  // what withPrefix adds in front of each key read through this view, e.g. "mbtiles_"
  private final String prefix;
  private boolean silent = false;

  private Arguments(Function<String, Value> provider, Supplier<? extends Collection<String>> keys, Registry registry,
    ArgumentGroup group, String prefix) {
    this.provider = provider;
    this.keys = keys;
    this.registry = registry;
    this.group = group;
    this.prefix = prefix;
  }

  private Arguments(Function<String, Value> provider, Supplier<? extends Collection<String>> keys, Registry registry,
    ArgumentGroup group) {
    this(provider, keys, registry, group, "");
  }

  private Arguments(Function<String, Value> provider, Supplier<? extends Collection<String>> keys) {
    this(provider, keys, new Registry(), null);
  }

  private static Arguments fromSource(UnaryOperator<String> provider, Supplier<? extends Collection<String>> keys,
    String source) {
    return new Arguments(key -> {
      String value = provider.apply(key);
      return value == null ? null : new Value(value, source);
    }, keys);
  }

  /** A value and where it came from (command line, environment, ...). */
  private record Value(String value, String source) {}

  /**
   * An option that was read, as shown in {@code --help} and {@code --verbose}.
   *
   * @param name         the option name as users type it, e.g. {@code tile-format}
   * @param shortName    the one-letter alias, e.g. {@code o} for {@code -o}, or {@code null}
   * @param description  what the option does
   * @param group        the help section it belongs to, or {@code null} for the profile's / command's own options
   * @param placeholder  what to type after {@code =}, e.g. {@code <file>}, or {@code null} for a flag that is off by
   *                     default
   * @param defaultValue the value used when it is not set, or {@code null} if there is none
   * @param value        the value used in this run
   * @param source       where the value came from: {@code command line}, {@code environment}, {@code JVM property},
   *                     {@code config file} or {@code default}
   */
  @RecordBuilder
  public record Option(
    String name, String shortName, String description, ArgumentGroup group, String placeholder, String defaultValue,
    String value, String source
  ) {

    /** Returns a builder for an option, with every field {@code null} until set. */
    public static ArgumentsOptionBuilder builder() {
      return ArgumentsOptionBuilder.builder();
    }
  }

  /** Every option read through an {@code Arguments} instance or the views derived from it, in the order first read. */
  private static final class Registry {
    // Options can be read from worker threads, so recording one that is already known must not take a lock.
    private final Map<String, Option> byName = new ConcurrentHashMap<>();
    private final Queue<String> order = new ConcurrentLinkedQueue<>();
    private volatile boolean logNewOptions = false;
    // Every key looked up (normalized, aliases and prefixes included), to find options that nothing read.
    private final Set<String> lookedUp = ConcurrentHashMap.newKeySet();
    private final Set<String> warnedAbout = ConcurrentHashMap.newKeySet();
    // options set on the command line or in a config file (normalized), the ones a typo could be in
    private final Set<String> setByUser = ConcurrentHashMap.newKeySet();
    // one-letter alias -> the option it stands for, so two options can't claim the same letter
    private final Map<String, String> shortNames = new ConcurrentHashMap<>();
    @SuppressWarnings("java:S3077") // only accepts immutable lists
    private volatile List<String> positional = List.of();
    private volatile boolean positionalRead = false;
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
    return fromPrefixed(sourceOf(getter, "JVM property"), keys, "planetiler", ".", false);
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
    return fromPrefixed(sourceOf(getter, "environment"), keys, "PLANETILER", "_", true);
  }

  /**
   * Returns arguments parsed from a {@link Properties} object.
   */
  public static Arguments from(Properties properties) {
    return fromSource(properties::getProperty, properties::stringPropertyNames, CONFIG_FILE);
  }

  /**
   * Returns arguments parsed from command-line arguments.
   * <p>
   * To set {@code key=value}: {@code --key=value}, {@code --key value} or {@code key=value}. To set {@code key=true}:
   * {@code --key}, and {@code key=false}: {@code --no-key}. One-letter aliases like {@code -o value} work for options
   * that define one ({@code "output|-o"}). Other words, and everything after {@code --}, are {@link #positional()}
   * arguments.
   *
   * @param args arguments provided to main method
   * @return arguments parsed from command-line arguments
   */
  public static Arguments fromArgs(String... args) {
    Map<String, String> parsed = new HashMap<>();
    List<String> positional = new ArrayList<>();
    for (int i = 0; i < args.length; i++) {
      String arg = args[i].strip();
      String[] kv = arg.split("=", 2);
      String key = SHORT_OPTION.matcher(kv[0]).matches() ? shortKey(kv[0].substring(1)) :
        kv[0].replaceAll("^[\\s-]+", "");
      if (arg.equals("--")) {
        positional.addAll(Arrays.asList(args).subList(i + 1, args.length));
        break;
      } else if (kv.length == 2) {
        parsed.put(key, kv[1]);
      } else if (!arg.startsWith("-")) {
        positional.add(arg);
      } else if (i >= args.length - 1 || args[i + 1].strip().startsWith("-")) {
        parsed.put(key, "true");
      } else {
        parsed.put(key, args[++i].strip());
      }
    }
    var result = of(parsed, COMMAND_LINE);
    result.registry.positional = List.copyOf(positional);
    parsed.keySet().forEach(key -> result.registry.setByUser.add(normalize(key)));
    return result;
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
    Path configFile = fromArgsOrEnv.file(
      ArgumentGroup.INPUT,
      "config",
      "read options from this .properties file",
      null);
    if (configFile != null) {
      var fromFile = fromConfigFile(configFile);
      fromFile.keys.get().forEach(key -> fromArgsOrEnv.registry.setByUser.add(normalize(key)));
      return fromArgsOrEnv.orElse(fromFile);
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

  private static final Pattern SHORT_OPTION = Pattern.compile("^-[a-zA-Z]$");

  // One-letter aliases are stored as "_x", which no long option normalizes to, so -o never matches --o.
  private static String shortKey(String letter) {
    return "_" + letter.toLowerCase(Locale.ROOT);
  }

  private static String displayName(String normalizedKey) {
    return normalizedKey.length() == 2 && normalizedKey.startsWith("_") ? "-" + normalizedKey.substring(1) :
      "--" + normalizedKey.replace('_', '-');
  }

  /** Returns arguments from {@code map}, which {@code --verbose} reports as defaults. */
  public static Arguments of(Map<String, String> map) {
    return of(map, "default");
  }

  private static Arguments of(Map<String, String> map, String source) {
    Map<String, String> updated = new LinkedHashMap<>();
    for (var entry : map.entrySet()) {
      updated.put(normalize(entry.getKey()), entry.getValue());
    }
    return fromSource(updated::get, updated::keySet, source);
  }

  private static Function<String, Value> sourceOf(UnaryOperator<String> getter, String source) {
    return key -> {
      String value = getter.apply(key);
      return value == null ? null : new Value(value, source);
    };
  }

  /** Shorthand for {@link #of(Map)} which constructs the map from a list of key/value pairs. */
  public static Arguments of(Object... args) {
    Map<String, String> map = new TreeMap<>();
    for (int i = 0; i < args.length; i += 2) {
      map.put(args[i].toString(), args[i + 1].toString());
    }
    return of(map);
  }

  private static Arguments from(Function<String, Value> provider, Supplier<? extends Collection<String>> rawKeys,
    UnaryOperator<String> forward, UnaryOperator<String> reverse) {
    Supplier<List<String>> keys = () -> rawKeys.get().stream().flatMap(key -> {
      String reversed = reverse.apply(key);
      return normalize(key).equals(normalize(reversed)) ? Stream.empty() : Stream.of(reversed);
    }).toList();
    return new Arguments(key -> provider.apply(forward.apply(key)), keys);
  }

  private static Arguments fromPrefixed(Function<String, Value> provider, Supplier<? extends Collection<String>> keys,
    String prefix, String separator, boolean uppperCase) {
    var prefixRegex = Pattern.compile("^" + Pattern.quote(normalize(prefix + separator, separator, uppperCase)),
      Pattern.CASE_INSENSITIVE);
    return from(provider, keys,
      key -> normalize(prefix + separator + key, separator, uppperCase),
      key -> normalize(prefixRegex.matcher(key).replaceFirst(""))
    );
  }

  private Value lookup(String key) {
    return lookup(key, true);
  }

  private Value lookup(String key, boolean warnIfDeprecated) {
    String[] options = key.split("\\|");
    for (int i = 0; i < options.length; i++) {
      String option = options[i].strip();
      boolean isShort = option.startsWith("-");
      String normalized =
        isShort ? claimShortName(option.substring(1), options[0].strip()) : normalize(prefix + option);
      registry.lookedUp.add(normalized);
      Value value = provider.apply(normalized);
      if (value != null) {
        // options are often read more than once (e.g. before the archive opens and when it does), so warn once
        if (i != 0 && warnIfDeprecated && !isShort && registry.warnedAbout.add("deprecated " + option)) {
          LOGGER.warn("Argument '{}' is deprecated", option);
        }
        return value;
      }
    }
    return null;
  }

  private String claimShortName(String letter, String longName) {
    String option = normalize(prefix + longName);
    String existing = registry.shortNames.putIfAbsent(letter, option);
    if (existing != null && !existing.equals(option)) {
      throw new IllegalStateException(
        "-" + letter + " can't be used for " + displayName(option) + ", it is already used for " +
          displayName(existing));
    }
    return shortKey(letter);
  }

  private String get(String key) {
    Value value = lookup(key);
    return value == null ? null : value.value();
  }

  /**
   * Chain two argument providers so that {@code other} is used as a fallback to {@code this}.
   *
   * @param other another arguments provider
   * @return arguments instance that checks {@code this} first and if a match is not found then {@code other}
   */
  public Arguments orElse(Arguments other) {
    // the result keeps this view's prefix, so other gets the key without it
    String keyPrefix = normalize(prefix);
    var result = new Arguments(
      key -> {
        Value ourResult = provider.apply(key);
        return ourResult != null ? ourResult :
          other.lookup(key.startsWith(keyPrefix) ? key.substring(keyPrefix.length()) : key);
      },
      () -> Stream.concat(
        other.keys.get().stream(),
        keys.get().stream()
      ).distinct().toList(),
      registry,
      group,
      prefix
    );
    if (silent) {
      result.silence();
    }
    return result;
  }

  /** Returns true if {@code key} is set, for defaults that depend on whether the user set something explicitly. */
  public boolean contains(String key) {
    return get(key) != null;
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
      result = parse(key, input, "west,south,east,north or planet", null, Arguments::parseEnvelope);
    }
    recordArgUsage(key, description, "<w,s,e,n|planet>", null, result == null ? null : envelopeToString(result));
    return result;
  }

  private static Envelope parseEnvelope(String input) {
    double[] bounds = Stream.of(input.split("[\\s,]+")).mapToDouble(Double::parseDouble).toArray();
    if (bounds.length != 4) {
      throw new IllegalArgumentException("bounds must have 4 coordinates, got: " + input);
    }
    return new Envelope(bounds[0], bounds[2], bounds[1], bounds[3]);
  }

  /**
   * Adds an option to the list {@link #options()} returns, unless it is already there.
   *
   * @param placeholder  what to type after {@code =} in help, or {@code null} for a flag that is off by default
   * @param defaultValue the value used when the option is not set
   * @param value        the value used in this run
   */
  private void recordArgUsage(String key, String description, String placeholder, Object defaultValue, Object value) {
    if (silent) {
      return;
    }
    String name = normalize(prefix + key.replaceFirst("\\|.*$", "").strip());
    if (registry.byName.containsKey(name)) {
      return;
    }
    Value found = lookup(key, false);
    String shortName = Stream.of(key.split("\\|")).map(String::strip).filter(alias -> alias.startsWith("-"))
      .map(alias -> alias.substring(1)).findFirst().orElse(null);
    var option = Option.builder()
      .name(name.replace('_', '-'))
      .shortName(shortName)
      .description(description)
      .group(group)
      .placeholder(placeholder)
      .defaultValue(Objects.toString(defaultValue, null))
      .value(Objects.toString(value, null))
      .source(found == null ? "default" : found.source())
      .build();
    if (registry.byName.putIfAbsent(name, option) == null) {
      registry.order.add(name);
      if (registry.logNewOptions) {
        LOGGER.info("argument: {}={} ({})", option.name(), option.value(), option.source());
      }
    }
  }

  /** Returns every option read so far through this instance or the views derived from it, in the order first read. */
  public List<Option> options() {
    return registry.order.stream().map(registry.byName::get).toList();
  }

  /**
   * Returns the command-line arguments that aren't options, like the two archives in
   * {@code compare a.pmtiles b.pmtiles}.
   */
  public List<String> positional() {
    registry.positionalRead = true;
    return registry.positional;
  }

  /**
   * Logs a warning for each option set on the command line or in a config file that nothing has read (with the closest
   * option that was read, when it looks like a typo), and for positional arguments when nothing reads them. Each one is
   * only warned about once.
   */
  // TODO fail instead of warning, after the 0.11 release
  public void warnAboutUnknownOptions() {
    unknownOptions().forEach(LOGGER::warn);
  }

  List<String> unknownOptions() {
    List<String> result = new ArrayList<>();
    List<String> known = registry.lookedUp.stream().filter(key -> !key.startsWith("_")).sorted().toList();
    for (String normalized : registry.setByUser.stream().sorted().toList()) {
      if (!registry.lookedUp.contains(normalized) && registry.warnedAbout.add(normalized)) {
        String closest = EditDistance.closest(normalized, known);
        result.add("Unknown option " + displayName(normalized) +
          (closest == null ? "" : " (did you mean " + displayName(closest) + "?)"));
      }
    }
    if (!registry.positionalRead) {
      for (String arg : registry.positional) {
        if (registry.warnedAbout.add("positional " + arg)) {
          result.add("Unexpected argument \"" + arg + "\" (the profile or command goes first, before its options)");
        }
      }
    }
    return result;
  }

  /**
   * Logs each option read from now on as it is read, for {@code --verbose} runs that already listed the ones read
   * before.
   */
  public void logOptionsReadFromNowOn() {
    registry.logNewOptions = true;
  }

  /**
   * Returns a view of these arguments whose options show up under {@code group} in {@code --help}. Options read without
   * a group belong to the profile or command itself.
   */
  public Arguments withGroup(ArgumentGroup group) {
    var result = new Arguments(provider, keys, registry, group, prefix);
    result.silent = silent;
    return result;
  }

  /** Shorthand for {@link #withGroup(ArgumentGroup)} with a group shown in {@code --help}. */
  public Arguments withGroup(String name) {
    return withGroup(ArgumentGroup.basic(name));
  }

  /** Shorthand for {@code withGroup(group).getString(key, description, defaultValue)}, for a single read. */
  public String getString(ArgumentGroup group, String key, String description, String defaultValue) {
    return withGroup(group).getString(key, description, defaultValue);
  }

  /** Shorthand for {@code withGroup(group).getBoolean(key, description, defaultValue)}, for a single read. */
  public boolean getBoolean(ArgumentGroup group, String key, String description, boolean defaultValue) {
    return withGroup(group).getBoolean(key, description, defaultValue);
  }

  /** Shorthand for {@code withGroup(group).file(key, description, defaultValue)}, for a single read. */
  public Path file(ArgumentGroup group, String key, String description, Path defaultValue) {
    return withGroup(group).file(key, description, defaultValue);
  }

  /** Builds the help text, with every option or only the regular ones. */
  @FunctionalInterface
  public interface HelpText {

    String build(boolean all);
  }

  /**
   * If {@code --help}, {@code -h} or {@code --help-all} is set, prints the usage line and every option read so far,
   * then exits. Read all the options first so they show up in it.
   *
   * @param usage what follows the command in the usage line, e.g. {@code [options] <archive1> <archive2>}
   */
  public void printHelpIfRequested(String usage) {
    printHelpIfRequested(all -> help("Usage: " + ArgumentHelp.command(usage) + "\n\n", "Options", Map.of(), all));
  }

  /** Like {@link #printHelpIfRequested(String)}, with the help from {@code helpText}. */
  @SuppressWarnings("java:S106") // help goes to stdout without log prefixes
  public void printHelpIfRequested(HelpText helpText) {
    boolean all = "true".equalsIgnoreCase(getArg("help_all"));
    if (all || "true".equalsIgnoreCase(getArg("help|-h"))) {
      System.out.print(helpText.build(all));
      System.out.flush();
      System.exit(0);
    }
  }

  /**
   * Call once a command has read all its options: {@link #printHelpIfRequested(String)}, then
   * {@link #warnAboutUnknownOptions()}. Commands that read options later call those two separately.
   *
   * @param usage what follows the command in the usage line, e.g. {@code [options] <archive1> <archive2>}
   */
  public void printHelpOrCheckOptions(String usage) {
    printHelpIfRequested(usage);
    warnAboutUnknownOptions();
  }

  /** Like {@link #printHelpOrCheckOptions(String)}, with the help from {@code helpText}. */
  public void printHelpOrCheckOptions(HelpText helpText) {
    printHelpIfRequested(helpText);
    warnAboutUnknownOptions();
  }

  /**
   * Returns the {@link ArgumentHelp#title()} line, {@code header}, then help for every option read so far: options read
   * without a group under {@code optionsTitle}, then a section per group followed by its text from {@code footers}
   * (e.g. a table of sources after the input options).
   *
   * @param all true to show the advanced groups too
   */
  public String help(String header, String optionsTitle, Map<ArgumentGroup, String> footers, boolean all) {
    return ArgumentHelp.title() + "\n\n" + header + ArgumentHelp.render(options(), optionsTitle, footers, all);
  }

  /** Stop logging argument values when they are read and return this instance. */
  public Arguments silence() {
    this.silent = true;
    return this;
  }

  public String getString(String key, String description, String defaultValue) {
    String value = getArg(key, defaultValue);
    recordArgUsage(key, description, "<value>", defaultValue, value);
    return value;
  }

  public String getString(String key, String description) {
    String value = getRequiredArg(key, description);
    recordArgUsage(key, description, "<value>", null, value);
    return value;
  }

  /** Returns a {@link Path} parsed from {@code key} argument, or fall back to a default if the argument is not set. */
  public Path file(String key, String description, Path defaultValue) {
    String value = getArg(key);
    Path file = value == null ? defaultValue : Path.of(value);
    recordArgUsage(key, description, "<file>", defaultValue, file);
    return file;
  }

  /** Returns a {@link Path} parsed from {@code key} argument which may or may not exist. */
  public Path file(String key, String description) {
    String value = getRequiredArg(key, description);
    Path file = value == null ? null : Path.of(value);
    recordArgUsage(key, description, "<file>", null, file);
    return file;
  }

  private String getRequiredArg(String key, String description) {
    String value = getArg(key);
    // with --help, a missing option shouldn't stop the help from showing
    if (value == null && !helpRequested()) {
      throw new IllegalArgumentException("Missing required parameter: " + key + " (" + description + ")");
    }
    return value;
  }

  private boolean helpRequested() {
    return provider.apply("help") != null || provider.apply("help_all") != null ||
      provider.apply(shortKey("h")) != null;
  }

  /**
   * Returns {@code value} parsed with {@code parser}, or fails with a message naming the option and what it expects.
   * With {@code --help}, returns {@code defaultValue} instead so the help still shows.
   */
  private <T> T parse(String key, String value, String expected, T defaultValue, Function<String, T> parser) {
    try {
      return parser.apply(value);
    } catch (RuntimeException e) {
      // don't fail planetiler --arg=bad --help
      if (helpRequested()) {
        return defaultValue;
      }
      String name = displayName(normalize(prefix + key.replaceFirst("\\|.*$", "").strip()));
      throw new IllegalArgumentException(name + ": expected " + expected + ", got \"" + value + "\"", e);
    }
  }

  private static boolean parseBoolean(String value) {
    return switch (value.toLowerCase(Locale.ROOT)) {
      case "true", "yes", "on", "1" -> true;
      case "false", "no", "off", "0" -> false;
      default -> throw new IllegalArgumentException(value);
    };
  }

  /** Returns the value of a boolean option, where {@code --no-key} is the same as {@code --key=false}. */
  private Boolean booleanArg(String key) {
    String value = getArg(key);
    if (value != null) {
      return parse(key, value, "true or false", null, Arguments::parseBoolean);
    }
    String negated = getArg("no_" + key.replaceFirst("\\|.*$", "").strip());
    return negated == null ? null : !parse("no_" + key, negated, "true or false", false, Arguments::parseBoolean);
  }

  /**
   * Returns a {@link Path} parsed from {@code key} argument which must exist for the program to function.
   *
   * @throws IllegalArgumentException if the file does not exist
   */
  public Path inputFile(String key, String description, Path defaultValue) {
    Path path = file(key, description, defaultValue);
    if (path != null && !Files.exists(path) && !helpRequested()) {
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
    if (path != null && !Files.exists(path) && !helpRequested()) {
      throw new IllegalArgumentException(path + " does not exist");
    }
    return path;
  }

  /**
   * Returns a boolean parsed from {@code key}: {@code true}, {@code yes}, {@code on} or {@code 1} for true,
   * {@code false}, {@code no}, {@code off} or {@code 0} for false, and {@code --no-key} also sets it to false.
   *
   * @throws IllegalArgumentException for any other value
   */
  public boolean getBoolean(String key, String description, boolean defaultValue) {
    boolean value = Objects.requireNonNullElse(booleanArg(key), defaultValue);
    recordArgUsage(key, description, defaultValue ? "<bool>" : null, defaultValue, value);
    return value;
  }

  /** Returns a boolean parsed from {@code key} like {@link #getBoolean}, or {@code null} if not specified. */
  public Boolean getBooleanObject(String key, String description) {
    Boolean value = booleanArg(key);
    recordArgUsage(key, description, "<bool>", null, value);
    return value;
  }

  /** Returns a {@link List} parsed from {@code key} argument where values are separated by commas. */
  public List<String> getList(String key, String description, List<String> defaultValue) {
    String value = getArg(key, String.join(",", defaultValue));
    List<String> results = Stream.of(value.split(","))
      .map(String::trim)
      .filter(c -> !c.isBlank()).toList();
    recordArgUsage(key, description, "<list>", String.join(",", defaultValue), value);
    return results;
  }

  /**
   * Returns the number of threads from {@link Runtime#availableProcessors()} but allow the user to override it by
   * setting the {@code threads} argument.
   *
   * @throws NumberFormatException if {@code threads} can't be parsed as an integer
   */
  public int threads() {
    int cores = Runtime.getRuntime().availableProcessors();
    String value = getArg("threads", Integer.toString(cores));
    int threads = Math.max(2, parse("threads", value, "a number", cores, Integer::parseInt));
    recordArgUsage("threads", "worker threads", "<n>", cores + " (the number of cores)", threads);
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
    var args = withGroup(ArgumentGroup.OTHER);
    String prometheus = args.getString("pushgateway", "prometheus push gateway URL to send stats to", null);
    if (prometheus != null && !prometheus.isBlank()) {
      String job = args.getString("pushgateway.job", "prometheus pushgateway job ID", "planetiler");
      Duration interval = args.getDuration(
        "pushgateway.interval",
        "how often to send stats to prometheus push gateway",
        "15s");
      return Stats.prometheusPushGateway(prometheus, job, interval);
    } else {
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
    int parsed = switch (value.toLowerCase(Locale.ROOT)) {
      case "false" -> 0;
      case "true" -> defaultValue;
      default -> parse(key, value, "a whole number", defaultValue, Integer::parseInt);
    };
    recordArgUsage(key, description, "<n>", defaultValue, parsed);
    return parsed;
  }

  /**
   * Returns an argument as double.
   *
   * @throws NumberFormatException if the argument cannot be parsed as a double
   */
  public double getDouble(String key, String description, double defaultValue) {
    String value = getArg(key, Double.toString(defaultValue));
    double parsed = parse(key, value, "a number", defaultValue, Double::parseDouble);
    recordArgUsage(key, description, "<n>", defaultValue, parsed);
    return parsed;
  }

  /**
   * Returns an argument as a {@link Duration} (i.e. "10s", "90m", "1h30m").
   *
   * @throws DateTimeParseException if the argument cannot be parsed as a duration
   */
  public Duration getDuration(String key, String description, String defaultValue) {
    String value = getArg(key, defaultValue);
    Duration parsed = parse(key, value, "a duration like 30s, 5m or 1h30m", parseDuration(defaultValue),
      Arguments::parseDuration);
    recordArgUsage(key, description, "<duration>", defaultValue, parsed.get(ChronoUnit.SECONDS) + " seconds");
    return parsed;
  }

  private static Duration parseDuration(String value) {
    boolean iso = value.startsWith("P") || value.startsWith("T") || value.startsWith("-");
    return Duration.parse(iso ? value : "PT" + value);
  }

  /**
   * Returns an argument as long.
   *
   * @throws NumberFormatException if the argument cannot be parsed as a long
   */
  public long getLong(String key, String description, long defaultValue) {
    String value = getArg(key, Long.toString(defaultValue));
    long parsed = parse(key, value, "a whole number", defaultValue, Long::parseLong);
    recordArgUsage(key, description, "<n>", defaultValue, parsed);
    return parsed;
  }

  public <T> T getObject(String key, String description, T defaultValue, Function<String, T> converter) {
    final String serializedValue = getArg(key);
    final T value = serializedValue == null ? defaultValue : converter.apply(serializedValue);
    recordArgUsage(key, description, "<value>", defaultValue, value);
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

  /**
   * Returns a copy of this instance that isn't silenced.
   *
   * @deprecated options are now listed once each in {@link #options()}, so use {@link #copy()}
   */
  @Deprecated(forRemoval = true)
  public Arguments withExactlyOnceLogging() {
    return copy();
  }

  public boolean silenced() {
    return silent;
  }

  /** Returns a view of these arguments that is not silenced, sharing the list of options read. */
  public Arguments copy() {
    return new Arguments(provider, keys, registry, group, prefix);
  }

  /**
   * Returns a view of these arguments that translates requests for a {@code "key"} to {@code "prefix_key"}, sharing the
   * list of options read (where they show up as {@code prefix-key}).
   */
  public Arguments withPrefix(String prefix) {
    String keyPrefix = normalize(prefix + "_");
    Supplier<List<String>> prefixedKeys = () -> keys.get().stream()
      .map(Arguments::normalize)
      .filter(key -> key.startsWith(keyPrefix))
      .map(key -> key.substring(keyPrefix.length()))
      .toList();
    var result = new Arguments(provider, prefixedKeys, registry, group, this.prefix + prefix + "_");
    result.silent = silent;
    return result;
  }

  /**
   * Returns a view of these arguments where {@code values} take precedence, e.g. options from the query string of an
   * output URI, sharing the list of options read.
   */
  public Arguments withOverrides(Map<String, String> values, String source) {
    Map<String, Value> overrides = new HashMap<>();
    values.forEach((key, value) -> overrides.put(normalize(prefix + key), new Value(value, source)));
    var result = new Arguments(
      key -> overrides.containsKey(key) ? overrides.get(key) : provider.apply(key),
      () -> Stream.concat(keys.get().stream(), values.keySet().stream()).distinct().toList(),
      registry,
      group,
      prefix
    );
    result.silent = silent;
    return result;
  }

  /** Returns a view of this instance, that only supports requests for {@code allowedKeys}. */
  public Arguments subset(String... allowedKeys) {
    Set<String> allowed = new HashSet<>();
    for (String key : allowedKeys) {
      allowed.add(normalize(key));
    }
    return new Arguments(
      key -> allowed.contains(normalize(key)) ? provider.apply(key) : null,
      () -> keys.get().stream().filter(key -> allowed.contains(normalize(key))).toList(),
      registry,
      group,
      prefix
    );
  }

  /** Returns a new arguments instance where the value for {@code key} defaults to {@code value}. */
  public Arguments withDefault(Object key, Object value) {
    return orElse(Arguments.of(key.toString().replaceFirst("^-*", ""), value));
  }
}
