package com.onthegomap.planetiler.config;

import com.onthegomap.planetiler.util.BuildInfo;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;

/** Formats the options {@link Arguments} recorded for {@code --help} and {@code --verbose}. */
public final class ArgumentHelp {

  private static final int WIDTH = 118;
  private static final int MAX_NAME_COLUMN = 38;
  private static final int MAX_DEFAULT_LENGTH = 40;
  // Run from an IDE, the program arguments of the dist module's Main start with the profile or command to run.
  private static final String DIST_MAIN = "com.onthegomap.planetiler.Main";
  private static final String DEFAULT_SOURCE = "default";
  private static volatile String subcommand = null;

  private ArgumentHelp() {}

  /**
   * Sets the subcommand to show after the program name in usage lines, e.g. {@code openmaptiles} for
   * {@code planetiler openmaptiles}.
   */
  public static void setSubcommand(String name) {
    subcommand = name;
  }

  /**
   * Returns the command line that runs planetiler the way the user started it, with {@code args} added: e.g.
   * {@code planetiler openmaptiles --area=monaco} under the launcher, {@code java -jar planetiler.jar ...} for a jar,
   * or only the program arguments ({@code openmaptiles --area=monaco}) when the dist module's Main runs from an IDE.
   */
  public static String command(String args) {
    String command = command(
      System.getProperty("planetiler.launcher"),
      System.getProperty("sun.java.command", ""),
      subcommand);
    return (command + " " + args).strip();
  }

  static String command(String launcher, String javaCommand, String subcommand) {
    String[] parts = javaCommand.strip().split("\\s+");
    String first = parts[0];
    // `java Profile.java` runs the JDK's source launcher with the file as its first argument
    String javaFile = parts.length > 1 && parts[1].endsWith(".java") ? parts[1] : null;
    String result;
    if (launcher != null) {
      result = launcher + (javaFile == null ? "" : " " + javaFile);
    } else if (first.endsWith(".jar")) {
      result = "java -jar " + Path.of(first).getFileName();
    } else if (javaFile != null) {
      result = "java -cp planetiler.jar " + javaFile;
    } else if (first.equals(DIST_MAIN)) {
      result = "";
    } else {
      result = "java " + first;
    }
    return subcommand == null ? result : (result + " " + subcommand).strip();
  }

  /**
   * A source the profile reads, for the table after the input options in {@code --help}.
   *
   * @param name           the source name, e.g. {@code osm}
   * @param defaultPath    where it is read from unless {@code --<name>-path} is set
   * @param downloadedFrom where it gets downloaded from, e.g. a host name
   */
  public record Source(String name, Path defaultPath, String downloadedFrom) {}

  /** Returns a table of {@code sources} and how to set each one, or an empty string if there are none. */
  public static String sourcesTable(Collection<Source> sources) {
    if (sources.isEmpty()) {
      return "";
    }
    int pathWidth = Math.max(40, sources.stream()
      .mapToInt(source -> source.defaultPath().toString().length() + 2)
      .max().orElse(0));
    String row = "    %-18s%-" + pathWidth + "s%s";
    var result = new StringBuilder("\n")
      .append(String.format("  %-20s%-" + pathWidth + "s%s", "Sources", "default path", "downloaded from"))
      .append('\n');
    for (var source : sources) {
      result.append(String.format(row, source.name(), source.defaultPath(), source.downloadedFrom()).stripTrailing())
        .append('\n');
    }
    return result.append("""

        Each source can also be set on its own:
          --<source>-path=<file>       local file
          --<source>-url=<url>         where to download it
          --refresh-<source>           re-download if changed
          --download-<source>=<mode>   override --download for a single source
          --free-<source>-after-read   delete the file after reading it, to save disk
      """).toString();
  }

  /** Returns the line every help screen starts with: planetiler, its version and what it does. */
  public static String title() {
    BuildInfo buildInfo = BuildInfo.get();
    String version = buildInfo == null || buildInfo.version() == null ? "" : " " + buildInfo.version();
    return "Planetiler" + version + ": build vector tiles from OpenStreetMap data fast";
  }

  /** Returns true when the {@code planetiler} launcher started planetiler (it sets the memory limit). */
  public static boolean wasStartedByLauncher() {
    return System.getProperty("planetiler.launcher") != null;
  }

  /**
   * Returns help for {@code options}: options without a group first under {@code optionsTitle}, then a section per
   * group followed by its text from {@code footers} (e.g. a table of sources after the input options). Advanced groups
   * only show up when {@code all} is true.
   */
  public static String render(List<Arguments.Option> options, String optionsTitle,
    Map<ArgumentGroup, String> footers, boolean all) {
    List<Arguments.Option> withLauncher = new ArrayList<>(options);
    withLauncher.addAll(launcherOptions());
    StringBuilder result = new StringBuilder();
    boolean hidden = false;
    for (var section : sections(withLauncher).entrySet()) {
      ArgumentGroup group = section.getKey();
      if (group != null && group.advanced() && !all) {
        hidden = true;
      } else {
        result.append(result.isEmpty() ? "" : "\n")
          .append(group == null ? optionsTitle : group.name()).append('\n')
          .append(format(section.getValue(), ArgumentHelp::flag, ArgumentHelp::description))
          .append(group == null ? "" : footers.getOrDefault(group, ""));
      }
    }
    if (hidden) {
      result.append("\nShow all options with --help-all\n");
    }
    return result.toString();
  }

  /**
   * Returns the value of each option, for {@code --verbose}, in the sections of help, with where the value came from
   * unless it is the default.
   */
  public static String verboseList(List<Arguments.Option> options, String optionsTitle) {
    StringBuilder result = new StringBuilder("Option values:\n");
    for (var section : sections(options).entrySet()) {
      result.append('\n')
        .append(section.getKey() == null ? optionsTitle : section.getKey().name()).append('\n')
        .append(format(section.getValue(),
          option -> "--" + option.name() +
            (option.value() == null ? "" : "=" + (option.value().isEmpty() ? "''" : option.value())),
          option -> DEFAULT_SOURCE.equals(option.source()) ? "" : "(" + option.source() + ")"));
    }
    return result.toString();
  }

  /** Returns options by group: the ones without a group (key null) first, then by {@link #groupOrder}. */
  private static Map<ArgumentGroup, List<Arguments.Option>> sections(List<Arguments.Option> options) {
    Map<ArgumentGroup, List<Arguments.Option>> byGroup = new LinkedHashMap<>();
    for (var option : options) {
      byGroup.computeIfAbsent(option.group(), g -> new ArrayList<>()).add(option);
    }
    Map<ArgumentGroup, List<Arguments.Option>> result = new LinkedHashMap<>();
    byGroup.entrySet().stream()
      .sorted(Comparator.comparingInt(entry -> groupOrder(entry.getKey())))
      .forEach(entry -> result.put(entry.getKey(), entry.getValue()));
    return result;
  }

  private static int groupOrder(ArgumentGroup group) {
    if (group == null) {
      return -2;
    }
    int index = ArgumentGroup.CORE_ORDER.indexOf(group);
    // groups a profile adds go next to its other options, or after core's when advanced
    return index >= 0 ? index : group.advanced() ? ArgumentGroup.CORE_ORDER.size() : -1;
  }

  // Written to match launcher/main.go. --memory / -Xmx go in Processing, the other launcher flags in their own
  // Launcher section after it.
  private static List<Arguments.Option> launcherOptions() {
    if (wasStartedByLauncher()) {
      return List.of(
        helpOnly(ArgumentGroup.PROCESSING, "memory", "<size>", "memory limit for planetiler, e.g. 4g", null),
        helpOnly(ArgumentGroup.LAUNCHER, "aot", "<auto|off|train>", "startup warm-up cache", "auto"),
        helpOnly(ArgumentGroup.LAUNCHER, "jvm-args", "\"<args>\"", "extra Java options", null)
      );
    }
    String example = command("");
    String description = example.startsWith("java ") ?
      "Java memory limit, e.g. " + example.replaceFirst("^java ", "java -Xmx4g ") :
      "Java memory limit (a JVM option)";
    return List.of(helpOnly(ArgumentGroup.PROCESSING, "-Xmx<size>", null, description, null));
  }

  private static Arguments.Option helpOnly(ArgumentGroup group, String name, String placeholder, String description,
    String defaultValue) {
    return Arguments.Option.builder()
      .name(name)
      .description(description)
      .group(group)
      .placeholder(placeholder)
      .defaultValue(defaultValue)
      .build();
  }

  private static String format(List<Arguments.Option> options, Function<Arguments.Option, String> flag,
    Function<Arguments.Option, String> text) {
    int nameColumn = Math.min(MAX_NAME_COLUMN,
      options.stream().mapToInt(option -> flag.apply(option).length()).max().orElse(0) + 4);
    StringBuilder result = new StringBuilder();
    for (var option : options) {
      String flagText = "  " + flag.apply(option);
      String wrapped = wrap(text.apply(option), nameColumn, nameColumn);
      if (flagText.length() + 1 >= nameColumn) {
        result.append(wrapAfterCommas(flagText, 6)).append('\n');
        if (!wrapped.isBlank()) {
          result.append(wrapped).append('\n');
        }
      } else {
        String line = flagText + " ".repeat(nameColumn - flagText.length()) + wrapped.strip();
        result.append(line.stripTrailing()).append('\n');
      }
    }
    return result.toString();
  }

  /** Wraps a long comma-separated list (like the value of {@code --languages}) without adding spaces to it. */
  private static String wrapAfterCommas(String text, int rest) {
    StringBuilder result = new StringBuilder();
    StringBuilder line = new StringBuilder();
    for (String part : text.split("(?<=,)")) {
      if (line.length() > rest && line.length() + part.length() > WIDTH) {
        result.append(line).append('\n');
        line = new StringBuilder(" ".repeat(rest));
      }
      line.append(part);
    }
    return result.append(line).toString();
  }

  private static String flag(Arguments.Option option) {
    // options that aren't planetiler's own, like the -Xmx Java option, come with their own dashes
    String name = option.name().startsWith("-") ? option.name() : "--" + option.name();
    String flag = name + (option.placeholder() == null ? "" : "=" + option.placeholder());
    return option.shortName() == null ? flag : "-" + option.shortName() + ", " + flag;
  }

  private static String description(Arguments.Option option) {
    String text = Objects.requireNonNullElse(option.description(), "");
    String defaultValue = option.defaultValue();
    if ("<list>".equals(option.placeholder()) && defaultValue != null && defaultValue.length() > MAX_DEFAULT_LENGTH) {
      defaultValue = defaultValue.split(",").length + " values";
    }
    if (option.placeholder() != null && defaultValue != null && !defaultValue.isBlank()) {
      text += (text.isEmpty() ? "" : " ") + "[default: " + defaultValue + "]";
    }
    return text;
  }

  /**
   * Wraps {@code text} at {@link #WIDTH} columns, the first line indented by {@code first}, the rest by {@code rest}.
   */
  private static String wrap(String text, int first, int rest) {
    StringBuilder result = new StringBuilder();
    StringBuilder line = new StringBuilder(" ".repeat(first));
    int indent = first;
    for (String word : text.split(" ")) {
      if (line.length() > indent && line.length() + 1 + word.length() > WIDTH) {
        result.append(line).append('\n');
        line = new StringBuilder(" ".repeat(rest));
        indent = rest;
      }
      if (line.length() > indent) {
        line.append(' ');
      }
      line.append(word);
    }
    return result.append(line).toString();
  }
}
