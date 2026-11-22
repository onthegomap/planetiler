package com.onthegomap.planetiler.config;

import com.onthegomap.planetiler.util.BuildInfo;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.util.Terminal;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.function.Predicate;

/** Formats the options {@link Arguments} recorded for {@code --help} and {@code --verbose}. */
public final class ArgumentHelp {

  // The width when output doesn't go to a terminal, so piped help and tests don't depend on the terminal size.
  private static final int DEFAULT_WIDTH = 100;
  private static final int MIN_WIDTH = 60;
  private static final int MAX_WIDTH = 120;
  private static final int MAX_NAME_COLUMN = 38;
  private static final int MAX_DEFAULT_LENGTH = 40;
  // Run from an IDE, the program arguments of the dist module's Main start with the profile or command to run.
  private static final String DIST_MAIN = "com.onthegomap.planetiler.Main";
  private static final String DEFAULT_SOURCE = "default";
  // The options the overview calls common go first in core's sections, whatever order they were read in.
  private static final List<String> LISTED_FIRST = List.of("area", "osm-path", "bounds", "output", "force");
  private static volatile String subcommand = null;
  private static volatile String subcommandDescription = null;

  private ArgumentHelp() {}

  /**
   * Sets the subcommand to show after the program name in usage lines, e.g. {@code openmaptiles} for
   * {@code planetiler openmaptiles}, and what it does for the first line of its help ({@code null} for none).
   */
  public static void setSubcommand(String name, String description) {
    subcommand = name;
    subcommandDescription = description;
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
    var result = new StringBuilder("\n  ")
      .append(Format.padRight("Sources", 20))
      .append(Format.padRight("default path", pathWidth))
      .append("downloaded from\n");
    for (var source : sources) {
      String row = "    " + Format.padRight(source.name(), 18) +
        Format.padRight(source.defaultPath().toString(), pathWidth) + source.downloadedFrom();
      result.append(row.stripTrailing()).append('\n');
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
    return render(options, optionsTitle, footers, group -> all || !group.advanced());
  }

  /**
   * Returns the help for a command like {@code compare}: the {@link #title()} line, what the command does (see
   * {@link #setSubcommand(String, String)}), the usage line and the options it reads itself. Options in a group (the
   * ones every run reads) only show up when {@code all} is true.
   *
   * @param usage what follows the command in the usage line, e.g. {@code [options] <archive1> <archive2>}
   */
  public static String commandHelp(List<Arguments.Option> options, String usage, boolean all) {
    String description = subcommandDescription == null ? null : subcommand + ": " + subcommandDescription;
    return commandHelp(options, description, usage, all);
  }

  static String commandHelp(List<Arguments.Option> options, String description, String usage, boolean all) {
    return title() + "\n\n" + (description == null ? "" : description + "\n\n") +
      "Usage: " + command(usage) + "\n\n" + render(options, "Options", Map.of(), group -> all);
  }

  private static String render(List<Arguments.Option> options, String optionsTitle,
    Map<ArgumentGroup, String> footers, Predicate<ArgumentGroup> shown) {
    List<Arguments.Option> withLauncher = new ArrayList<>(options);
    // Only runs that build a map read the processing options, and only those care about the memory limit.
    if (options.stream().anyMatch(option -> ArgumentGroup.PROCESSING.equals(option.group()))) {
      withLauncher.addAll(launcherOptions());
    }
    StringBuilder result = new StringBuilder();
    boolean hidden = false;
    for (var section : sections(withLauncher).entrySet()) {
      ArgumentGroup group = section.getKey();
      if (group != null && !shown.test(group)) {
        hidden = true;
      } else {
        result.append(result.isEmpty() ? "" : "\n")
          .append(group == null ? optionsTitle : group.name()).append(":\n")
          .append(format(section.getValue(), flagColumn(section.getValue()), ArgumentHelp::description))
          .append(group == null ? "" : footers.getOrDefault(group, ""));
      }
    }
    if (hidden) {
      result.append("\nShow all options with --help-all\n");
    }
    return wrapToTerminal(result.toString());
  }

  /**
   * Returns {@code text} with each indented line that is wider than the terminal wrapped: a two-column line (a gap of 2
   * or more spaces after the indent) under its second column, a flag on its own line only after commas (e.g. a list
   * value), other lines at their indent. Lines that aren't indented (titles, usage lines) stay as they are. Leave commands that people copy
   * out of {@code text}: the terminal soft-wraps a long line instead, which keeps it in one piece when copied.
   */
  public static String wrapToTerminal(String text) {
    int width = outputWidth();
    StringBuilder result = new StringBuilder();
    for (String line : text.split("\n", -1)) {
      int indent = line.length() - line.stripLeading().length();
      int column = line.indexOf("  ", indent);
      while (column >= 0 && column < line.length() && line.charAt(column) == ' ') {
        column++;
      }
      if (line.length() <= width || indent == 0) {
        result.append(line);
      } else if (column > 0 && column < line.length()) {
        result.append(line, 0, column).append(wrap(line.substring(column), column, column, width).substring(column));
      } else if (line.startsWith("-", indent)) {
        // a flag without a description, like a value in --verbose, so only lists split
        result.append(wrapAfterCommas(line, indent + 4, width));
      } else {
        result.append(wrap(line.strip(), indent, indent, width));
      }
      result.append('\n');
    }
    return result.substring(0, result.length() - 1);
  }

  /** Returns the width to wrap help at: the terminal's, or a fixed width when output doesn't go to a terminal. */
  private static int outputWidth() {
    return Terminal.width().stream()
      .map(width -> Math.clamp(width, MIN_WIDTH, MAX_WIDTH))
      .findFirst()
      .orElse(DEFAULT_WIDTH);
  }

  /**
   * Returns the value of each option, for {@code --verbose}, in the sections of help, with where the value came from
   * unless it is the default.
   */
  public static String verboseList(List<Arguments.Option> options, String optionsTitle) {
    StringBuilder result = new StringBuilder("Option values:\n");
    for (var section : sections(options).entrySet()) {
      result.append('\n')
        .append(section.getKey() == null ? optionsTitle : section.getKey().name()).append(":\n")
        .append(format(section.getValue(),
          option -> "--" + option.name() +
            (option.value() == null ? "" : "=" + (option.value().isEmpty() ? "''" : option.value())),
          option -> DEFAULT_SOURCE.equals(option.source()) ? "" : "(" + option.source() + ")"));
    }
    return wrapToTerminal(result.toString());
  }

  /**
   * Returns options by group: the ones without a group (key null) first, then by {@link #groupOrder}, each in the order
   * read except that {@link #LISTED_FIRST} go first in the groups they're in.
   */
  private static Map<ArgumentGroup, List<Arguments.Option>> sections(List<Arguments.Option> options) {
    Map<ArgumentGroup, List<Arguments.Option>> byGroup = new LinkedHashMap<>();
    for (var option : options) {
      byGroup.computeIfAbsent(option.group(), g -> new ArrayList<>()).add(option);
    }
    Map<ArgumentGroup, List<Arguments.Option>> result = new LinkedHashMap<>();
    byGroup.entrySet().stream()
      .sorted(Comparator.comparingInt(entry -> groupOrder(entry.getKey())))
      .forEach(entry -> result.put(entry.getKey(), entry.getKey() == null ? entry.getValue() : entry.getValue().stream()
        .sorted(Comparator.comparingInt(option -> listedFirstOrder(option.name())))
        .toList()));
    return result;
  }

  private static int listedFirstOrder(String name) {
    int index = LISTED_FIRST.indexOf(name);
    return index >= 0 ? index : LISTED_FIRST.size();
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

  /**
   * Returns a function that writes the flag of an option, with every {@code --} lined up under {@code -o, --output}.
   */
  private static Function<Arguments.Option, String> flagColumn(List<Arguments.Option> options) {
    boolean anyShortNames = options.stream().anyMatch(option -> option.shortName() != null);
    return option -> (anyShortNames && option.shortName() == null ? "    " : "") + flag(option);
  }

  /** Returns a line per option, the flag then its text in a column after the longest flag, unwrapped. */
  private static String format(List<Arguments.Option> options, Function<Arguments.Option, String> flag,
    Function<Arguments.Option, String> text) {
    int nameColumn = Math.min(MAX_NAME_COLUMN,
      options.stream().mapToInt(option -> flag.apply(option).length()).max().orElse(0) + 4);
    StringBuilder result = new StringBuilder();
    for (var option : options) {
      String flagText = "  " + flag.apply(option);
      String description = text.apply(option);
      if (flagText.length() + 1 >= nameColumn) {
        result.append(flagText).append('\n');
        if (!description.isBlank()) {
          result.append(" ".repeat(nameColumn)).append(description).append('\n');
        }
      } else {
        result.append((Format.padRight(flagText, nameColumn) + description).stripTrailing()).append('\n');
      }
    }
    return result.toString();
  }


  /** Wraps a long comma-separated list (like the value of {@code --languages}) without adding spaces to it. */
  private static String wrapAfterCommas(String text, int rest, int width) {
    StringBuilder result = new StringBuilder();
    StringBuilder line = new StringBuilder();
    for (String part : text.split("(?<=,)")) {
      if (line.length() > rest && line.length() + part.length() > width) {
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
   * Wraps {@code text} at {@code width} columns, the first line indented by {@code first}, the rest by {@code rest}.
   */
  private static String wrap(String text, int first, int rest, int width) {
    StringBuilder result = new StringBuilder();
    StringBuilder line = new StringBuilder(" ".repeat(first));
    int indent = first;
    for (String word : text.split(" ")) {
      if (line.length() > indent && line.length() + 1 + word.length() > width) {
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
