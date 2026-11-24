package com.onthegomap.planetiler.config;

import com.onthegomap.planetiler.util.BuildInfo;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Predicate;

/** Formats the options {@link Arguments} recorded for {@code --help} and {@code --verbose}. */
public final class ArgumentHelp {

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
  public static String planetilerCommand(String args) {
    String command = planetilerCommand(
      System.getProperty("planetiler.launcher"),
      System.getProperty("sun.java.command", ""),
      subcommand);
    return (command + " " + args).strip();
  }

  static String planetilerCommand(String launcher, String javaCommand, String subcommand) {
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

  /** Returns the line every help screen starts with: planetiler, its version and what it does. */
  public static String title() {
    return BuildInfo.planetilerVersion() + ": build vector tiles from OpenStreetMap data fast";
  }

  /** Returns true when the {@code planetiler} launcher started planetiler (it sets the memory limit). */
  public static boolean wasStartedByLauncher() {
    return System.getProperty("planetiler.launcher") != null;
  }

  /**
   * Returns help for {@code options}: options without a group first under {@code optionsTitle}, then a section per
   * group followed by its page from {@code footers} (e.g. a table of sources after the input options). Advanced groups
   * only show up when {@code all} is true.
   */
  public static HelpPage options(List<Arguments.Option> options, String optionsTitle,
    Map<ArgumentGroup, HelpPage> footers, boolean all) {
    return options(options, optionsTitle, footers, group -> all || !group.advanced());
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
    return commandHelp(options, description, usage, all).render();
  }

  static HelpPage commandHelp(List<Arguments.Option> options, String description, String usage, boolean all) {
    var page = HelpPage.create().line(title()).line();
    if (description != null) {
      page.line(description).line();
    }
    return page.usage(usage)
      .append(options(options, "Options", Map.of(), group -> all));
  }

  private static HelpPage options(List<Arguments.Option> options, String optionsTitle,
    Map<ArgumentGroup, HelpPage> footers, Predicate<ArgumentGroup> shown) {
    List<Arguments.Option> withLauncher = new ArrayList<>(options);
    // Only runs that build a map read the processing options, and only those care about the memory limit.
    if (options.stream().anyMatch(option -> ArgumentGroup.PROCESSING.equals(option.group()))) {
      withLauncher.addAll(launcherOptions());
    }
    var page = HelpPage.create();
    boolean hidden = false;
    for (var section : sections(withLauncher).entrySet()) {
      ArgumentGroup group = section.getKey();
      if (group != null && !shown.test(group)) {
        hidden = true;
      } else {
        page.section(group == null ? optionsTitle : group.name());
        for (var option : section.getValue()) {
          page.flag(option.shortName() == null ? null : "-" + option.shortName(), flag(option), description(option));
        }
        if (group != null && footers.containsKey(group)) {
          page.append(footers.get(group));
        }
      }
    }
    if (hidden) {
      page.line().line("Show all options with --help-all");
    }
    return page;
  }

  /**
   * Returns the value of each option, for {@code --verbose}, in the sections of help, with where the value came from
   * unless it is the default.
   */
  public static String verboseList(List<Arguments.Option> options, String optionsTitle) {
    var page = HelpPage.create().line("Option values:");
    for (var section : sections(options).entrySet()) {
      page.section(section.getKey() == null ? optionsTitle : section.getKey().name());
      for (var option : section.getValue()) {
        String value = option.value() == null ? "" : "=" + (option.value().isEmpty() ? "''" : option.value());
        page.row("--" + option.name() + value,
          DEFAULT_SOURCE.equals(option.source()) ? "" : "(" + option.source() + ")");
      }
    }
    return page.render();
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
    String example = planetilerCommand("");
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

  private static String flag(Arguments.Option option) {
    // options that aren't planetiler's own, like the -Xmx Java option, come with their own dashes
    String name = option.name().startsWith("-") ? option.name() : "--" + option.name();
    return name + (option.placeholder() == null ? "" : "=" + option.placeholder());
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
}
