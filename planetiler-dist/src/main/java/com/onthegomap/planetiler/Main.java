package com.onthegomap.planetiler;

import static com.onthegomap.planetiler.util.LogUtil.UNTIMED_LOGGER;

import com.onthegomap.planetiler.benchmarks.LongLongMapBench;
import com.onthegomap.planetiler.benchmarks.OpenMapTilesMapping;
import com.onthegomap.planetiler.config.ArgumentHelp;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.HelpPage;
import com.onthegomap.planetiler.custommap.ConfiguredMapMain;
import com.onthegomap.planetiler.custommap.validator.SchemaValidator;
import com.onthegomap.planetiler.examples.BikeRouteOverlay;
import com.onthegomap.planetiler.examples.OsmQaTiles;
import com.onthegomap.planetiler.examples.ToiletsOverlay;
import com.onthegomap.planetiler.examples.ToiletsOverlayLowLevelApi;
import com.onthegomap.planetiler.examples.overture.OvertureBasemap;
import com.onthegomap.planetiler.fuzz.DeterminismFuzzer;
import com.onthegomap.planetiler.mbtiles.Verify;
import com.onthegomap.planetiler.reader.FileFormatException;
import com.onthegomap.planetiler.util.BuildInfo;
import com.onthegomap.planetiler.util.CompareArchives;
import com.onthegomap.planetiler.util.EditDistance;
import com.onthegomap.planetiler.util.TileSizeStats;
import com.onthegomap.planetiler.util.TopOsmTiles;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;
import org.openmaptiles.OpenMapTilesMain;
import org.openmaptiles.util.VerifyMonaco;

/**
 * Main entry-point for executable jar and container distributions of Planetiler, which delegates to individual {@code
 * public static void main(String[] args)} methods of runnable classes.
 */
@SuppressWarnings("java:S106")
public class Main {

  // Set by or for the launcher (launcher/main.go, cache.go), not planetiler options.
  private static final Set<String> LAUNCHER_SETTINGS = Set.of(
    "planetiler.launcher",
    "PLANETILER_JAVA_HOME",
    "PLANETILER_CLASSPATH",
    "PLANETILER_CACHE_DIR");
  static {
    int version = Runtime.version().feature();
    if (version < 25) {
      System.err.println(
        "You are using Java " + version +
          " but Planetiler requires 25 or later, for more details on upgrading see: https://github.com/onthegomap/planetiler/blob/main/CONTRIBUTING.md");
      System.exit(1);
    }
  }

  private static final Task OPENMAPTILES =
    profile("openmaptiles", "OpenMapTiles schema (default)", OpenMapTilesMain::main, "generate-openmaptiles");
  private static final List<Task> TASKS = List.of(
    OPENMAPTILES,
    profile("shortbread", "Shortbread schema", bundledSchema("shortbread.yml"), "generate-shortbread"),
    example("custom", ConfiguredMapMain::main, "generate-custom"),
    example("example-bikeroutes", BikeRouteOverlay::main),
    example("example-toilets", ToiletsOverlay::main),
    example("example-toilets-lowlevel", ToiletsOverlayLowLevelApi::main),
    example("example-overture", OvertureBasemap::main, "overture"),
    example("example-qa", OsmQaTiles::main, "osm-qa"),

    command("compare <a> <b>", "compare two tile archives", CompareArchives::main),
    command("stats", "tile size statistics for an archive", TileSizeStats::main),
    command("verify <schema.yml>", "run a YAML schema's tests", SchemaValidator::main, "verify-custom",
      "verify-schema"),
    command("version", "show the version", _ -> System.out.println(BuildInfo.currentVersionLine())),

    hiddenTool("benchmark-mapping", OpenMapTilesMapping::main),
    hiddenTool("benchmark-longlongmap", LongLongMapBench::main),
    hiddenTool("verify-mbtiles", Verify::main),
    hiddenTool("verify-monaco", VerifyMonaco::main),
    hiddenTool("top-osm-tiles", TopOsmTiles::main),
    hiddenTool("fuzz-determinism", DeterminismFuzzer::main)
  );

  /**
   * A task to run as {@code planetiler <name>}.
   *
   * @param usage       the name followed by its arguments, as listed in the overview (e.g. {@code compare <a> <b>})
   * @param description what it does, or {@code null} to leave it out of the overview
   * @param buildsMap   true for profiles, where {@code planetiler <name>} alone shows the task's help
   * @param aliases     other names that run it (older names)
   */
  private record Task(String usage, String description, boolean buildsMap, EntryPoint main, List<String> aliases) {

    String name() {
      return usage.split("\\s")[0];
    }
  }

  private static Task profile(String name, String description, EntryPoint main, String... aliases) {
    return new Task(name, description, true, main, List.of(aliases));
  }

  private static Task example(String name, EntryPoint main, String... aliases) {
    return new Task(name, null, true, main, List.of(aliases));
  }

  private static Task command(String usage, String description, EntryPoint main, String... aliases) {
    return new Task(usage, description, false, main, List.of(aliases));
  }

  private static Task hiddenTool(String name, EntryPoint main) {
    return new Task(name, null, false, main, List.of());
  }

  private static EntryPoint bundledSchema(String path) {
    return args -> ConfiguredMapMain.main(Stream.concat(
      Stream.of("--schema=" + path),
      Stream.of(args)
    ).toArray(String[]::new));
  }

  private static Map<String, Task> tasksByName() {
    Map<String, Task> result = new LinkedHashMap<>();
    for (var task : TASKS) {
      result.put(task.name(), task);
      task.aliases().forEach(alias -> result.put(alias, task));
    }
    return result;
  }

  public static void main(String[] args) throws Exception {
    try {
      run(args);
    } catch (IllegalArgumentException | FileFormatException e) {
      // Bad options or inputs: the message says what to fix, so the stack trace only shows when asked for.
      if (verbose(args)) {
        throw e;
      }
      // Omit elapsed time since it's the one line saying why the run stopped, often before it started.
      UNTIMED_LOGGER.error("{}", Objects.requireNonNullElse(e.getMessage(), e + " (run with --verbose for details)"));
      System.exit(1);
    }
  }

  private static boolean verbose(String[] args) {
    try {
      return Arguments.fromEnvOrArgs(args).silence().getBoolean("verbose|-v", "", false);
    } catch (IllegalArgumentException _) {
      // a bad --verbose value is the error being reported
      return false;
    }
  }

  private static void run(String[] args) throws Exception {
    // `planetiler help <task>` is the same as `planetiler <task> --help`
    if (args.length == 2 && args[0].equals("help")) {
      args = new String[]{args[1], "--help"};
    }
    if (isHelpRequest(args)) {
      System.out.print(help());
      return;
    }
    EntryPoint task = OPENMAPTILES.main();
    String first = args.length == 0 ? "" : args[0].strip();
    if (first.matches("^.*\\.ya?ml$")) {
      task = ConfiguredMapMain::main;
      ArgumentHelp.setSubcommand(first, null);
      args = args.length == 1 && !configuredOutsideArgs() ? new String[]{"--schema=" + first, "--help"} :
        Stream.concat(Stream.of("--schema=" + first), Stream.of(args).skip(1)).toArray(String[]::new);
    } else if (!first.isEmpty() && !first.contains("=") && !first.startsWith("-")) {
      String name = first.toLowerCase(Locale.ROOT);
      Task found = tasksByName().get(name);
      if (found == null) {
        String closest = EditDistance.closest(name, tasksByName().keySet());
        throw new IllegalArgumentException("unknown profile or command: " + first +
          (closest == null ? "" : " (did you mean " + closest + "?)") + ", see " +
          ArgumentHelp.planetilerCommand("--help"));
      }
      ArgumentHelp.setSubcommand(found.name(), found.description());
      args = args.length == 1 && found.buildsMap() && !configuredOutsideArgs() ? new String[]{"--help"} :
        Arrays.copyOfRange(args, 1, args.length);
      task = found.main();
    }
    task.main(args);
  }

  /**
   * Returns true for {@code planetiler --help}, and for {@code planetiler} with no arguments unless options are set in
   * the environment or JVM properties (e.g. a docker container configured with environment variables).
   */
  private static boolean isHelpRequest(String[] args) {
    if (args.length == 1) {
      return args[0].matches("^(--?help|-h|help)$");
    }
    return args.length == 0 && !configuredOutsideArgs();
  }

  /** Returns true when planetiler options are set in environment variables or JVM properties. */
  private static boolean configuredOutsideArgs() {
    return System.getenv().keySet().stream()
      .anyMatch(key -> key.startsWith("PLANETILER_") && !LAUNCHER_SETTINGS.contains(key)) ||
      System.getProperties().stringPropertyNames().stream()
        .anyMatch(key -> key.startsWith("planetiler.") && !LAUNCHER_SETTINGS.contains(key));
  }

  /** Returns the overview that {@code planetiler --help} prints. */
  static String help() {
    var page = HelpPage.create()
      .line(ArgumentHelp.title())
      .section("Build a map of Monaco")
      .planetilerCommand("--area=monaco --output=monaco.pmtiles")
      .line()
      .usage("[profile] [options]")
      .section("Profiles");
    listedTasks(true).forEach(task -> page.row(task.usage(), task.description()));
    page
      .row("<schema.yml>",
        "your own schema in YAML: https://github.com/onthegomap/planetiler/tree/main/planetiler-custommap")
      .row("<Profile.java>", "your own profile in Java: https://github.com/onthegomap/planetiler-examples")
      .section("Common options")
      .flag("--area=<name>",
        "region to build: monaco, massachusetts, germany, planet, ... (downloaded if missing)")
      .flag("--osm-path=<file>", "use a local .osm.pbf instead")
      .flag("--bounds=<w,s,e,n|planet>", "only build tiles inside these bounds")
      .flag("-o", "--output=<file>", ".pmtiles or .mbtiles");
    if (ArgumentHelp.wasStartedByLauncher()) {
      page.flag("--memory=<size>", "memory limit for planetiler, e.g. 4g");
    } else {
      page.flag("-Xmx<size> (Java option)", "memory limit for planetiler, e.g. java -Xmx4g -jar ...");
    }
    page
      .flag("-f", "--force", "overwrite the output")
      .flag("-v", "--verbose",
        "show option values and where each came from, details of each stage, and stack traces for errors")
      .section("Other commands");
    listedTasks(false).forEach(task -> page.row(task.usage(), task.description()));
    return page
      .line()
      .line("Options for a profile or command:  " + ArgumentHelp.planetilerCommand("<profile or command> --help"))
      .line("Docs, questions and issues: https://github.com/onthegomap/planetiler")
      .render();
  }

  private static Stream<Task> listedTasks(boolean profiles) {
    return TASKS.stream().filter(task -> task.description() != null && task.buildsMap() == profiles);
  }

  @FunctionalInterface
  private interface EntryPoint {

    void main(String[] args) throws Exception;
  }
}
