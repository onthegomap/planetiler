package com.onthegomap.planetiler;

import com.onthegomap.planetiler.benchmarks.LongLongMapBench;
import com.onthegomap.planetiler.benchmarks.OpenMapTilesMapping;
import com.onthegomap.planetiler.config.ArgumentHelp;
import com.onthegomap.planetiler.config.Arguments;
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
import com.onthegomap.planetiler.util.CompareArchives;
import com.onthegomap.planetiler.util.EditDistance;
import com.onthegomap.planetiler.util.TileSizeStats;
import com.onthegomap.planetiler.util.TopOsmTiles;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.openmaptiles.OpenMapTilesMain;
import org.openmaptiles.util.VerifyMonaco;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Main entry-point for executable jar and container distributions of Planetiler, which delegates to individual {@code
 * public static void main(String[] args)} methods of runnable classes.
 */
@SuppressWarnings("java:S106")
public class Main {
  static {
    int version = Runtime.version().feature();
    if (version < 25) {
      System.err.println(
        "You are using Java " + version +
          " but Planetiler requires 25 or later, for more details on upgrading see: https://github.com/onthegomap/planetiler/blob/main/CONTRIBUTING.md");
      System.exit(1);
    }
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(Main.class);

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

    hidden("benchmark-mapping", OpenMapTilesMapping::main),
    hidden("benchmark-longlongmap", LongLongMapBench::main),
    hidden("verify-mbtiles", Verify::main),
    hidden("verify-monaco", VerifyMonaco::main),
    hidden("top-osm-tiles", TopOsmTiles::main),
    hidden("fuzz-determinism", DeterminismFuzzer::main)
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

  /** A profile that runs but isn't listed in the overview. */
  private static Task example(String name, EntryPoint main, String... aliases) {
    return new Task(name, null, true, main, List.of(aliases));
  }

  private static Task command(String usage, String description, EntryPoint main, String... aliases) {
    return new Task(usage, description, false, main, List.of(aliases));
  }

  /** Benchmarks and other tools for developing planetiler, not listed in the overview. */
  private static Task hidden(String name, EntryPoint main) {
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
      if (Arguments.fromEnvOrArgs(args).silence().getBoolean("verbose", "", false)) {
        throw e;
      }
      LOGGER.error("{} (--verbose shows the stack trace)", e.getMessage());
      System.exit(1);
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
      ArgumentHelp.setSubcommand(first);
      args = args.length == 1 ? new String[]{"--schema=" + first, "--help"} :
        Stream.concat(Stream.of("--schema=" + first), Stream.of(args).skip(1)).toArray(String[]::new);
    } else if (!first.isEmpty() && !first.contains("=") && !first.startsWith("-")) {
      String name = first.toLowerCase(Locale.ROOT);
      Task found = tasksByName().get(name);
      if (found == null) {
        String closest = EditDistance.closest(name, tasksByName().keySet());
        throw new IllegalArgumentException("Unknown profile or command: " + first +
          (closest == null ? "" : " (did you mean " + closest + "?)") + ", see " + ArgumentHelp.command("--help"));
      }
      ArgumentHelp.setSubcommand(found.name());
      args = args.length == 1 && found.buildsMap() ? new String[]{"--help"} : Arrays.copyOfRange(args, 1, args.length);
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
    return args.length == 0 &&
      System.getenv().keySet().stream().noneMatch(key -> key.startsWith("PLANETILER_")) &&
      System.getProperties().stringPropertyNames().stream()
        .noneMatch(key -> key.startsWith("planetiler.") && !key.equals("planetiler.launcher"));
  }

  /** Returns the overview that {@code planetiler --help} prints. */
  static String help() {
    String memory = ArgumentHelp.wasStartedByLauncher() ?
      "  --memory=<size>            memory limit for planetiler, e.g. 4g" :
      "  -Xmx<size> (Java option)   memory limit for planetiler, e.g. java -Xmx4g -jar ...";
    return """
      %s

      Build a map of Monaco:
        %s

      Usage:
        %s

      Profiles:
      %s
        <schema.yml>       your own schema in YAML      https://github.com/onthegomap/planetiler/tree/main/planetiler-custommap
        <Profile.java>     your own profile in Java     https://github.com/onthegomap/planetiler-examples

      Common options:
        --area=<name>              region to build: monaco, massachusetts, germany, planet, ... (downloaded if missing)
        --osm-path=<file>          use a local .osm.pbf instead
        --bounds=<w,s,e,n|planet>  only build tiles inside these bounds
        -o, --output=<file>        .pmtiles or .mbtiles
      %s
        -f, --force                overwrite the output
        --verbose                  show option values and where each came from

      Other commands:
      %s

      Options for a profile or command:  %s
      Docs, questions and issues: https://github.com/onthegomap/planetiler
      """
      .formatted(
        ArgumentHelp.title(),
        ArgumentHelp.command("--area=monaco --output=monaco.pmtiles"),
        ArgumentHelp.command("[profile] [options]"),
        overviewList(true, "  %-19s%s"),
        memory,
        overviewList(false, "  %-27s%s"),
        ArgumentHelp.command("<profile or command> --help"));
  }

  private static String overviewList(boolean profiles, String format) {
    return TASKS.stream()
      .filter(task -> task.description() != null && task.buildsMap() == profiles)
      .map(task -> format.formatted(task.usage(), task.description()))
      .collect(Collectors.joining("\n"));
  }

  @FunctionalInterface
  private interface EntryPoint {

    void main(String[] args) throws Exception;
  }
}
