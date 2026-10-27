package com.onthegomap.planetiler.fuzz;

import com.onthegomap.planetiler.archive.TileArchiveConfig;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.util.CompareArchives;
import com.onthegomap.planetiler.util.FileUtils;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Checks that planetiler output does not depend on thread timing or on the tuning options that should only affect how
 * fast it runs.
 * <p>
 * Runs the same planetiler command several times, each in a new JVM (some nondeterminism like {@code Set.of} iteration
 * order only changes between JVMs), and fails if the outputs differ:
 * <ol>
 * <li>with the arguments as given</li>
 * <li>again with the same arguments: the output must be byte-for-byte identical</li>
 * <li>then with randomly chosen thread counts, node map types and temp storage options: the tiles must be identical
 * (archive metadata may differ since it records some of these options)</li>
 * </ol>
 * Thread timing varies from run to run, so passing does not prove output is deterministic, but each run with different
 * options gives a race another chance to show up. Failures print the random seed, rerun with {@code --seed} to try the
 * same options again.
 * <p>
 * Usage:
 *
 * <pre>{@code
 * java -jar planetiler.jar fuzz-determinism [--runs=10] [--seed=...] [--dir=data/fuzz-determinism] \
 *   [--main_class=org.openmaptiles.OpenMapTilesMain] [-- <arguments for main_class>]
 * }</pre>
 *
 * Without arguments after {@code --}, it runs the OpenMapTiles profile on the small Monaco, Natural Earth and water
 * polygon files in {@code planetiler-core/src/test/resources}. Don't set the options this fuzzer varies there.
 */
public class DeterminismFuzzer {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeterminismFuzzer.class);

  private DeterminismFuzzer() {}

  /** Runs the fuzzer with {@code [fuzzer options] [-- <planetiler arguments>]}, exits with 1 if outputs differ. */
  public static void main(String[] args) throws IOException, InterruptedException {
    int separator = List.of(args).indexOf("--");
    var arguments = Arguments.fromArgs(separator < 0 ? args : Arrays.copyOfRange(args, 0, separator));
    long seed = arguments.getLong("seed", "random seed to choose options with", new Random().nextLong());
    int runs = arguments.getInteger("runs", "number of runs including the 2 with the arguments as given", 10);
    Path dir = arguments.file("dir", "directory for outputs and logs", Path.of("data", "fuzz-determinism"));
    String mainClass = arguments.getString("main_class", "main class to run", "org.openmaptiles.OpenMapTilesMain");
    // the array node map needs 8 bytes for every ID up to the largest node ID even for small extracts (~100GB with
    // today's OSM IDs) so only use it when asked to
    List<String> nodemapTypes = arguments.getList("nodemap_types", "node map types to choose from",
      List.of("sparsearray", "sortedtable"));
    int threads = arguments.threads();
    List<String> defaultArgs = monacoTestFiles();
    // uncomment to change default args:
    // defaultArgs = List.of("--area=massachusetts");
    List<String> command = separator < 0 ? defaultArgs :
      List.of(Arrays.copyOfRange(args, separator + 1, args.length));

    LOGGER.info("Checking determinism of {} {} with seed {}", mainClass, command, seed);
    Random random = new Random(seed);
    Files.createDirectories(dir);
    Path reference = run(mainClass, command, List.of(), dir, 0);
    for (int i = 1; i < runs; i++) {
      List<String> options = i == 1 ? List.of() : randomOptions(random, threads, nodemapTypes);
      Path output = run(mainClass, command, options, dir, i);
      boolean same = options.isEmpty() ? Files.mismatch(reference, output) < 0 :
        CompareArchives.compare(TileArchiveConfig.from(reference.toString()), TileArchiveConfig.from(output.toString()),
          PlanetilerConfig.defaults(), false).tileDiffs() == 0;
      if (!same) {
        LOGGER.error("Run {} differs from run 0, outputs and logs kept in {}, rerun with --seed={}", i, dir, seed);
        System.exit(1);
      }
      FileUtils.delete(output);
    }
    FileUtils.delete(reference);
    LOGGER.info("All {} runs produced the same tiles", runs);
  }

  private static List<String> monacoTestFiles() {
    Path water = FuzzUtils.testResource("water-polygons-split-3857.zip");
    return List.of(
      "--osm_path=" + FuzzUtils.testResource("monaco-latest.osm.pbf"),
      "--natural_earth_path=" + FuzzUtils.testResource("natural_earth_vector.sqlite.zip"),
      "--water_polygons_path=" + water,
      // no lake centerlines in Monaco, so use any shapefile
      "--lake_centerlines_path=" + water
    );
  }

  /** Returns randomly chosen options that should not change the tiles planetiler generates. */
  private static List<String> randomOptions(Random random, int maxThreads, List<String> nodemapTypes) {
    // up to twice as many threads as asked for, more threads than cores interleave work differently
    int threads = 2 + random.nextInt(2 * maxThreads - 1);
    String nodemapType = nodemapTypes.get(random.nextInt(nodemapTypes.size()));
    // array needs too much memory without mmap storage
    String nodemapStorage = nodemapType.equals("array") || random.nextBoolean() ? "mmap" : "ram";
    return List.of(
      "--threads=" + threads,
      "--process_threads=" + (1 + random.nextInt(threads)),
      "--write_threads=" + (1 + random.nextInt(2)),
      "--feature_read_threads=" + (1 + random.nextInt(2)),
      "--sort_max_readers=" + (1 + random.nextInt(threads)),
      "--sort_max_writers=" + (1 + random.nextInt(threads)),
      "--parallel_tmp_io=" + random.nextBoolean(),
      "--storage=" + (random.nextBoolean() ? "mmap" : "ram"),
      "--nodemap_type=" + nodemapType,
      "--nodemap_storage=" + nodemapStorage,
      "--compress_temp=" + random.nextBoolean(),
      "--osm_lazy_reads=" + random.nextBoolean()
    );
  }

  private static Path run(String mainClass, List<String> command, List<String> options, Path dir, int index)
    throws IOException, InterruptedException {
    LOGGER.info("Run {}: {}", index, options.isEmpty() ? "arguments as given" : String.join(" ", options));
    Path output = dir.resolve("run-" + index + ".pmtiles");
    Path tmp = dir.resolve("tmp-" + index);
    try {
      FuzzUtils.runInNewJvm(mainClass,
        Stream.of(command, options, List.of("--output=" + output, "--tmpdir=" + tmp, "--force"))
          .flatMap(List::stream).toList(),
        dir.resolve("run-" + index + ".log"));
    } finally {
      FileUtils.delete(tmp);
    }
    return output;
  }
}
