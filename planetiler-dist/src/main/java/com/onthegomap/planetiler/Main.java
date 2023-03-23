package com.onthegomap.planetiler;

import static java.util.Map.entry;

import com.onthegomap.planetiler.benchmarks.LongLongMapBench;
import com.onthegomap.planetiler.benchmarks.OpenMapTilesMapping;
import com.onthegomap.planetiler.contour.AsterV3;
import com.onthegomap.planetiler.custommap.ConfiguredMapMain;
import com.onthegomap.planetiler.custommap.validator.SchemaValidator;
import com.onthegomap.planetiler.examples.BikeRouteOverlay;
import com.onthegomap.planetiler.examples.OsmQaTiles;
import com.onthegomap.planetiler.examples.ToiletsOverlay;
import com.onthegomap.planetiler.examples.ToiletsOverlayLowLevelApi;
import com.onthegomap.planetiler.mbtiles.Verify;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Stream;
import org.openmaptiles.OpenMapTilesMain;
import org.openmaptiles.util.VerifyMonaco;

/**
 * Main entry-point for executable jar and container distributions of Planetiler, which delegates to individual {@code
 * public static void main(String[] args)} methods of runnable classes.
 */
public class Main {

  private static final EntryPoint DEFAULT_TASK = OpenMapTilesMain::main;
  private static final Map<String, EntryPoint> ENTRY_POINTS = Map.ofEntries(
    entry("generate-openmaptiles", OpenMapTilesMain::main),
    entry("openmaptiles", OpenMapTilesMain::main),

    entry("generate-custom", ConfiguredMapMain::main),
    entry("custom", ConfiguredMapMain::main),

    entry("generate-shortbread", bundledSchema("shortbread.yml")),
    entry("shortbread", bundledSchema("shortbread.yml")),

    entry("verify", SchemaValidator::main),
    entry("verify-custom", SchemaValidator::main),
    entry("verify-schema", SchemaValidator::main),

    entry("example-bikeroutes", BikeRouteOverlay::main),
    entry("example-toilets", ToiletsOverlay::main),
    entry("example-toilets-lowlevel", ToiletsOverlayLowLevelApi::main),

    entry("example-qa", OsmQaTiles::main),
    entry("osm-qa", OsmQaTiles::main),

    entry("benchmark-mapping", OpenMapTilesMapping::main),
    entry("benchmark-longlongmap", LongLongMapBench::main),

    entry("verify-mbtiles", Verify::main),
    entry("verify-monaco", VerifyMonaco::main),

    entry("asterv3", AsterV3::main)
  );

  private static EntryPoint bundledSchema(String path) {
    return args -> ConfiguredMapMain.main(Stream.concat(
      Stream.of("--schema=" + path),
      Stream.of(args)
    ).toArray(String[]::new));
  }

  public static void main(String[] args) throws Exception {
    EntryPoint task = DEFAULT_TASK;

    if (args.length > 0) {
      String maybeTask = args[0].trim().toLowerCase(Locale.ROOT);
      if (maybeTask.matches("^.*\\.ya?ml$")) {
        task = ConfiguredMapMain::main;
        args[0] = "--schema=" + args[0];
      } else {
        EntryPoint taskFromArg0 = ENTRY_POINTS.get(maybeTask);
        if (taskFromArg0 != null) {
          args = Arrays.copyOfRange(args, 1, args.length);
          task = taskFromArg0;
        } else if (!maybeTask.contains("=") && !maybeTask.startsWith("-")) {
          System.err.println("Unrecognized task: " + maybeTask);
          System.err.println("possibilities: " + ENTRY_POINTS.keySet());
          System.exit(1);
        }
      }
    }

    task.main(args);
  }

  @FunctionalInterface
  private interface EntryPoint {

    void main(String[] args) throws Exception;
  }
}
