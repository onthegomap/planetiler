package com.onthegomap.planetiler;

import com.onthegomap.planetiler.basemap.BasemapMain;
import com.onthegomap.planetiler.basemap.util.VerifyMonaco;
import com.onthegomap.planetiler.benchmarks.BasemapMapping;
import com.onthegomap.planetiler.benchmarks.LongLongMapBench;
import com.onthegomap.planetiler.examples.BikeRouteOverlay;
import com.onthegomap.planetiler.examples.ToiletsOverlay;
import com.onthegomap.planetiler.examples.ToiletsOverlayLowLevelApi;
import com.onthegomap.planetiler.mbtiles.Verify;
import java.util.Arrays;
import java.util.Locale;
import java.util.Map;

/**
 * Main entry-point for executable jar and container distributions of Planetiler, which delegates to
 * individual {@code public static void main(String[] args)} methods of runnable classes.
 */
public class Main {

  private static final EntryPoint DEFAULT_TASK = BasemapMain::main;
  private static final Map<String, EntryPoint> ENTRY_POINTS =
      Map.of(
          "generate-basemap", BasemapMain::main,
          "basemap", BasemapMain::main,
          "example-bikeroutes", BikeRouteOverlay::main,
          "example-toilets", ToiletsOverlay::main,
          "example-toilets-lowlevel", ToiletsOverlayLowLevelApi::main,
          "benchmark-mapping", BasemapMapping::main,
          "benchmark-longlongmap", LongLongMapBench::main,
          "verify-mbtiles", Verify::main,
          "verify-monaco", VerifyMonaco::main);

  public static void main(String[] args) throws Exception {
    EntryPoint task = DEFAULT_TASK;

    if (args.length > 0) {
      String maybeTask = args[0].trim().toLowerCase(Locale.ROOT);
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

    task.main(args);
  }

  @FunctionalInterface
  private interface EntryPoint {

    void main(String[] args) throws Exception;
  }
}
