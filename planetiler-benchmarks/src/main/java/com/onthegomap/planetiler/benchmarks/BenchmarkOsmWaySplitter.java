package com.onthegomap.planetiler.benchmarks;

import com.carrotsearch.hppc.LongArrayList;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import com.onthegomap.planetiler.reader.osm.OsmWaySplitter;
import com.onthegomap.planetiler.util.Format;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class BenchmarkOsmWaySplitter {
  private static int numWays;

  public static void main(String[] args) {
    List<LongArrayList> wayNodes = new ArrayList<>();
    try (var reader = OsmInputFile.readFrom(Path.of("data", "sources", "massachusetts.osm.pbf"))) {
      reader.forEachBlock(block -> {
        for (var element : block.decodeElements()) {
          if (element instanceof OsmElement.Way way) {
            wayNodes.add(way.nodes());
          }
        }
      });
    }

    for (int i = 0; i < 10; i++) {
      System.err.println(String.join("\t",
        timePerSec(wayNodes, OsmWaySplitter::mapSplitter),
        timePerSec(wayNodes, OsmWaySplitter::roaringBitmapSplitter)
      ));
    }
    System.err.println(numWays);
  }

  private static String timePerSec(List<LongArrayList> ways, Supplier<OsmWaySplitter> make) {
    OsmWaySplitter splitter = make.get();
    long start = System.nanoTime();
    try (var writer = splitter.writerForThread()) {
      for (var way : ways) {
        writer.addWay(way);
      }
    }
    long indexed = System.nanoTime();
    for (var way : ways) {
      numWays += splitter.getSplitIndices(way).size();
    }
    long done = System.nanoTime();
    return Format.defaultInstance()
      .numeric(Math.round(ways.size() * 1d / ((indexed - start) * 1d / Duration.ofSeconds(1).toNanos())), true) + "\t" +
      Format.defaultInstance()
        .numeric(Math.round(ways.size() * 1d / ((done - indexed) * 1d / Duration.ofSeconds(1).toNanos())), true);
  }
}
