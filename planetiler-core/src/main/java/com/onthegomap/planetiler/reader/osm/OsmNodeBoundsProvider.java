package com.onthegomap.planetiler.reader.osm;

import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility for extracting the lat/lon bounds of all the points in a {@code .osm.pbf} file.
 */
public class OsmNodeBoundsProvider implements Bounds.Provider {
  private static final Logger LOGGER = LoggerFactory.getLogger(OsmNodeBoundsProvider.class);

  private final OsmInputFile file;
  private final Stats stats;
  private final PlanetilerConfig config;

  public OsmNodeBoundsProvider(OsmInputFile file, PlanetilerConfig config, Stats stats) {
    this.file = file;
    this.config = config;
    this.stats = stats;
  }

  @Override
  public Envelope getLatLonBounds() {
    LOGGER.warn("Bounds not found in header for {} and --bounds not provided, parsing bounds from nodes",
      file.getPath().getFileName());

    var timer = stats.startStage("osm_bounds");

    // If the node location writer supports parallel writes, then parse, process, and write node locations from worker threads
    int parseThreads = Math.max(1, config.threads() - 1);
    var phaser = new OsmPhaser(parseThreads);
    List<Envelope> envelopes = new CopyOnWriteArrayList<>();

    var pipeline = WorkerPipeline.start("osm_bounds", stats)
      .fromGenerator("read", file.get()::forEachBlock)
      .addBuffer("pbf_blocks", parseThreads * 2)
      .sinkTo("process", parseThreads, blocks -> {
        var envelope = new Envelope();
        envelopes.add(envelope);
        try (var worker = phaser.forWorker()) {
          for (var block : blocks) {
            for (var element : block) {
              if (element instanceof OsmElement.Node node) {
                worker.arrive(OsmPhaser.Phase.NODES);
                envelope.expandToInclude(node.lon(), node.lat());
              } else if (element instanceof OsmElement.Way) {
                worker.arrive(OsmPhaser.Phase.WAYS);
              } else if (element instanceof OsmElement.Relation) {
                worker.arrive(OsmPhaser.Phase.RELATIONS);
              }
            }
          }
        }
      });

    var loggers = ProgressLoggers.create()
      .addRateCounter("nodes", phaser::nodes, true)
      .addRateCounter("ways", phaser::ways, true)
      .addRateCounter("rels", phaser::relations, true)
      .newLine()
      .addProcessStats()
      .newLine()
      .addPipelineStats(pipeline);

    pipeline.awaitAndLog(loggers, config.logInterval());
    phaser.printSummary();
    timer.stop();

    Envelope allBounds = new Envelope();
    for (var env : envelopes) {
      allBounds.expandToInclude(env);
    }
    return allBounds;
  }
}
