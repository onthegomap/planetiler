package com.onthegomap.planetiler.osmmirror;

import static com.onthegomap.planetiler.worker.Worker.joinFutures;

import com.conveyal.osmlib.Node;
import com.conveyal.osmlib.OSM;
import com.conveyal.osmlib.OSMEntity;
import com.conveyal.osmlib.Relation;
import com.conveyal.osmlib.Way;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.reader.osm.OsmBlockSource;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import com.onthegomap.planetiler.reader.osm.OsmPhaser;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Timer;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.worker.WeightedHandoffQueue;
import com.onthegomap.planetiler.worker.WorkQueue;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicLong;

public class OsmLibLoader {

  public static void main(String[] args) throws MalformedURLException {
    Timer timer = Timer.start();

    Arguments arguments = Arguments.fromEnvOrArgs(args);
    var stats = arguments.getStats();
    Path input = Path.of("data/sources/massachusetts.osm.pbf");
    Path outputTmp = Path.of("data/tmp/massachusetts-tmp");
    Path output = outputTmp.resolve("data");

    FileUtils.delete(output);
    FileUtils.createDirectory(outputTmp);
    OSM osm = new OSM(output.toString());
    int processThreads = arguments.threads();
    OsmInputFile in = new OsmInputFile(input);
    var blockCounter = new AtomicLong();
    var nodes = new AtomicLong();
    var ways = new AtomicLong();
    var relations = new AtomicLong();
    try (var blocks = in.get()) {
      record Batch(WeightedHandoffQueue<OsmElement> results, OsmBlockSource.Block block) {}
      var queue = new WorkQueue<Batch>("batches", 1_000, 1, stats);

      var pipeline = WorkerPipeline.start("osm2sqlite", stats);
      var readBranch = pipeline.<Batch>fromGenerator("pbf", next -> {
        blocks.forEachBlock(block -> {
          var result = new Batch(new WeightedHandoffQueue<>(1_000, 100), block);
          queue.accept(result);
          next.accept(result);
        });
      })
        .addBuffer("pbf_blocks", processThreads * 2)
        .sinkTo("parse", processThreads, (prev) -> {
          try (queue) {
            for (var batch : prev) {
              try (batch.results) {
                for (var item : batch.block) {
                  batch.results.accept(item, 1);
                }
              }
            }
          }
        });

      var writeBranch = pipeline.readFromQueue(queue)
        .sinkTo("write", 1, prev -> {
          var phaser = new OsmPhaser(1);
          osm.writeBegin();

          try (
            var phaserForWorker = phaser.forWorker()
          ) {
            for (var batch : prev) {
              for (var item : batch.results) {
                if (item instanceof OsmElement.Node node) {
                  phaserForWorker.arrive(OsmPhaser.Phase.NODES);
                  var outNode = new Node(node.lat(), node.lon());
                  applyTags(node, outNode);
                  osm.writeNode(node.id(), outNode);
                  nodes.incrementAndGet();
                } else if (item instanceof OsmElement.Way way) {
                  phaserForWorker.arrive(OsmPhaser.Phase.WAYS);
                  var outWay = new Way();
                  outWay.nodes = way.nodes().toArray();
                  applyTags(way, outWay);
                  osm.writeWay(way.id(), outWay);
                  ways.incrementAndGet();
                } else if (item instanceof OsmElement.Relation relation) {
                  phaserForWorker.arrive(OsmPhaser.Phase.RELATIONS);
                  var outRel = new Relation();
                  for (var member : relation.members()) {
                    var outMember = new Relation.Member();
                    outMember.role = member.role();
                    outMember.id = member.ref();
                    outMember.type = switch (member.type()) {
                      case NODE -> OSMEntity.Type.NODE;
                      case WAY -> OSMEntity.Type.WAY;
                      case RELATION -> OSMEntity.Type.RELATION;
                    };
                    outRel.members.add(outMember);
                  }
                  applyTags(relation, outRel);
                  osm.writeRelation(relation.id(), outRel);
                  relations.incrementAndGet();
                }
              }
              blockCounter.incrementAndGet();
            }
            phaserForWorker.arrive(OsmPhaser.Phase.DONE);
            osm.writeEnd();
          }
          phaser.printSummary();
        });

      ProgressLoggers loggers = ProgressLoggers.create()
        .addRateCounter("blocks", blockCounter)
        .addRateCounter("nodes", nodes, true)
        .addRateCounter("ways", ways, true)
        .addRateCounter("rels", relations, true)
        .addFileSize(() -> FileUtils.size(outputTmp))
        .newLine()
        .addProcessStats()
        .newLine()
        .addPipelineStats(readBranch)
        .addPipelineStats(writeBranch);

      loggers.awaitAndLog(joinFutures(readBranch.done(), writeBranch.done()), Duration.ofSeconds(10));
    }

    osm.close();
    System.err.println(timer.stop());
  }

  private static void applyTags(OsmElement node, OSMEntity outNode) {
    if (!node.tags().isEmpty()) {
      outNode.tags = new ArrayList<>(node.tags().size());
      for (var entry : node.tags().entrySet()) {
        outNode.tags.add(new OSMEntity.Tag(entry.getKey(), entry.getValue().toString()));
      }
    }
  }
}
