package com.onthegomap.planetiler.osmmirror;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.reader.osm.OsmPhaser;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.worker.Worker;
import java.nio.file.Path;
import java.time.Duration;

public class ReadOsmMirror {

  public static void main(String[] args) {
    var arguments = Arguments.fromEnvOrArgs(args);
    var input = arguments.inputFile("input", "input file", Path.of("data/tmp/massachusetts-tmp/test.db"));
    var db = arguments.getString("db", "db type", "sqlite");
    var stats = arguments.getStats();
    var threads = arguments.threads();
    var phaser = new OsmPhaser(threads);
    // deserializing:
    // 0:00:25 DEB -   nodes: 33,997,633 (1.5M/s) in 22s cpu:23s avg:1
    //0:00:25 DEB -   ways: 3,776,868 (1.4M/s) in 3s cpu:3s avg:1.1
    //0:00:25 DEB -   relations: 28,041 (387k/s) in 0.1s cpu:0.2s avg:2.8
    // lazy:
    //0:00:13 DEB -   nodes: 33,997,633 (3M/s) in 11s cpu:11s avg:1
    //0:00:13 DEB -   ways: 3,776,868 (2.4M/s) in 2s cpu:2s avg:1
    //0:00:13 DEB -   relations: 28,041 (1M/s) in 0s cpu:0s avg:1.5
    var worker = new Worker("read", stats, threads, shard -> {
      try (
        var mirror = OsmMirror.newReader(db, input);
        var iter = mirror.iterator(shard, threads);
        var workerPhaser = phaser.forWorker()
      ) {
        while (iter.hasNext()) {
          var element = iter.next();
          if (element instanceof Serialized.LazyNode node) {
            workerPhaser.arrive(OsmPhaser.Phase.NODES);
          } else if (element instanceof Serialized.LazyWay way) {
            workerPhaser.arrive(OsmPhaser.Phase.WAYS);
          } else if (element instanceof Serialized.LazyRelation relation) {
            workerPhaser.arrive(OsmPhaser.Phase.RELATIONS);
          }
        }
      }
    });
    var logger = ProgressLoggers.create()
      .addRateCounter("nodes", phaser::nodes, true)
      .addRateCounter("ways", phaser::ways, true)
      .addRateCounter("relations", phaser::relations, true)
      .newLine()
      .addThreadPoolStats("read", worker)
      .newLine()
      .addProcessStats();
    worker.awaitAndLog(logger, Duration.ofSeconds(10));
    phaser.printSummary();
  }
}
