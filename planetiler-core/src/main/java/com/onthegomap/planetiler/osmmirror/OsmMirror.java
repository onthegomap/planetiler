package com.onthegomap.planetiler.osmmirror;

import static com.onthegomap.planetiler.worker.Worker.joinFutures;

import com.carrotsearch.hppc.LongArrayList;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.reader.osm.OsmBlockSource;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import com.onthegomap.planetiler.reader.osm.OsmPhaser;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.util.CloseableIterator;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.worker.WeightedHandoffQueue;
import com.onthegomap.planetiler.worker.WorkQueue;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.Closeable;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicLong;
import org.msgpack.core.MessagePack;

public interface OsmMirror extends AutoCloseable {

  static OsmMirror newInMemory() {
    return new InMemoryOsmMirror();
  }

  static OsmMirror newSqliteWrite(Path path) {
    return SqliteOsmMirror.newWriteToFileDatabase(path, Arguments.of());
  }

  static OsmMirror newSqliteMemory() {
    return SqliteOsmMirror.newInMemoryDatabase();
  }

  static OsmMirror newDummyWriter() {
    return new DummyOsmMirror();
  }

  static void main(String[] args) throws InterruptedException {
    Arguments arguments = Arguments.fromEnvOrArgs(args);
    var stats = arguments.getStats();
    String type = arguments.getString("db", "type of db to use", "mapdb");
    Path input = arguments.inputFile("input", "input", Path.of("data/sources/massachusetts.osm.pbf"));
    Path output = arguments.file("output", "output", Path.of("data/tmp/output"));
    FileUtils.delete(output);
    FileUtils.createDirectory(output);
    int processThreads = arguments.threads();
    OsmInputFile in = new OsmInputFile(input);
    var blockCounter = new AtomicLong();
    var nodes = new AtomicLong();
    var ways = new AtomicLong();
    var relations = new AtomicLong();
    try (var blocks = in.get()) {
      record Batch(WeightedHandoffQueue<Serialized<? extends OsmElement>> results, OsmBlockSource.Block block) {}
      var queue = new WorkQueue<Batch>("batches", processThreads * 2, 1, stats);

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
          try (queue; var packer = MessagePack.newDefaultBufferPacker()) {
            for (var batch : prev) {
              try (batch.results) {
                for (var item : batch.block) {
                  packer.clear();
                  if (item instanceof OsmElement.Node node) {
                    OsmMirrorUtil.pack(packer, node);
                    batch.results.accept(new Serialized.SerializedNode(node, packer.toByteArray()), item.cost());
                  } else if (item instanceof OsmElement.Way way) {
                    OsmMirrorUtil.pack(packer, way);
                    batch.results.accept(new Serialized.SerializedWay(way, packer.toByteArray()), item.cost());
                  } else if (item instanceof OsmElement.Relation relation) {
                    OsmMirrorUtil.pack(packer, relation);
                    batch.results.accept(new Serialized.SerializedRelation(relation, packer.toByteArray()),
                      item.cost());
                  }
                }
              }
            }
          }
        });

      var writeBranch = pipeline.readFromQueue(queue)
        .sinkTo("write", 1, prev -> {
          var phaser = new OsmPhaser(1);

          try (
            OsmMirror out = newWriter(type, output.resolve("test.db"));
            var writer = out.newBulkWriter();
            var phaserForWorker = phaser.forWorker()
          ) {
            System.err.println("Using " + out.getClass().getSimpleName());
            for (var batch : prev) {
              for (var item : batch.results) {
                if (item instanceof Serialized.SerializedNode node) {
                  phaserForWorker.arrive(OsmPhaser.Phase.NODES);
                  writer.putNode(node);
                  nodes.incrementAndGet();
                } else if (item instanceof Serialized.SerializedWay way) {
                  phaserForWorker.arrive(OsmPhaser.Phase.WAYS);
                  writer.putWay(way);
                  ways.incrementAndGet();
                } else if (item instanceof Serialized.SerializedRelation relation) {
                  phaserForWorker.arrive(OsmPhaser.Phase.RELATIONS);
                  writer.putRelation(relation);
                  relations.incrementAndGet();
                }
              }
              blockCounter.incrementAndGet();
            }
            phaserForWorker.arrive(OsmPhaser.Phase.DONE);
          }
          phaser.printSummary();
        });

      ProgressLoggers loggers = ProgressLoggers.create()
        .addRateCounter("blocks", blockCounter)
        .addRateCounter("nodes", nodes, true)
        .addRateCounter("ways", ways, true)
        .addRateCounter("rels", relations, true)
        .addFileSize(() -> FileUtils.size(output))
        .newLine()
        .addProcessStats()
        .newLine()
        .addPipelineStats(readBranch)
        .addPipelineStats(writeBranch);

      loggers.awaitAndLog(joinFutures(readBranch.done(), writeBranch.done()), Duration.ofSeconds(10));
    }
  }

  static OsmMirror newWriter(String type, Path path) {
    return switch (type) {
      case "mapdb" -> newMapdbWrite(path);
      case "mapdb-memory" -> newMapdbMemory();
      case "sqlite" -> newSqliteWrite(path);
      case "sqlite-memory" -> newSqliteMemory();
      case "memory" -> newInMemory();
      case "lmdb" -> newLmdbWrite(path);
      default -> throw new IllegalArgumentException("Unrecognized type: " + type);
    };
  }

  static OsmMirror newMapdbWrite(Path file) {
    return MapDbOsmMirror.newWriteToFile(file);
  }

  static OsmMirror newMapdbMemory() {
    return MapDbOsmMirror.newInMemory();
  }

  static OsmMirror newLmdbWrite(Path file) {
    return new LmdbOsmMirror(file);
  }

  interface BulkWriter extends Closeable {

    default void putNode(Serialized.SerializedNode node) {
      putNode(node.item());
    }

    default void putWay(Serialized.SerializedWay way) {
      putWay(way.item());
    }

    default void putRelation(Serialized.SerializedRelation node) {
      putRelation(node.item());
    }

    default void putNode(OsmElement.Node node) {
      putNode(new Serialized.SerializedNode(node, OsmMirrorUtil.encode(node)));
    }

    default void putWay(OsmElement.Way way) {
      putWay(new Serialized.SerializedWay(way, OsmMirrorUtil.encode(way)));
    }

    default void putRelation(OsmElement.Relation node) {
      putRelation(new Serialized.SerializedRelation(node, OsmMirrorUtil.encode(node)));
    }
  }

  BulkWriter newBulkWriter();

  void deleteNode(long nodeId, int version);

  void deleteWay(long wayId, int version);

  void deleteRelation(long relationId, int version);

  OsmElement.Node getNode(long id);

  LongArrayList getParentWaysForNode(long nodeId);

  OsmElement.Way getWay(long id);

  OsmElement.Relation getRelation(long id);

  LongArrayList getParentRelationsForNode(long nodeId);

  LongArrayList getParentRelationsForWay(long nodeId);

  LongArrayList getParentRelationsForRelation(long nodeId);

  CloseableIterator<OsmElement> iterator();
}
