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
import com.onthegomap.planetiler.worker.WorkQueue;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.Closeable;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
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

  static void main(String[] args) throws Exception {
    Arguments arguments = Arguments.fromEnvOrArgs(args);
    var stats = arguments.getStats();
    String type = arguments.getString("db", "type of db to use", "mapdb");
    boolean doNodes = arguments.getBoolean("nodes", "process nodes", true);
    boolean doWays = arguments.getBoolean("ways", "process ways", true);
    boolean doRelations = arguments.getBoolean("relations", "process relations", true);
    Path input = arguments.inputFile("input", "input", Path.of("data/sources/massachusetts.osm.pbf"));
    Path output = arguments.file("output", "output", Path.of("data/tmp/output"));
    FileUtils.delete(output);
    FileUtils.createDirectory(output);
    int processThreads = arguments.threads();
    OsmInputFile in = new OsmInputFile(input);
    var blockCounter = new AtomicLong();
    try (var blocks = in.get()) {
      record Batch(CompletableFuture<List<Serialized<? extends OsmElement>>> results, OsmBlockSource.Block block) {}
      var queue = new WorkQueue<Batch>("batches", processThreads * 2, 1, stats);

      var phaser = new OsmPhaser(1);
      var pipeline = WorkerPipeline.start("osm2sqlite", stats);
      var readBranch = pipeline.<Batch>fromGenerator("pbf", next -> {
        blocks.forEachBlock(block -> {
          var result = new Batch(new CompletableFuture<>(), block);
          queue.accept(result);
          next.accept(result);
        });
      })
        .addBuffer("pbf_blocks", processThreads * 2)
        .sinkTo("parse", processThreads, (prev) -> {
          try (queue; var packer = MessagePack.newDefaultBufferPacker()) {
            for (var batch : prev) {
              List<Serialized<? extends OsmElement>> result = new ArrayList<>();
              for (var item : batch.block) {
                packer.clear();
                if (item instanceof OsmElement.Node node) {
                  if (doNodes) {
                    OsmMirrorUtil.pack(packer, node);
                    result.add(new Serialized.Node(node, packer.toByteArray()));
                  }
                } else if (item instanceof OsmElement.Way way) {
                  if (doWays) {
                    OsmMirrorUtil.pack(packer, way);
                    result.add(new Serialized.Way(way, packer.toByteArray()));
                  }
                } else if (item instanceof OsmElement.Relation relation) {
                  if (doRelations) {
                    OsmMirrorUtil.pack(packer, relation);
                    result.add(new Serialized.Relation(relation, packer.toByteArray()));
                  }
                }
              }
              batch.results.complete(result);
            }
          }
        });

      var writeBranch = pipeline.readFromQueue(queue)
        .sinkTo("write", 1, prev -> {

          try (
            OsmMirror out = newWriter(type, output.resolve("test.db"));
            var writer = out.newBulkWriter();
            var phaserForWorker = phaser.forWorker()
          ) {
            System.err.println("Using " + out.getClass().getSimpleName());
            for (var batch : prev) {
              for (var item : batch.results.get()) {
                if (item instanceof Serialized.Node node) {
                  phaserForWorker.arrive(OsmPhaser.Phase.NODES);
                  writer.putNode(node);
                } else if (item instanceof Serialized.Way way) {
                  phaserForWorker.arrive(OsmPhaser.Phase.WAYS);
                  writer.putWay(way);
                } else if (item instanceof Serialized.Relation relation) {
                  phaserForWorker.arrive(OsmPhaser.Phase.RELATIONS);
                  writer.putRelation(relation);
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
        .addRateCounter("nodes", phaser::nodes, true)
        .addRateCounter("ways", phaser::ways, true)
        .addRateCounter("rels", phaser::relations, true)
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
      case "dummy" -> newDummyWriter();
      default -> throw new IllegalArgumentException("Unrecognized type: " + type);
    };
  }

  static OsmMirror newReader(String type, Path path) {
    return switch (type) {
      case "mapdb" -> MapDbOsmMirror.newReadFromFile(path);
      case "sqlite" -> newSqliteWrite(path);
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

    default void putNode(Serialized.Node node) {
      putNode(node.item());
    }

    default void putWay(Serialized.Way way) {
      putWay(way.item());
    }

    default void putRelation(Serialized.Relation node) {
      putRelation(node.item());
    }

    default void putNode(OsmElement.Node node) {
      putNode(new Serialized.Node(node, OsmMirrorUtil.encode(node)));
    }

    default void putWay(OsmElement.Way way) {
      putWay(new Serialized.Way(way, OsmMirrorUtil.encode(way)));
    }

    default void putRelation(OsmElement.Relation node) {
      putRelation(new Serialized.Relation(node, OsmMirrorUtil.encode(node)));
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
