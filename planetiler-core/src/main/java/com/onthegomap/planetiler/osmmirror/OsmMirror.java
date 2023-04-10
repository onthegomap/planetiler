package com.onthegomap.planetiler.osmmirror;

import static com.onthegomap.planetiler.worker.Worker.joinFutures;

import com.carrotsearch.hppc.LongArrayList;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.reader.osm.OsmBlockSource;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import com.onthegomap.planetiler.reader.osm.OsmPhaser;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.stats.Timer;
import com.onthegomap.planetiler.util.CloseableIterator;
import com.onthegomap.planetiler.util.DiskBacked;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.worker.WorkQueue;
import com.onthegomap.planetiler.worker.Worker;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.Closeable;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.msgpack.core.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface OsmMirror extends AutoCloseable, DiskBacked {
  Logger LOGGER = LoggerFactory.getLogger(OsmMirror.class);

  static OsmMirror newInMemory() {
    return new InMemoryOsmMirror();
  }

  static OsmMirror newSqliteWrite(Path path, int maxWorkers) {
    return SqliteOsmMirror.newWriteToFileDatabase(path, Arguments.of(), maxWorkers);
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
    String password = arguments.getString("password", "password");
    Path input = arguments.inputFile("input", "input", Path.of("data/sources/massachusetts.osm.pbf"));
    Path output = arguments.file("output", "output", Path.of("data/tmp/output"));
    FileUtils.delete(output);
    FileUtils.createDirectory(output);
    int processThreads = arguments.threads();
    OsmInputFile in = new OsmInputFile(input);
    var blockCounter = new AtomicLong();
    boolean id = !type.contains("sqlite") && !type.contains("postgres");
    var timer = Timer.start();
    try (
      var blocks = in.get();
      OsmMirror out = newWriter(type, output.resolve("test.db"), processThreads, password)
    ) {
      record Batch(CompletableFuture<List<Serialized<? extends OsmElement>>> results, OsmBlockSource.Block block) {}
      var queue = new WorkQueue<Batch>("batches", processThreads * 2, 1, stats);

      int threads = type.contains("postgres") ? 4 : 1;
      var phaser = new OsmPhaser(threads);
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
                    OsmMirrorUtil.pack(packer, node, id);
                    result.add(new Serialized.Node(node, packer.toByteArray()));
                  }
                } else if (item instanceof OsmElement.Way way) {
                  if (doWays) {
                    OsmMirrorUtil.pack(packer, way, id);
                    result.add(new Serialized.Way(way, packer.toByteArray()));
                  }
                } else if (item instanceof OsmElement.Relation relation) {
                  if (doRelations) {
                    OsmMirrorUtil.pack(packer, relation, id);
                    result.add(new Serialized.Relation(relation, packer.toByteArray()));
                  }
                }
              }
              batch.results.complete(result);
            }
          }
        });

      var writeBranch = pipeline.readFromQueue(queue)
        .sinkTo("write", threads, prev -> {

          try (
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
        });

      ProgressLoggers loggers = ProgressLoggers.create()
        .addRateCounter("blocks", blockCounter)
        .addRateCounter("nodes", phaser::nodes, true)
        .addRateCounter("ways", phaser::ways, true)
        .addRateCounter("rels", phaser::relations, true)
        .addFileSize(out)
        .newLine()
        .addProcessStats()
        .newLine()
        .addPipelineStats(readBranch)
        .addPipelineStats(writeBranch);


      loggers.awaitAndLog(joinFutures(readBranch.done(), writeBranch.done()), Duration.ofSeconds(10));
      var worker = new Worker("finish", stats, 1, out::finish);
      var pl2 = ProgressLoggers.create()
        .addFileSize(out)
        .newLine()
        .addThreadPoolStats("worker", worker)
        .newLine()
        .addProcessStats();
      worker.awaitAndLog(pl2, Duration.ofSeconds(10));
      LOGGER.info("Finished in {} final size {}", timer.stop(), Format.defaultInstance().storage(out.diskUsageBytes()));
      phaser.printSummary();
    }
  }

  default void finish() {}

  static OsmMirror newWriter(String type, Path path, int maxWorkers, String password) {
    return switch (type) {
      case "mapdb" -> newMapdbWrite(path);
      case "mapdb-memory" -> newMapdbMemory();
      case "sqlite" -> newSqliteWrite(path, maxWorkers);
      case "sqlite-memory" -> newSqliteMemory();
      case "memory" -> newInMemory();
      case "lmdb" -> newLmdbWrite(path);
      case "dummy" -> newDummyWriter();
      case "postgres" -> PostgresOsmMirror.newMirror(
        "jdbc:postgresql://localhost:54321/pgdb?user=admin&password=" + password, 1,
        Stats.inMemory());
      default -> throw new IllegalArgumentException("Unrecognized type: " + type);
    };
  }

  static OsmMirror newReader(String type, Path path) {
    return switch (type) {
      case "mapdb" -> MapDbOsmMirror.newReadFromFile(path);
      case "sqlite" -> SqliteOsmMirror.newReadFromFile(path);
      case "postgres" -> PostgresOsmMirror.newMirror(
        "jdbc:postgresql://localhost:5432/pgdb?user=admin&password=password", 1,
        Stats.inMemory());
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

    void putNode(Serialized.Node node);

    void putWay(Serialized.Way way);

    void putRelation(Serialized.Relation node);
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

  default CloseableIterator<Serialized<? extends OsmElement>> iterator(int shard, int shards) {
    throw new UnsupportedOperationException();
  }

  default CloseableIterator<Serialized<? extends OsmElement>> lazyIter() {
    throw new UnsupportedOperationException();
  }
}
