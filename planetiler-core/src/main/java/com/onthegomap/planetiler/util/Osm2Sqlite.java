package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmInputFile;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.msgpack.core.MessagePack;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;

public class Osm2Sqlite implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Osm2Sqlite.class);
  private final NodeWriter nodeWriter;
  private final WayWriter wayWriter;
  private final WayMembersWriter wayMemberWriter;
  private final RelationWriter relationWriter;
  private final RelationMembersWriter relationMemberWriter;
  private final Connection connection;

  private Osm2Sqlite(Connection connection) {
    this.connection = connection;
    createTables();
    this.nodeWriter = new NodeWriter(connection);
    this.wayWriter = new WayWriter(connection);
    this.relationWriter = new RelationWriter(connection);
    this.wayMemberWriter = new WayMembersWriter(connection);
    this.relationMemberWriter = new RelationMembersWriter(connection);
  }

  private static byte[] encodeTags(Map<String, Object> tags) {
    try (var msgPack = MessagePack.newDefaultBufferPacker()) {
      msgPack.packMapHeader(tags.size());
      for (var entry : tags.entrySet()) {
        msgPack.packString(entry.getKey());
        if (entry.getValue() == null) {
          msgPack.packNil();
        } else {
          msgPack.packString(entry.getValue().toString());
        }
      }
      return msgPack.toByteArray();
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static Connection newConnection(String url, SQLiteConfig defaults, Arguments args) {
    try {
      args = args.copy().silence();
      var config = new SQLiteConfig(defaults.toProperties());
      for (var pragma : SQLiteConfig.Pragma.values()) {
        var value = args.getString(pragma.getPragmaName(), pragma.getPragmaName(), null);
        if (value != null) {
          LOGGER.info("Setting custom mbtiles sqlite pragma {}={}", pragma.getPragmaName(), value);
          config.setPragma(pragma, value);
        }
      }
      return DriverManager.getConnection(url, config.toProperties());
    } catch (SQLException throwables) {
      throw new IllegalArgumentException("Unable to open " + url, throwables);
    }
  }

  public static Osm2Sqlite newWriteToFileDatabase(Path path, Arguments options) {
    Objects.requireNonNull(path);
    SQLiteConfig sqliteConfig = new SQLiteConfig();
    sqliteConfig.setJournalMode(SQLiteConfig.JournalMode.OFF);
    sqliteConfig.setSynchronous(SQLiteConfig.SynchronousMode.OFF);
    sqliteConfig.setCacheSize(1_000_000); // 1GB
    sqliteConfig.setLockingMode(SQLiteConfig.LockingMode.EXCLUSIVE);
    sqliteConfig.setTempStore(SQLiteConfig.TempStore.MEMORY);
    var connection = newConnection("jdbc:sqlite:" + path.toAbsolutePath(), sqliteConfig, options);
    return new Osm2Sqlite(connection);
  }

  public static void main(String[] args) {
    Arguments arguments = Arguments.fromEnvOrArgs(args);
    var stats = arguments.getStats();
    Path input = arguments.inputFile("input", "input");
    Path output = arguments.file("output", "output");
    FileUtils.delete(output);
    int processThreads = arguments.threads();
    OsmInputFile in = new OsmInputFile(input);
    var blockCounter = new AtomicLong();
    var nodes = new AtomicLong();
    var ways = new AtomicLong();
    var relations = new AtomicLong();
    try (var blocks = in.get()) {

      // TODO iterated in order?
      var pipeline = WorkerPipeline.start("wikidata", stats)
        .fromGenerator("pbf", blocks::forEachBlock)
        .addBuffer("pbf_blocks", processThreads * 2)
        .<OsmElement>addWorker("parse", processThreads, (prev, next) -> {
          for (var block : prev) {
            for (var item : block) {
              next.accept(item);
            }
          }
        })
        .addBuffer("write_queue", 1_000_000, 1_000)
        .sinkTo("write", 1, prev -> {
          try (Osm2Sqlite out = Osm2Sqlite.newWriteToFileDatabase(output, Arguments.of())) {
            for (var item : prev) {
              if (item instanceof OsmElement.Node node) {
                out.write(node);
                nodes.incrementAndGet();
              } else if (item instanceof OsmElement.Way way) {
                out.write(way);
                ways.incrementAndGet();
              } else if (item instanceof OsmElement.Relation relation) {
                out.write(relation);
                relations.incrementAndGet();
              }
            }
            blockCounter.incrementAndGet();
          }
        });

      ProgressLoggers loggers = ProgressLoggers.create()
        .addRateCounter("blocks", blockCounter)
        .addRateCounter("nodes", nodes, true)
        .addRateCounter("ways", ways, true)
        .addRateCounter("rels", relations, true)
        .addFileSize(output)
        .newLine()
        .addProcessStats()
        .newLine()
        .addPipelineStats(pipeline);

      pipeline.awaitAndLog(loggers, Duration.ofSeconds(10));
    }
  }

  private void execute(String query) {
    try (var statement = connection.createStatement()) {
      LOGGER.debug("Execute: {}", query);
      statement.execute(query);
    } catch (SQLException throwables) {
      throw new IllegalStateException("Error executing queries " + query, throwables);
    }
  }

  private void createTables() {
    /*
    0:00:10 INF -  blocks: [ 8.2M 820k/s ] nodes: [ 8.2M 820k/s ] ways: [    0    0/s ] rels: [    0    0/s ] 203M
    cpus: 1.6 gc:  5% heap: 1.1G/8.5G direct: 210k postGC: 587M
    pbf( 1%) ->   (21/31) -> parse( 3%  2%  2%  3%  2%  3%  3%  3%  3%  2%) ->   (1M/1M) -> write(93%)
    0:00:20 INF -  blocks: [  17M 886k/s ] nodes: [  17M 886k/s ] ways: [    0    0/s ] rels: [    0    0/s ] 425M
    cpus: 1.4 gc:  4% heap: 1.1G/8.5G direct: 210k postGC: 595M
    pbf( 0%) ->   (21/31) -> parse( 1%  1%  1%  1%  1%  1%  1%  1%  1%  1%) ->   (1M/1M) -> write(96%)
    0:00:30 INF -  blocks: [  25M 882k/s ] nodes: [  25M 882k/s ] ways: [    0    0/s ] rels: [    0    0/s ] 640M
    cpus: 1.4 gc:  4% heap: 733M/8.5G direct: 210k postGC: 599M
    pbf( 0%) ->   (21/31) -> parse( 1%  1%  1%  1%  1%  1%  1%  1%  1%  1%) ->   (1M/1M) -> write(96%)
    0:00:40 INF -  blocks: [  33M 725k/s ] nodes: [  33M 721k/s ] ways: [  34k 3.4k/s ] rels: [    0    0/s ] 862M
    cpus: 1.5 gc:  4% heap: 2.7G/8.5G direct: 1.8M postGC: 1.1G
    pbf( 0%) ->   (21/31) -> parse( 2%  2%  2%  2%  2%  2%  2%  2%  2%  2%) ->   (1M/1M) -> write(93%)
    0:00:50 INF -  blocks: [  33M  44k/s ] nodes: [  33M    0/s ] ways: [ 482k  44k/s ] rels: [    0    0/s ] 1.2G
    cpus: 1 gc:  1% heap: 1.9G/8.5G direct: 1.8M postGC: 1.4G
    pbf( 0%) ->   (21/31) -> parse( 0%  0%  0%  0%  0%  0%  0%  0%  0%  0%) ->   (1M/1M) -> write(94%)
    0:01:00 INF -  blocks: [  33M  16k/s ] nodes: [  33M    0/s ] ways: [ 647k  16k/s ] rels: [    0    0/s ] 1.5G
    cpus: 0.9 gc:  0% heap: 2.1G/8.5G direct: 1.8M postGC: 1.4G
    pbf( 0%) ->   (21/31) -> parse( 0%  0%  0%  0%  0%  0%  0%  0%  0%  0%) ->   (1M/1M) -> write(84%)
    0:01:10 INF -  blocks: [  34M  87k/s ] nodes: [  33M    0/s ] ways: [ 1.5M  87k/s ] rels: [    0    0/s ] 1.9G
    cpus: 1.1 gc:  2% heap: 2.7G/8.5G direct: 1.8M postGC: 1.8G
    pbf( 0%) ->   (21/31) -> parse( 0%  0%  0%  0%  0%  0%  0%  0%  0%  0%) ->   (1M/1M) -> write(89%)
    0:01:20 INF -  blocks: [  35M  95k/s ] nodes: [  33M    0/s ] ways: [ 2.4M  95k/s ] rels: [    0    0/s ] 2.5G
    cpus: 1.2 gc:  2% heap: 2.3G/8.5G direct: 1.8M postGC: 976M
    pbf( 0%) ->   (21/31) -> parse( 1%  1%  1%  1%  1%  1%  1%  1%  1%  0%) ->   (1M/1M) -> write(95%)
    0:01:30 INF -  blocks: [  36M  53k/s ] nodes: [  33M    0/s ] ways: [   3M  53k/s ] rels: [    0    0/s ] 2.8G
    cpus: 0.9 gc:  1% heap: 2.4G/8.5G direct: 49k postGC: 1G
    pbf( -%) ->    (0/31) -> parse( -%  -%  -%  -%  -%  -%  -%  -%  -%  -%) -> (672k/1M) -> write(76%)
    0:01:40 INF -  blocks: [  36M  44k/s ] nodes: [  33M    0/s ] ways: [ 3.4M  44k/s ] rels: [    0    0/s ] 3.1G
    cpus: 0.8 gc:  0% heap: 2.9G/8.5G direct: 49k postGC: 1G
    pbf( -%) ->    (0/31) -> parse( -%  -%  -%  -%  -%  -%  -%  -%  -%  -%) -> (227k/1M) -> write(72%)
     */
    execute("""
      CREATE TABLE nodes (
        id INTEGER PRIMARY KEY,
        version INTEGER,
        tags BLOB,
        location INTEGER
      ) WITHOUT ROWID""");
    execute("""
      CREATE TABLE ways (
        id INTEGER PRIMARY KEY,
        version INTEGER,
        tags BLOB
      ) WITHOUT ROWID""");
    execute("""
      CREATE TABLE relations (
        id INTEGER PRIMARY KEY,
        version INTEGER,
        tags BLOB
      ) WITHOUT ROWID""");
    execute("""
      CREATE TABLE way_members (
        way_id INTEGER,
        `order` INTEGER,
        node_id INTEGER,
        version INTEGER,
        PRIMARY KEY(way_id, `order`)
      ) WITHOUT ROWID""");
    execute("CREATE INDEX way_members_node_idx ON way_members (node_id)");
    execute("""
      CREATE TABLE relation_members (
        relation_id INTEGER,
        `order` INTEGER,
        type INTEGER,
        version INTEGER,
        role TEXT,
        ref INTEGER,
        PRIMARY KEY(relation_id, `order`)
      ) WITHOUT ROWID""");
    execute("CREATE INDEX relation_members_type_ref_idx ON relation_members (type, ref)");
  }

  private void write(OsmElement.Node node) {
    nodeWriter.write(node);
  }

  private void write(OsmElement.Way way) {
    wayWriter.write(way);
    for (int i = 0; i < way.nodes().size(); i++) {
      wayMemberWriter.write(new ParentChild<>(way, i));
    }
  }

  private void write(OsmElement.Relation relation) {
    relationWriter.write(relation);
    for (int i = 0; i < relation.members().size(); i++) {
      relationMemberWriter.write(new ParentChild<>(relation, i));
    }
  }

  @Override
  public void close() throws IOException {
    try {
      nodeWriter.close();
      wayWriter.close();;
      wayMemberWriter.close();;
      relationWriter.close();;
      relationMemberWriter.close();;
      connection.close();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private record ParentChild<P> (P parent, int idx) {}

  private class NodeWriter extends Mbtiles.BatchedTableWriterBase<OsmElement.Node> {
    NodeWriter(Connection conn) {
      super("nodes", List.of("id", "version", "tags", "location"), false, conn);
    }

    @Override
    protected int setParamsInStatementForItem(int positionOffset, PreparedStatement statement, OsmElement.Node item)
      throws SQLException {
      statement.setLong(positionOffset++, item.id());
      statement.setInt(positionOffset++, item.info().version());
      statement.setBytes(positionOffset++, encodeTags(item.tags()));
      statement.setLong(positionOffset++, item.encodedLocation());
      return positionOffset;
    }
  }

  private class WayWriter extends Mbtiles.BatchedTableWriterBase<OsmElement.Way> {
    WayWriter(Connection conn) {
      super("ways", List.of("id", "version", "tags"), false, conn);
    }

    @Override
    protected int setParamsInStatementForItem(int positionOffset, PreparedStatement statement, OsmElement.Way item)
      throws SQLException {
      statement.setLong(positionOffset++, item.id());
      statement.setInt(positionOffset++, item.info().version());
      statement.setBytes(positionOffset++, encodeTags(item.tags()));
      return positionOffset;
    }
  }

  private class RelationWriter extends Mbtiles.BatchedTableWriterBase<OsmElement.Relation> {
    RelationWriter(Connection conn) {
      super("relations", List.of("id", "version", "tags"), false, conn);
    }

    @Override
    protected int setParamsInStatementForItem(int positionOffset, PreparedStatement statement, OsmElement.Relation item)
      throws SQLException {
      statement.setLong(positionOffset++, item.id());
      statement.setInt(positionOffset++, item.info().version());
      statement.setBytes(positionOffset++, encodeTags(item.tags()));
      return positionOffset;
    }
  }

  private class RelationMembersWriter
    extends Mbtiles.BatchedTableWriterBase<ParentChild<OsmElement.Relation>> {
    RelationMembersWriter(Connection conn) {
      super("relation_members", List.of("relation_id", "`order`", "type", "version", "role", "ref"), false, conn);
    }

    @Override
    protected int setParamsInStatementForItem(int positionOffset, PreparedStatement statement,
      ParentChild<OsmElement.Relation> item) throws SQLException {
      var relation = item.parent;
      var member = item.parent.members().get(item.idx);
      statement.setLong(positionOffset++, relation.id());
      statement.setInt(positionOffset++, item.idx);
      statement.setInt(positionOffset++, member.type().ordinal());
      statement.setInt(positionOffset++, relation.info().version());
      statement.setString(positionOffset++, member.role());
      statement.setLong(positionOffset++, member.ref());
      return positionOffset;
    }
  }

  private class WayMembersWriter extends Mbtiles.BatchedTableWriterBase<ParentChild<OsmElement.Way>> {
    WayMembersWriter(Connection conn) {
      super("way_members", List.of("way_id", "`order`", "node_id", "version"), false, conn);
    }

    @Override
    protected int setParamsInStatementForItem(int positionOffset, PreparedStatement statement,
      ParentChild<OsmElement.Way> item) throws SQLException {
      var way = item.parent;
      statement.setLong(positionOffset++, way.id());
      statement.setInt(positionOffset++, item.idx);
      statement.setLong(positionOffset++, way.nodes().get(item.idx));
      statement.setInt(positionOffset++, way.info().version());
      return positionOffset;
    }
  }
}
