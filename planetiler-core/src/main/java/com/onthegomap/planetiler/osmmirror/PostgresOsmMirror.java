package com.onthegomap.planetiler.osmmirror;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.CloseableIterator;
import de.bytefish.pgbulkinsert.row.SimpleRowWriter;
import de.bytefish.pgbulkinsert.util.PostgreSqlUtils;
import java.io.Closeable;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import org.postgresql.PGConnection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PostgresOsmMirror implements OsmMirror {

  private static final Logger LOGGER = LoggerFactory.getLogger(PostgresOsmMirror.class);
  private final Connection connection;
  private final Stats stats;
  private final int maxWorkers;
  private final String url;

  public static void main(String[] args) throws Exception {
    try (
      var mirror = PostgresOsmMirror.newMirror("jdbc:postgresql://localhost:5432/pgdb?user=admin&password=password", 1,
        Stats.inMemory());
      var bulk = mirror.newBulkWriter()
    ) {
      bulk.putNode(new Serialized.Node(new OsmElement.Node(1, 2, 3), false));
      System.err.println("put node");
    }
  }

  private PostgresOsmMirror(Connection connection, Stats stats, int maxWorkers, String url) {
    this.connection = connection;
    this.stats = stats;
    this.maxWorkers = maxWorkers;
    this.url = url;
  }

  public static PostgresOsmMirror newMirror(String url, int maxWorkers, Stats stats) {
    try {
      var connection = DriverManager.getConnection(url);
      var result = new PostgresOsmMirror(connection, stats, maxWorkers, url);
      result.execute(
        // https://pgtune.leopard.in.ua/
        "ALTER SYSTEM SET max_connections = '20'",
        "ALTER SYSTEM SET shared_buffers = '50GB'",
        "ALTER SYSTEM SET effective_cache_size = '150GB'",
        "ALTER SYSTEM SET maintenance_work_mem = '2GB'",
        "ALTER SYSTEM SET checkpoint_completion_target = '0.9'",
        "ALTER SYSTEM SET wal_buffers = '16MB'",
        "ALTER SYSTEM SET default_statistics_target = '500'",
        "ALTER SYSTEM SET random_page_cost = '1.1'",
        "ALTER SYSTEM SET effective_io_concurrency = '200'",
        "ALTER SYSTEM SET work_mem = '64MB'",
        "ALTER SYSTEM SET min_wal_size = '4GB'",
        "ALTER SYSTEM SET max_wal_size = '16GB'",
        "ALTER SYSTEM SET max_worker_processes = '40'",
        "ALTER SYSTEM SET max_parallel_workers_per_gather = '20'",
        "ALTER SYSTEM SET max_parallel_workers = '40'",
        "ALTER SYSTEM SET max_parallel_maintenance_workers = '4'"
      // pgconfig.org
      //        "ALTER SYSTEM SET shared_buffers TO '50GB'",
      //        "ALTER SYSTEM SET effective_cache_size TO '150GB'",
      //        "ALTER SYSTEM SET work_mem TO '1GB'",
      //        "ALTER SYSTEM SET maintenance_work_mem TO '10GB'",
      //        "ALTER SYSTEM SET min_wal_size TO '2GB'",
      //        "ALTER SYSTEM SET max_wal_size TO '3GB'",
      //        "ALTER SYSTEM SET checkpoint_completion_target TO '0.9'",
      //        "ALTER SYSTEM SET wal_buffers TO '-1'",
      //        "ALTER SYSTEM SET listen_addresses TO '*'",
      //        "ALTER SYSTEM SET max_connections TO '100'",
      //        "ALTER SYSTEM SET random_page_cost TO '1.1'",
      //        "ALTER SYSTEM SET effective_io_concurrency TO '200'",
      //        "ALTER SYSTEM SET max_worker_processes TO '8'",
      //        "ALTER SYSTEM SET max_parallel_workers_per_gather TO '2'",
      //        "ALTER SYSTEM SET max_parallel_workers TO '2'"
      );
      result.dropTables();
      result.createTables();
      return result;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void execute(String... commands) {
    for (String command : commands) {
      try {
        connection.prepareStatement(command).execute();
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  void createTables() {
    execute("""
      CREATE TABLE IF NOT EXISTS nodes (
        id BIGINT,
        data BYTEA
      )""");
    execute("""
      CREATE TABLE IF NOT EXISTS ways (
        id BIGINT,
        data BYTEA
      )""");
    execute("""
      CREATE TABLE IF NOT EXISTS relations (
        id BIGINT,
        data BYTEA
      )""");
    execute("""
      CREATE TABLE IF NOT EXISTS node_to_way (
        child_id BIGINT,
        parent_id BIGINT
      )""");
    execute("""
      CREATE TABLE IF NOT EXISTS node_to_relation (
        child_id BIGINT,
        parent_id BIGINT
      )""");
    execute("""
      CREATE TABLE IF NOT EXISTS way_to_relation (
        child_id BIGINT,
        parent_id BIGINT
      )""");
    execute("""
      CREATE TABLE IF NOT EXISTS relation_to_relation (
        child_id BIGINT,
        parent_id BIGINT
      )""");
  }

  void dropTables() {
    for (String table : List.of("nodes", "ways", "relations", "node_to_way", "node_to_relation", "way_to_relation",
      "relation_to_relation")) {
      execute("DROP TABLE IF EXISTS " + table);
    }
    execute("VACUUM");
  }


  private void execute(String query) {
    try (var statement = connection.createStatement()) {
      LOGGER.debug("Execute: {}", query);
      statement.execute(query);
    } catch (SQLException throwables) {
      throw new IllegalStateException("Error executing queries " + query, throwables);
    }
  }

  @Override
  public long diskUsageBytes() {
    try (var statement = connection.createStatement()) {
      var resultSet = statement.executeQuery("SELECT pg_database_size(current_database())");
      resultSet.next();
      return resultSet.getLong(1);
    } catch (SQLException throwables) {
      throw new IllegalStateException("Error getting DB size", throwables);
    }
  }

  record Writer(Connection conn, SimpleRowWriter writer) implements Closeable {
    @Override
    public void close() {
      try (conn; writer) {
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }
  }

  private class Bulk implements BulkWriter {

    private final Writer wayWriter;
    private final Writer relWriter;
    private final Writer nodeToWayWriter;
    private final Writer nodeToRelWriter;
    private final Writer wayToRelWriter;
    private final Writer relToRelWriter;
    private final Writer nodeWriter;

    private Writer newWriter(String table, String... columns) {
      try {
        Connection conn = DriverManager.getConnection(url);
        PGConnection pgConnection = PostgreSqlUtils.getPGConnection(conn);
        return new Writer(conn, new SimpleRowWriter(new SimpleRowWriter.Table("public", table, columns), pgConnection));
      } catch (SQLException e) {
        throw new RuntimeException(e);
      }
    }

    private Bulk() {
      nodeWriter = newWriter("nodes", "id", "data");
      wayWriter = newWriter("ways", "id", "data");
      relWriter = newWriter("relations", "id", "data");
      nodeToWayWriter = newWriter("node_to_way", "child_id", "parent_id");
      nodeToRelWriter = newWriter("node_to_relation", "child_id", "parent_id");
      wayToRelWriter = newWriter("way_to_relation", "child_id", "parent_id");
      relToRelWriter = newWriter("relation_to_relation", "child_id", "parent_id");
    }

    @Override
    public void putNode(Serialized.Node node) {
      nodeWriter.writer().startRow(row -> {
        row.setLong(0, node.item().id());
        row.setByteArray(1, node.bytes());
      });
    }

    @Override
    public void putWay(Serialized.Way way) {
      wayWriter.writer().startRow(row -> {
        row.setLong(0, way.item().id());
        row.setByteArray(1, way.bytes());
      });
      LongSet nodes = new LongHashSet();
      for (var node : way.item().nodes()) {
        if (nodes.add(node.value)) {
          nodeToWayWriter.writer().startRow(row -> {
            row.setLong(0, node.value);
            row.setLong(1, way.item().id());
          });
        }
      }
    }

    @Override
    public void putRelation(Serialized.Relation rel) {
      relWriter.writer().startRow(row -> {
        row.setLong(0, rel.item().id());
        row.setByteArray(1, rel.bytes());
      });
      LongSet nodes = new LongHashSet();
      LongSet ways = new LongHashSet();
      LongSet rels = new LongHashSet();
      for (var member : rel.item().members()) {
        if (member.type() == OsmElement.Type.NODE && nodes.add(member.ref())) {
          nodeToRelWriter.writer().startRow(row -> {
            row.setLong(0, member.ref());
            row.setLong(1, rel.item().id());
          });
        } else if (member.type() == OsmElement.Type.WAY && ways.add(member.ref())) {
          wayToRelWriter.writer().startRow(row -> {
            row.setLong(0, member.ref());
            row.setLong(1, rel.item().id());
          });
        } else if (member.type() == OsmElement.Type.RELATION && rels.add(member.ref())) {
          relToRelWriter.writer().startRow(row -> {
            row.setLong(0, member.ref());
            row.setLong(1, rel.item().id());
          });
        }
      }
    }

    @Override
    public void close() throws IOException {
      try (
        nodeWriter;
        wayWriter;
        relWriter;
        nodeToWayWriter;
        nodeToRelWriter;
        wayToRelWriter;
        relToRelWriter;
      ) {
      }
    }
  }

  @Override
  public BulkWriter newBulkWriter() {
    return new Bulk();
  }

  @Override
  public void deleteNode(long nodeId, int version) {

  }

  @Override
  public void deleteWay(long wayId, int version) {

  }

  @Override
  public void deleteRelation(long relationId, int version) {

  }

  @Override
  public OsmElement.Node getNode(long id) {
    return null;
  }

  @Override
  public LongArrayList getParentWaysForNode(long nodeId) {
    return null;
  }

  @Override
  public OsmElement.Way getWay(long id) {
    return null;
  }

  @Override
  public OsmElement.Relation getRelation(long id) {
    return null;
  }

  @Override
  public LongArrayList getParentRelationsForNode(long nodeId) {
    return null;
  }

  @Override
  public LongArrayList getParentRelationsForWay(long nodeId) {
    return null;
  }

  @Override
  public LongArrayList getParentRelationsForRelation(long nodeId) {
    return null;
  }

  @Override
  public CloseableIterator<OsmElement> iterator() {
    return null;
  }

  @Override
  public void close() throws Exception {
    connection.close();
  }

  @Override
  public void finish() {
    for (String table : List.of("nodes", "ways", "relations")) {
      execute("ALTER TABLE " + table + " ADD PRIMARY KEY (id)");
    }
    for (String table : List.of("node_to_way", "node_to_relation", "way_to_relation", "relation_to_relation")) {
      execute("ALTER TABLE " + table + " ADD PRIMARY KEY (child_id, parent_id)");
    }
  }
}
