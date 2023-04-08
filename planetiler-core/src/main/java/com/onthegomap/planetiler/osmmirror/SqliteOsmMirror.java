package com.onthegomap.planetiler.osmmirror;

import com.carrotsearch.hppc.LongArrayList;
import com.carrotsearch.hppc.LongHashSet;
import com.carrotsearch.hppc.LongSet;
import com.google.common.collect.Iterators;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.stats.Timer;
import com.onthegomap.planetiler.util.CloseableIterator;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;
import org.sqlite.SQLiteOpenMode;

// java -Xmx30g -cp planetiler-osmmirror.jar com.onthegomap.planetiler.osmmirror.OsmMirror --input data/sources/planet.osm.pbf --output planet.db --threads 10 --db sqlite 2>&1 | tee sqlite2.txt
public class SqliteOsmMirror implements OsmMirror {

  private static final boolean INSERT_IGNORE = false;

  private static final Logger LOGGER = LoggerFactory.getLogger(SqliteOsmMirror.class);
  private final Connection connection;
  private final Path path;
  private final Stats stats;
  private final int maxWorkers;

  private SqliteOsmMirror(Connection connection, Path path, Stats stats, int maxWorkers) {
    this.connection = connection;
    this.path = path;
    this.stats = stats;
    this.maxWorkers = maxWorkers;
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

  public static SqliteOsmMirror newWriteToFileDatabase(Path path, Arguments options, int maxWorkers) {
    Objects.requireNonNull(path);
    SQLiteConfig sqliteConfig = new SQLiteConfig();
    sqliteConfig.setJournalMode(SQLiteConfig.JournalMode.OFF);
    sqliteConfig.setSynchronous(SQLiteConfig.SynchronousMode.OFF);
    sqliteConfig.setCacheSize(1_000_000); // 1GB
    sqliteConfig.setLockingMode(SQLiteConfig.LockingMode.EXCLUSIVE);
    sqliteConfig.setTempStore(SQLiteConfig.TempStore.MEMORY);
    var connection = newConnection("jdbc:sqlite:" + path.toAbsolutePath(), sqliteConfig, options);
    var result = new SqliteOsmMirror(connection, path, options.getStats(), maxWorkers);
    result.createTables();
    return result;
  }

  public static SqliteOsmMirror newInMemoryDatabase() {
    SQLiteConfig sqliteConfig = new SQLiteConfig();
    var connection = newConnection("jdbc:sqlite::memory:", sqliteConfig, Arguments.of());
    var result = new SqliteOsmMirror(connection, null, Stats.inMemory(), 1);
    result.createTables();
    return result;
  }

  public static OsmMirror newReadFromFile(Path path) {
    SQLiteConfig config = new SQLiteConfig();
    config.setReadOnly(true);
    config.setExplicitReadOnly(true);
    config.setCacheSize(10_000);
    config.setLockingMode(SQLiteConfig.LockingMode.EXCLUSIVE);
    config.setOpenMode(SQLiteOpenMode.NOMUTEX);
    config.setPragma(SQLiteConfig.Pragma.MMAP_SIZE, "100000");
    var connection = newConnection("jdbc:sqlite:" + path.toAbsolutePath(), config, Arguments.of());
    return new SqliteOsmMirror(connection, null, Stats.inMemory(), 1);
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
    execute("""
      CREATE TABLE IF NOT EXISTS nodes (
        id INTEGER PRIMARY KEY,
        data BLOB
      ) WITHOUT ROWID""");
    execute("""
      CREATE TABLE IF NOT EXISTS ways (
        id INTEGER PRIMARY KEY,
        data BLOB
      ) WITHOUT ROWID""");
    execute("""
      CREATE TABLE IF NOT EXISTS relations (
        id INTEGER PRIMARY KEY,
        data BLOB
      ) WITHOUT ROWID""");
    execute("""
      CREATE TABLE IF NOT EXISTS node_to_way (
        child_id INTEGER,
        parent_id INTEGER,
        PRIMARY KEY(child_id, parent_id)
      ) WITHOUT ROWID""");
    execute("""
      CREATE TABLE IF NOT EXISTS node_to_relation (
        child_id INTEGER,
        parent_id INTEGER,
        PRIMARY KEY(child_id, parent_id)
      ) WITHOUT ROWID""");
    execute("""
      CREATE TABLE IF NOT EXISTS way_to_relation (
        child_id INTEGER,
        parent_id INTEGER,
        PRIMARY KEY(child_id, parent_id)
      ) WITHOUT ROWID""");
    execute("""
      CREATE TABLE IF NOT EXISTS relation_to_relation (
        child_id INTEGER,
        parent_id INTEGER,
        PRIMARY KEY(child_id, parent_id)
      ) WITHOUT ROWID""");
  }

  @Override
  public BulkWriter newBulkWriter() {
    return new Bulk();
  }

  @Override
  public void deleteNode(long nodeId, int version) {
    try (var statement = connection.prepareStatement("DELETE FROM nodes WHERE ID=?")) {
      statement.setLong(1, nodeId);
      statement.execute();
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void deleteWay(long wayId, int version) {
    try (var statement = connection.prepareStatement("DELETE FROM ways WHERE ID=?")) {
      statement.setLong(1, wayId);
      statement.execute();
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public void deleteRelation(long relationId, int version) {
    try (var statement = connection.prepareStatement("DELETE FROM relations WHERE ID=?")) {
      statement.setLong(1, relationId);
      statement.execute();
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public OsmElement.Node getNode(long id) {
    try (var statement = connection.prepareStatement("SELECT * FROM nodes WHERE id=?")) {
      statement.setLong(1, id);
      var result = statement.executeQuery();
      return result.next() ? parseNode(result) : null;
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  private OsmElement.Way parseWay(ResultSet result) {
    try {
      return OsmMirrorUtil.decodeWay(result.getLong("id"), result.getBytes("data"));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private OsmElement.Node parseNode(ResultSet result) {
    try {
      return OsmMirrorUtil.decodeNode(result.getLong("id"), result.getBytes("data"));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private OsmElement.Relation parseRelation(ResultSet result) {
    try {
      return OsmMirrorUtil.decodeRelation(result.getLong("id"), result.getBytes("data"));
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public LongArrayList getParentWaysForNode(long nodeId) {
    return getParents(nodeId, "node_to_way");
  }

  private LongArrayList parseLongList(ResultSet resultSet) {
    LongArrayList result = new LongArrayList();
    try {
      while (resultSet.next()) {
        result.add(resultSet.getLong(1));
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
    return result;
  }

  @Override
  public OsmElement.Way getWay(long id) {
    try (var statement = connection.prepareStatement("SELECT * FROM ways WHERE id=?")) {
      statement.setLong(1, id);
      var result = statement.executeQuery();
      return result.next() ? parseWay(result) : null;
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public OsmElement.Relation getRelation(long id) {
    try (var statement = connection.prepareStatement("SELECT * FROM relations WHERE id=?")) {
      statement.setLong(1, id);
      var result = statement.executeQuery();
      return result.next() ? parseRelation(result) : null;
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public LongArrayList getParentRelationsForNode(long nodeId) {
    return getParents(nodeId, "node_to_relation");
  }

  private LongArrayList getParents(long childId, String table) {
    try (
      var statement = connection.prepareStatement(
        """
          SELECT parent_id
          FROM %s
          WHERE child_id=?
          ORDER BY parent_id ASC
          """.formatted(table))
    ) {
      statement.setLong(1, childId);
      var result = statement.executeQuery();
      return parseLongList(result);
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public LongArrayList getParentRelationsForWay(long nodeId) {
    return getParents(nodeId, "way_to_relation");
  }

  @Override
  public LongArrayList getParentRelationsForRelation(long nodeId) {
    return getParents(nodeId, "relation_to_relation");
  }

  private <T> Iterator<T> resultToIterator(ResultSet resultSet, Function<ResultSet, T> parse) {
    return new Iterator<>() {
      boolean hasNext;

      {
        try {
          hasNext = resultSet.next();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
      }

      @Override
      public boolean hasNext() {
        return hasNext;
      }

      @Override
      public T next() {
        if (!hasNext) {
          throw new NoSuchElementException();
        }
        T result = parse.apply(resultSet);
        try {
          hasNext = resultSet.next();
        } catch (SQLException e) {
          throw new RuntimeException(e);
        }
        return result;
      }
    };
  }

  @Override
  public CloseableIterator<Serialized<? extends OsmElement>> iterator(int shard, int shards) {
    try {
      var nodestmt = connection.prepareStatement("SELECT * FROM nodes WHERE id % ? = ? ORDER BY id ASC");
      nodestmt.setLong(1, shards);
      nodestmt.setLong(2, shard);
      var nodes = nodestmt.executeQuery();
      var waystmt = connection.prepareStatement("SELECT * FROM ways WHERE id % ? = ? ORDER BY id ASC");
      waystmt.setLong(1, shards);
      waystmt.setLong(2, shard);
      var ways = waystmt.executeQuery();
      var relationstmt =
        connection.prepareStatement("SELECT * FROM relations WHERE id % ? = ? ORDER BY id ASC");
      relationstmt.setLong(1, shards);
      relationstmt.setLong(2, shard);
      var relations = relationstmt.executeQuery();
      var nodesIter =
        resultToIterator(nodes, result -> {
          try {
            return new Serialized.LazyNode(result.getLong("id"), result.getBytes("data"));
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
      var waysIter =
        resultToIterator(ways, result -> {
          try {
            return new Serialized.LazyWay(result.getLong("id"), result.getBytes("data"));
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
      var relsIter =
        resultToIterator(relations, result -> {
          try {
            return new Serialized.LazyRelation(result.getLong("id"), result.getBytes("data"));
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
      var iter = Iterators.concat(nodesIter, waysIter, relsIter);
      return new CloseableIterator<>() {
        @Override
        public boolean hasNext() {
          return iter.hasNext();
        }

        @Override
        public Serialized<? extends OsmElement> next() {
          return iter.next();
        }

        @Override
        public void close() {
          try (nodes; ways; relations) {
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
      };
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public CloseableIterator<OsmElement> iterator() {
    try {
      var nodes = connection.prepareStatement("SELECT * FROM nodes ORDER BY id ASC").executeQuery();
      var ways = connection.prepareStatement("SELECT * FROM ways ORDER BY id ASC").executeQuery();
      var relations = connection.prepareStatement("SELECT * FROM relations ORDER BY id ASC").executeQuery();
      var nodesIter = resultToIterator(nodes, this::parseNode);
      var waysIter = resultToIterator(ways, this::parseWay);
      var relsIter = resultToIterator(relations, this::parseRelation);
      var iter = Iterators.concat(nodesIter, waysIter, relsIter);
      return new CloseableIterator<>() {
        @Override
        public boolean hasNext() {
          return iter.hasNext();
        }

        @Override
        public OsmElement next() {
          return iter.next();
        }

        @Override
        public void close() {
          try (nodes; ways; relations) {
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
      };
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public CloseableIterator<Serialized<? extends OsmElement>> lazyIter() {
    try {
      var nodes = connection.prepareStatement("SELECT * FROM nodes ORDER BY id ASC").executeQuery();
      var ways = connection.prepareStatement("SELECT * FROM ways ORDER BY id ASC").executeQuery();
      var relations = connection.prepareStatement("SELECT * FROM relations ORDER BY id ASC").executeQuery();
      var nodesIter =
        resultToIterator(nodes, result -> {
          try {
            return new Serialized.LazyNode(result.getLong("id"), result.getBytes("data"));
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
      var waysIter =
        resultToIterator(ways, result -> {
          try {
            return new Serialized.LazyWay(result.getLong("id"), result.getBytes("data"));
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
      var relsIter =
        resultToIterator(relations, result -> {
          try {
            return new Serialized.LazyRelation(result.getLong("id"), result.getBytes("data"));
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        });
      var iter = Iterators.concat(nodesIter, waysIter, relsIter);
      return new CloseableIterator<>() {
        @Override
        public boolean hasNext() {
          return iter.hasNext();
        }

        @Override
        public Serialized<? extends OsmElement> next() {
          return iter.next();
        }

        @Override
        public void close() {
          try (nodes; ways; relations) {
          } catch (SQLException e) {
            throw new RuntimeException(e);
          }
        }
      };
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void close() throws IOException {
    try {
      connection.close();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private record ParentChild(long child, long parent) {}

  private class Bulk implements BulkWriter {

    private final ElementWriter nodeWriter = new ElementWriter(connection, "nodes");
    private final ElementWriter wayWriter = new ElementWriter(connection, "ways");
    private final ChildToParentWriter wayMemberWriter = new ChildToParentWriter(connection, "node_to_way");
    private final ElementWriter relationWriter = new ElementWriter(connection, "relations");
    private final ChildToParentWriter nodeToRelWriter = new ChildToParentWriter(connection, "node_to_relation");
    private final ChildToParentWriter wayToRelWriter = new ChildToParentWriter(connection, "way_to_relation");
    private final ChildToParentWriter relToRelWriter = new ChildToParentWriter(connection, "relation_to_relation");
    private final LongLongSorter nodeToWay = path == null ? new LongLongSorter.InMemory() :
      new LongLongSorter.DiskBacked(path.resolveSibling("node_to_way_tmp"), stats, maxWorkers);

    @Override
    public void putNode(Serialized.Node node) {
      nodeWriter.write(node);
    }

    @Override
    public void putWay(Serialized.Way way) {
      wayWriter.write(way);
      long wayId = way.item().id();
      var nodes = way.item().nodes();
      LongSet written = new LongHashSet();
      for (int i = 0; i < nodes.size(); i++) {
        long id = nodes.get(i);
        if (written.add(id)) {
          nodeToWay.put(id, wayId);
        }
      }
    }

    @Override
    public void putRelation(Serialized.Relation relation) {
      relationWriter.write(relation);
      LongHashSet nodes = new LongHashSet();
      LongHashSet ways = new LongHashSet();
      LongHashSet rels = new LongHashSet();
      for (int i = 0; i < relation.item().members().size(); i++) {
        var member = relation.item().members().get(i);
        var set = (switch (member.type()) {
          case NODE -> nodes;
          case WAY -> ways;
          case RELATION -> rels;
        });
        if (set.add(member.ref())) {
          (switch (member.type()) {
            case NODE -> nodeToRelWriter;
            case WAY -> wayToRelWriter;
            case RELATION -> relToRelWriter;
          }).write(new ParentChild(member.ref(), relation.item().id()));
        }
      }
    }

    @Override
    public void close() throws IOException {
      var counter = new AtomicLong(0);
      var timer = Timer.start();
      LOGGER.info("Inserting {} sorted way members...", nodeToWay.count());
      var pipeline = WorkerPipeline.start("write", stats)
        .<ParentChild>fromGenerator("read", next -> {
          for (var item : nodeToWay) {
            next.accept(new ParentChild(item.a(), item.b()));
          }
        })
        .addBuffer("to_write", 100_000, 1_000)
        .sinkTo("write", 1, prev -> {
          for (var item : prev) {
            wayMemberWriter.write(item);
            counter.incrementAndGet();
          }
        });
      ProgressLoggers loggers = ProgressLoggers.create()
        .addRatePercentCounter("way_members", nodeToWay.count(), counter, true)
        .addFileSize(() -> FileUtils.size(path))
        .newLine()
        .addPipelineStats(pipeline)
        .newLine()
        .addProcessStats();
      pipeline.awaitAndLog(loggers, Duration.ofSeconds(10));
      LOGGER.info("Inserted sorted way members in {}", timer.stop());
      nodeWriter.close();
      wayWriter.close();
      wayMemberWriter.close();
      relationWriter.close();
      nodeToRelWriter.close();
      wayToRelWriter.close();
      relToRelWriter.close();
    }
  }

  private class ElementWriter extends Mbtiles.BatchedTableWriterBase<Serialized<? extends OsmElement>> {

    ElementWriter(Connection conn, String table) {
      super(table, List.of("id", "data"), INSERT_IGNORE, conn);
    }

    @Override
    protected int setParamsInStatementForItem(int positionOffset, PreparedStatement statement,
      Serialized<? extends OsmElement> item)
      throws SQLException {
      statement.setLong(positionOffset++, item.item().id());
      statement.setBytes(positionOffset++, item.bytes());
      return positionOffset;
    }
  }

  private class ChildToParentWriter extends Mbtiles.BatchedTableWriterBase<ParentChild> {

    ChildToParentWriter(Connection conn, String table) {
      super(table, List.of("child_id", "parent_id"), INSERT_IGNORE, conn);
    }

    @Override
    protected int setParamsInStatementForItem(int positionOffset, PreparedStatement statement,
      ParentChild item) throws SQLException {
      statement.setLong(positionOffset++, item.child());
      statement.setLong(positionOffset++, item.parent());
      return positionOffset;
    }
  }
}
