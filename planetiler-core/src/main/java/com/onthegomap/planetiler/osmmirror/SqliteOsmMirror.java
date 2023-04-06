package com.onthegomap.planetiler.osmmirror;

import static com.onthegomap.planetiler.osmmirror.OsmMirrorUtil.encodeTags;
import static com.onthegomap.planetiler.osmmirror.OsmMirrorUtil.parseTags;

import com.carrotsearch.hppc.LongArrayList;
import com.google.common.collect.Iterators;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.util.CloseableIterator;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;

public class SqliteOsmMirror implements OsmMirror {
  // TODO
  // - try letting it finish on a cheaper machine (ran out of 450g disk at 534m ways)
  // - try custom mapdb implementation
  //   - nodes
  //   - ways
  //     - nodeToParentWay
  //   - rels
  //     - nodeToParentRel
  //     - wayToParentRel
  //     - relToParentRel

  private static final boolean INSERT_IGNORE = false;

  private static final Logger LOGGER = LoggerFactory.getLogger(SqliteOsmMirror.class);
  private final Connection connection;

  private SqliteOsmMirror(Connection connection) {
    this.connection = connection;
    createTables();
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

  public static SqliteOsmMirror newWriteToFileDatabase(Path path, Arguments options) {
    Objects.requireNonNull(path);
    SQLiteConfig sqliteConfig = new SQLiteConfig();
    sqliteConfig.setJournalMode(SQLiteConfig.JournalMode.OFF);
    sqliteConfig.setSynchronous(SQLiteConfig.SynchronousMode.OFF);
    sqliteConfig.setCacheSize(1_000_000); // 1GB
    sqliteConfig.setLockingMode(SQLiteConfig.LockingMode.EXCLUSIVE);
    sqliteConfig.setTempStore(SQLiteConfig.TempStore.MEMORY);
    var connection = newConnection("jdbc:sqlite:" + path.toAbsolutePath(), sqliteConfig, options);
    return new SqliteOsmMirror(connection);
  }

  public static SqliteOsmMirror newInMemoryDatabase() {
    SQLiteConfig sqliteConfig = new SQLiteConfig();
    var connection = newConnection("jdbc:sqlite::memory:", sqliteConfig, Arguments.of());
    return new SqliteOsmMirror(connection);
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
    execute("CREATE INDEX IF NOT EXISTS way_members_node_idx ON way_members (node_id)");
    execute("CREATE INDEX IF NOT EXISTS relation_members_type_ref_idx ON relation_members (type, ref)");
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

  }

  @Override
  public void deleteRelation(long relationId, int version) {

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
      String nodeString = result.getString("nodes");
      LongArrayList nodeIds;
      if (nodeString != null && !nodeString.isBlank()) {
        String[] nodes = result.getString("nodes").split(",");
        long[] nodeIdArray = new long[nodes.length];
        for (int i = 0; i < nodes.length; i++) {
          nodeIdArray[i] = Long.parseLong(nodes[i]);
        }
        nodeIds = LongArrayList.from(nodeIdArray);
      } else {
        nodeIds = LongArrayList.from();
      }
      return new OsmElement.Way(
        result.getInt("id"),
        parseTags(result.getBytes("tags")),
        nodeIds,
        OsmElement.Info.forVersion(result.getInt("version"))
      );
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private OsmElement.Node parseNode(ResultSet result) {
    try {
      return new OsmElement.Node(
        result.getInt("id"),
        parseTags(result.getBytes("tags")),
        result.getLong("location"),
        OsmElement.Info.forVersion(result.getInt("version"))
      );
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private OsmElement.Relation parseRelation(ResultSet result) {
    try {
      String refsString = result.getString("refs");
      List<OsmElement.Relation.Member> members;
      if (refsString != null && !refsString.isBlank()) {
        String[] refs = result.getString("refs").split(",");
        String[] types = result.getString("types").split(",");
        String[] roles = result.getString("roles").split(",");
        members = new ArrayList<>(refs.length);

        for (int i = 0; i < refs.length; i++) {
          members.add(new OsmElement.Relation.Member(
            OsmElement.Type.values()[Integer.parseInt(types[i])],
            Long.parseLong(refs[i]),
            roles[i]
          ));
        }
      } else {
        members = List.of();
      }
      return new OsmElement.Relation(
        result.getInt("id"),
        parseTags(result.getBytes("tags")),
        members,
        OsmElement.Info.forVersion(result.getInt("version"))
      );
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public LongArrayList getParentWaysForNode(long nodeId) {
    try (
      var statement = connection.prepareStatement(
        """
          SELECT way_id
          FROM way_members
          WHERE node_id=?
          ORDER BY way_id ASC
          """)
    ) {
      statement.setLong(1, nodeId);
      var result = statement.executeQuery();
      return parseLongList(result);
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
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
    try (
      var statement = connection.prepareStatement(
        """
          SELECT ways.*, group_concat(way_members.node_id) nodes
          FROM ways
          INNER JOIN way_members on ways.id=way_members.way_id
          WHERE ways.id=?
          ORDER BY way_members.`order` ASC
          """)
    ) {
      statement.setLong(1, id);
      var result = statement.executeQuery();
      return result.next() && result.getLong("id") == id ? parseWay(result) : null;
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public OsmElement.Relation getRelation(long id) {
    try (
      var statement = connection.prepareStatement(
        """
          SELECT
            relations.*,
            group_concat(relation_members.role) roles,
            group_concat(relation_members.type) types,
            group_concat(relation_members.ref) refs
          FROM relations
          JOIN relation_members on relations.id=relation_members.relation_id
          WHERE relations.id=?
          ORDER BY relation_members.`order` ASC
          """)
    ) {
      statement.setLong(1, id);
      var result = statement.executeQuery();
      return result.next() && result.getLong("id") == id ? parseRelation(result) : null;
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public LongArrayList getParentRelationsForNode(long nodeId) {
    return getParentRelations(nodeId, OsmElement.Type.NODE);
  }

  private LongArrayList getParentRelations(long nodeId, OsmElement.Type type) {
    try (
      var statement = connection.prepareStatement(
        """
          SELECT relation_id
          FROM relation_members
          WHERE ref=? AND type=?
          ORDER BY relation_id ASC
          """)
    ) {
      statement.setLong(1, nodeId);
      statement.setInt(2, type.ordinal());
      var result = statement.executeQuery();
      return parseLongList(result);
    } catch (SQLException e) {
      throw new IllegalStateException(e);
    }
  }

  @Override
  public LongArrayList getParentRelationsForWay(long nodeId) {
    return getParentRelations(nodeId, OsmElement.Type.WAY);
  }

  @Override
  public LongArrayList getParentRelationsForRelation(long nodeId) {
    return getParentRelations(nodeId, OsmElement.Type.RELATION);
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
  public CloseableIterator<OsmElement> iterator() {
    try {
      var nodes = connection.prepareStatement("SELECT * FROM nodes ORDER BY id ASC").executeQuery();
      var ways = connection.prepareStatement(
        """
          SELECT ways.*, group_concat(way_members.node_id) nodes
          FROM ways
          JOIN way_members on ways.id=way_members.way_id
          GROUP BY ways.id
          ORDER BY ways.id ASC, way_members.`order` ASC
          """).executeQuery();
      var relations = connection.prepareStatement("""
          SELECT
            relations.*,
            group_concat(relation_members.role) roles,
            group_concat(relation_members.type) types,
            group_concat(relation_members.ref) refs
          FROM relations
          JOIN relation_members on relations.id=relation_members.relation_id
          GROUP BY relations.id
          ORDER BY relations.id ASC, relation_members.`order` ASC
        """).executeQuery();
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
  public void close() throws IOException {
    try {
      connection.close();
    } catch (SQLException e) {
      throw new IOException(e);
    }
  }

  private record ParentChild<P> (P parent, int idx) {}

  private class Bulk implements BulkWriter {
    private final NodeWriter nodeWriter = new NodeWriter(connection);
    private final WayWriter wayWriter = new WayWriter(connection);
    private final WayMembersWriter wayMemberWriter = new WayMembersWriter(connection);
    private final RelationWriter relationWriter = new RelationWriter(connection);
    private final RelationMembersWriter relationMemberWriter = new RelationMembersWriter(connection);

    @Override
    public void putNode(OsmElement.Node node) {
      nodeWriter.write(node);
    }

    @Override
    public void putWay(OsmElement.Way way) {
      wayWriter.write(way);
      for (int i = 0; i < way.nodes().size(); i++) {
        wayMemberWriter.write(new ParentChild<>(way, i));
      }
    }

    @Override
    public void putRelation(OsmElement.Relation relation) {
      relationWriter.write(relation);
      for (int i = 0; i < relation.members().size(); i++) {
        relationMemberWriter.write(new ParentChild<>(relation, i));
      }
    }

    @Override
    public void close() throws IOException {
      nodeWriter.close();
      wayWriter.close();
      wayMemberWriter.close();
      relationWriter.close();
      relationMemberWriter.close();
    }
  }

  private class NodeWriter extends Mbtiles.BatchedTableWriterBase<OsmElement.Node> {
    NodeWriter(Connection conn) {
      super("nodes", List.of("id", "version", "tags", "location"), INSERT_IGNORE, conn);
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
      super("ways", List.of("id", "version", "tags"), INSERT_IGNORE, conn);
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
      super("relations", List.of("id", "version", "tags"), INSERT_IGNORE, conn);
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
      super("relation_members", List.of("relation_id", "`order`", "type", "version", "role", "ref"), INSERT_IGNORE,
        conn);
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
      super("way_members", List.of("way_id", "`order`", "node_id", "version"), INSERT_IGNORE, conn);
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
