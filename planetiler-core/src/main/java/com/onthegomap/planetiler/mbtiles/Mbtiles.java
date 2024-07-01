package com.onthegomap.planetiler.mbtiles;

import com.carrotsearch.hppc.LongIntHashMap;
import com.fasterxml.jackson.core.type.TypeReference;
import com.onthegomap.planetiler.archive.ReadableTileArchive;
import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileArchiveMetadataDeSer;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.archive.WriteableTileArchive;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.reader.FileFormatException;
import com.onthegomap.planetiler.util.CloseableIterator;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.util.Format;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;

/**
 * Interface into an mbtiles sqlite file containing tiles and metadata about the tileset.
 *
 * @see <a href="https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md">MBTiles Specification</a>
 */
public final class Mbtiles implements WriteableTileArchive, ReadableTileArchive {

  // Options that can be set through "file.mbtiles?compact=true" query parameters
  // or "file.mbtiles" with "--mbtiles-compact=true" command-line flag
  public static final String COMPACT_DB = "compact";
  public static final String SKIP_INDEX_CREATION = "no_index";
  public static final String VACUUM_ANALYZE = "vacuum_analyze";

  public static final String LEGACY_COMPACT_DB = "compact_db";
  public static final String LEGACY_SKIP_INDEX_CREATION = "skip_mbtiles_index_creation";
  public static final String LEGACY_VACUUM_ANALYZE = "optimize_db";


  // https://www.sqlite.org/src/artifact?ci=trunk&filename=magic.txt
  private static final int MBTILES_APPLICATION_ID = 0x4d504258;

  private static final String TILES_TABLE = "tiles";
  private static final String TILES_COL_X = "tile_column";
  private static final String TILES_COL_Y = "tile_row";
  private static final String TILES_COL_Z = "zoom_level";
  private static final String TILES_COL_DATA = "tile_data";

  private static final String TILES_DATA_TABLE = "tiles_data";
  private static final String TILES_DATA_COL_DATA_ID = "tile_data_id";
  private static final String TILES_DATA_COL_DATA = "tile_data";

  private static final String TILES_SHALLOW_TABLE = "tiles_shallow";
  private static final String TILES_SHALLOW_COL_X = TILES_COL_X;
  private static final String TILES_SHALLOW_COL_Y = TILES_COL_Y;
  private static final String TILES_SHALLOW_COL_Z = TILES_COL_Z;
  private static final String TILES_SHALLOW_COL_DATA_ID = TILES_DATA_COL_DATA_ID;

  private static final String METADATA_TABLE = "metadata";
  private static final String METADATA_COL_NAME = "name";
  private static final String METADATA_COL_VALUE = "value";

  private static final Logger LOGGER = LoggerFactory.getLogger(Mbtiles.class);

  // load the sqlite driver
  static {
    try {
      Class.forName("org.sqlite.JDBC");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("JDBC driver not found");
    }
  }

  private final Connection connection;
  private final boolean compactDb;
  private final boolean skipIndexCreation;
  private final boolean vacuumAnalyze;
  private PreparedStatement getTileStatement = null;

  private final LongSupplier bytesWritten;

  private Mbtiles(Connection connection, Arguments arguments, LongSupplier bytesWritten) {
    this.connection = connection;
    this.compactDb = arguments.getBoolean(
      COMPACT_DB + "|" + LEGACY_COMPACT_DB,
      "mbtiles: reduce the DB size by separating and deduping the tile data",
      true
    );
    this.skipIndexCreation = arguments.getBoolean(
      SKIP_INDEX_CREATION + "|" + LEGACY_SKIP_INDEX_CREATION,
      "mbtiles: skip adding index to sqlite DB",
      false
    );
    this.vacuumAnalyze = arguments.getBoolean(
      VACUUM_ANALYZE + "|" + LEGACY_VACUUM_ANALYZE,
      "mbtiles: vacuum analyze sqlite DB after writing",
      false
    );
    this.bytesWritten = bytesWritten;
  }

  /** Returns a new mbtiles file that won't get written to disk. Useful for toy use-cases like unit tests. */
  public static Mbtiles newInMemoryDatabase(boolean compactDb) {
    return newInMemoryDatabase(Arguments.of(COMPACT_DB, compactDb ? "true" : "false"));
  }

  /** Returns an in-memory database with extra mbtiles and pragma options set from {@code options}. */
  public static Mbtiles newInMemoryDatabase(Arguments options) {
    SQLiteConfig config = new SQLiteConfig();
    config.setApplicationId(MBTILES_APPLICATION_ID);
    return new Mbtiles(newConnection("jdbc:sqlite::memory:", config, options), options, () -> 0);
  }

  /** Alias for {@link #newInMemoryDatabase(boolean)} */
  public static Mbtiles newInMemoryDatabase() {
    return newInMemoryDatabase(true);
  }

  /**
   * Returns a new connection to an mbtiles file optimized for fast bulk writes with extra mbtiles and pragma options
   * set from {@code options}.
   */
  public static Mbtiles newWriteToFileDatabase(Path path, Arguments options) {
    Objects.requireNonNull(path);
    SQLiteConfig sqliteConfig = new SQLiteConfig();
    sqliteConfig.setJournalMode(SQLiteConfig.JournalMode.OFF);
    sqliteConfig.setSynchronous(SQLiteConfig.SynchronousMode.OFF);
    sqliteConfig.setCacheSize(1_000_000); // 1GB
    sqliteConfig.setLockingMode(SQLiteConfig.LockingMode.EXCLUSIVE);
    sqliteConfig.setTempStore(SQLiteConfig.TempStore.MEMORY);
    sqliteConfig.setApplicationId(MBTILES_APPLICATION_ID);
    var connection = newConnection("jdbc:sqlite:" + path.toAbsolutePath(), sqliteConfig, options);
    return new Mbtiles(connection, options, () -> FileUtils.size(path));
  }

  /** Returns a new connection to an mbtiles file optimized for reads. */
  public static Mbtiles newReadOnlyDatabase(Path path) {
    return newReadOnlyDatabase(path, Arguments.of());
  }

  /**
   * Returns a new connection to an mbtiles file optimized for reads with extra mbtiles and pragma options set from
   * {@code options}.
   */
  public static Mbtiles newReadOnlyDatabase(Path path, Arguments options) {
    Objects.requireNonNull(path);
    SQLiteConfig config = new SQLiteConfig();
    config.setReadOnly(true);
    config.setCacheSize(100_000);
    config.setLockingMode(SQLiteConfig.LockingMode.EXCLUSIVE);
    config.setPageSize(32_768);
    // helps with 3 or more threads concurrently accessing:
    // config.setOpenMode(SQLiteOpenMode.NOMUTEX);
    Connection connection = newConnection("jdbc:sqlite:" + path.toAbsolutePath(), config, options);
    return new Mbtiles(connection, options, () -> 0);
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

  private static TileCoord getResultCoord(ResultSet rs) throws SQLException {
    int z = rs.getInt(TILES_COL_Z);
    int rawy = rs.getInt(TILES_COL_Y);
    int x = rs.getInt(TILES_COL_X);
    return TileCoord.ofXYZ(x, (1 << z) - 1 - rawy, z);
  }

  @Override
  public boolean deduplicates() {
    return compactDb;
  }

  @Override
  public TileOrder tileOrder() {
    return TileOrder.TMS;
  }

  @Override
  public void initialize() {
    if (skipIndexCreation) {
      createTablesWithoutIndexes();
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("Skipping index creation. Add later by executing: {}",
          String.join(" ; ", getManualIndexCreationStatements()));
      }
    } else {
      createTablesWithIndexes();
    }
  }

  @Override
  public void finish(TileArchiveMetadata tileArchiveMetadata) {
    metadataTable().set(tileArchiveMetadata);
    if (vacuumAnalyze) {
      vacuumAnalyze();
    }
  }

  @Override
  public long bytesWritten() {
    return bytesWritten.getAsLong();
  }

  @Override
  public void close() throws IOException {
    try {
      connection.close();
    } catch (SQLException throwables) {
      throw new IOException(throwables);
    }
  }

  private Mbtiles execute(Collection<String> queries) {
    for (String query : queries) {
      try (var statement = connection.createStatement()) {
        LOGGER.debug("Execute mbtiles: {}", query);
        statement.execute(query);
      } catch (SQLException throwables) {
        throw new IllegalStateException("Error executing queries " + String.join(",", queries), throwables);
      }
    }
    return this;
  }

  private Mbtiles execute(String... queries) {
    return execute(Arrays.asList(queries));
  }

  /**
   * Creates the required tables (and views) but skips index creation on some tables. Those indexes should be added
   * later manually as described in {@code #getManualIndexCreationStatements()}.
   */
  public Mbtiles createTablesWithoutIndexes() {
    return createTables(true);
  }

  /** Creates the required tables (and views) including all indexes. */
  public Mbtiles createTablesWithIndexes() {
    return createTables(false);
  }

  private Mbtiles createTables(boolean skipIndexCreation) {

    List<String> ddlStatements = new ArrayList<>();

    ddlStatements
      .add("create table " + METADATA_TABLE + " (" + METADATA_COL_NAME + " text, " + METADATA_COL_VALUE + " text);");
    ddlStatements
      .add("create unique index name on " + METADATA_TABLE + " (" + METADATA_COL_NAME + ");");

    if (compactDb) {
      /*
       * "primary key without rowid" results in a clustered index which is much more compact and performant (r/w)
       * than "unique" which results in a non-clustered index
       */
      String tilesShallowPrimaryKeyAddition = skipIndexCreation ? "" : """
        , primary key(%s,%s,%s)
        """.formatted(TILES_SHALLOW_COL_Z, TILES_SHALLOW_COL_X, TILES_SHALLOW_COL_Y);
      ddlStatements
        .add("""
          create table %s (
            %s integer,
            %s integer,
            %s integer,
            %s integer

            %s
          ) %s
          """.formatted(TILES_SHALLOW_TABLE,
          TILES_SHALLOW_COL_Z, TILES_SHALLOW_COL_X, TILES_SHALLOW_COL_Y, TILES_SHALLOW_COL_DATA_ID,
          tilesShallowPrimaryKeyAddition,
          skipIndexCreation ? "" : "without rowid"));
      // here it's not worth to skip the "primary key"/index - doing so even hurts write performance
      ddlStatements.add("""
        create table %s (
          %s integer primary key,
          %s blob
        )
        """.formatted(TILES_DATA_TABLE, TILES_DATA_COL_DATA_ID, TILES_DATA_COL_DATA));
      ddlStatements.add("""
        create view %s AS
        select
          %s.%s as %s,
          %s.%s as %s,
          %s.%s as %s,
          %s.%s as %s
        from %s
        join %s on %s.%s = %s.%s
        """.formatted(
        TILES_TABLE,
        TILES_SHALLOW_TABLE, TILES_SHALLOW_COL_Z, TILES_COL_Z,
        TILES_SHALLOW_TABLE, TILES_SHALLOW_COL_X, TILES_COL_X,
        TILES_SHALLOW_TABLE, TILES_SHALLOW_COL_Y, TILES_COL_Y,
        TILES_DATA_TABLE, TILES_DATA_COL_DATA, TILES_COL_DATA,
        TILES_SHALLOW_TABLE,
        TILES_DATA_TABLE, TILES_SHALLOW_TABLE, TILES_SHALLOW_COL_DATA_ID, TILES_DATA_TABLE, TILES_DATA_COL_DATA_ID
      ));
    } else {
      // here "primary key (with rowid)" is much more compact than a "primary key without rowid" because the tile data is part of the table
      String tilesUniqueAddition = skipIndexCreation ? "" : """
        , primary key(%s,%s,%s)
        """.formatted(TILES_COL_Z, TILES_COL_X, TILES_COL_Y);
      ddlStatements.add("""
        create table %s (
          %s integer,
          %s integer,
          %s integer,
          %s blob
          %s
        )
        """.formatted(TILES_TABLE, TILES_COL_Z, TILES_COL_X, TILES_COL_Y, TILES_COL_DATA, tilesUniqueAddition));
    }

    return execute(ddlStatements);
  }

  /** Returns the DDL statements to create the indexes manually when the option to skip index creation was chosen. */
  public List<String> getManualIndexCreationStatements() {
    if (compactDb) {
      return List.of(
        "create unique index tiles_shallow_index on %s (%s, %s, %s)"
          .formatted(TILES_SHALLOW_TABLE, TILES_SHALLOW_COL_Z, TILES_SHALLOW_COL_X, TILES_SHALLOW_COL_Y)
      );
    } else {
      return List.of(
        "create unique index tile_index on %s (%s, %s, %s)"
          .formatted(TILES_TABLE, TILES_COL_Z, TILES_COL_X, TILES_COL_Y)
      );
    }
  }

  public Mbtiles vacuumAnalyze() {
    return execute(
      "VACUUM;",
      "ANALYZE;"
    );
  }

  /** Returns a writer that queues up inserts into the tile database(s) into large batches before executing them. */
  public WriteableTileArchive.TileWriter newTileWriter() {
    if (compactDb) {
      return new BatchedCompactTileWriter();
    } else {
      return new BatchedNonCompactTileWriter();
    }
  }

  // TODO: exists for compatibility purposes
  public WriteableTileArchive.TileWriter newBatchedTileWriter() {
    return newTileWriter();
  }

  @Override
  public TileArchiveMetadata metadata() {
    return new Metadata().get();
  }

  /** Returns the contents of the metadata table. */
  public Metadata metadataTable() {
    return new Metadata();
  }

  private PreparedStatement getTileStatement() {
    if (getTileStatement == null) {
      try {
        getTileStatement = connection.prepareStatement("""
          SELECT tile_data FROM %s
          WHERE %s=? AND %s=? AND %s=?
          """.formatted(TILES_TABLE, TILES_COL_X, TILES_COL_Y, TILES_COL_Z));
      } catch (SQLException throwables) {
        throw new IllegalStateException(throwables);
      }
    }
    return getTileStatement;
  }

  @Override
  public byte[] getTile(int x, int y, int z) {
    try {
      PreparedStatement stmt = getTileStatement();
      stmt.setInt(1, x);
      stmt.setInt(2, (1 << z) - 1 - y);
      stmt.setInt(3, z);
      try (ResultSet rs = stmt.executeQuery()) {
        return rs.next() ? rs.getBytes(TILES_COL_DATA) : null;
      }
    } catch (SQLException throwables) {
      throw new IllegalStateException("Could not get tile", throwables);
    }
  }

  @Override
  public CloseableIterator<TileCoord> getAllTileCoords() {
    return new QueryIterator<>(
      statement -> statement.executeQuery(
        "select %s, %s, %s from %s".formatted(TILES_COL_Z, TILES_COL_X, TILES_COL_Y, TILES_TABLE)
      ),
      Mbtiles::getResultCoord
    );
  }

  @Override
  public CloseableIterator<Tile> getAllTiles() {
    return new QueryIterator<>(
      statement -> statement.executeQuery(
        "select %s, %s, %s, %s from %s".formatted(TILES_COL_Z, TILES_COL_X, TILES_COL_Y, TILES_COL_DATA, TILES_TABLE)
      ),
      rs -> new Tile(getResultCoord(rs), rs.getBytes(TILES_COL_DATA))
    );
  }

  public Connection connection() {
    return connection;
  }

  public boolean skipIndexCreation() {
    return skipIndexCreation;
  }

  public boolean compactDb() {
    return compactDb;
  }

  @FunctionalInterface
  private interface SqlFunction<I, O> {
    O apply(I t) throws SQLException;
  }

  /**
   * Data contained in the {@code json} row of the metadata table
   *
   * @see <a href="https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#vector-tileset-metadata">MBtiles
   *      schema</a>
   */

     /** Contents of a row of the tiles_shallow table. */
  private record TileShallowEntry(TileCoord coord, int tileDataId) {}

  /** Contents of a row of the tiles_data table. */
  private record TileDataEntry(int tileDataId, byte[] tileData) {

    @Override
    public String toString() {
      return "TileDataEntry [tileDataId=" + tileDataId + ", tileData=" + Arrays.toString(tileData) + "]";
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + Arrays.hashCode(tileData);
      result = prime * result + Objects.hash(tileDataId);
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (!(obj instanceof TileDataEntry other)) {
        return false;
      }
      return Arrays.equals(tileData, other.tileData) && tileDataId == other.tileDataId;
    }
  }

  /** Iterates through the results of a query one at a time without materializing the entire list in memory. */
  private class QueryIterator<T> implements CloseableIterator<T> {
    private final Statement statement;
    private final ResultSet rs;
    private final SqlFunction<ResultSet, T> rowMapper;
    private boolean hasNext = false;

    private QueryIterator(
      SqlFunction<Statement, ResultSet> query,
      SqlFunction<ResultSet, T> rowMapper
    ) {
      this.rowMapper = rowMapper;
      try {
        this.statement = connection.createStatement();
        this.rs = query.apply(statement);
        hasNext = rs.next();
      } catch (SQLException e) {
        throw new FileFormatException("Could not read tile coordinates from mbtiles file", e);
      } finally {
        if (!hasNext) {
          close();
        }
      }
    }

    @Override
    public void close() {
      try {
        statement.close();
      } catch (SQLException e) {
        throw new IllegalStateException("Could not close mbtiles file", e);
      }
    }

    @Override
    public boolean hasNext() {
      return hasNext;
    }

    @Override
    public T next() {
      if (!hasNext()) {
        throw new NoSuchElementException();
      }
      try {
        T result = rowMapper.apply(rs);
        hasNext = rs.next();
        if (!hasNext) {
          close();
        }
        return result;
      } catch (SQLException e) {
        throw new IllegalStateException("Could not read mbtiles file", e);
      }
    }
  }

  private abstract class BatchedTableWriterBase<T> implements AutoCloseable {

    private static final int MAX_PARAMETERS_IN_PREPARED_STATEMENT = 999;
    private final List<T> batch;
    private final PreparedStatement batchStatement;
    private final int batchLimit;
    private final String insertStmtTableName;
    private final boolean insertStmtInsertIgnore;
    private final String insertStmtValuesPlaceHolder;
    private final String insertStmtColumnsCsv;
    private long count = 0;


    protected BatchedTableWriterBase(String tableName, List<String> columns, boolean insertIgnore) {
      batchLimit = MAX_PARAMETERS_IN_PREPARED_STATEMENT / columns.size();
      batch = new ArrayList<>(batchLimit);
      insertStmtTableName = tableName;
      insertStmtInsertIgnore = insertIgnore;
      insertStmtValuesPlaceHolder = columns.stream().map(c -> "?").collect(Collectors.joining(",", "(", ")"));
      insertStmtColumnsCsv = String.join(",", columns);
      batchStatement = createBatchInsertPreparedStatement(batchLimit);
    }

    /** Queue-up a write or flush to disk if enough are waiting. */
    void write(T item) {
      count++;
      batch.add(item);
      if (batch.size() >= batchLimit) {
        flush(batchStatement);
      }
    }

    protected abstract int setParamsInStatementForItem(int positionOffset, PreparedStatement statement, T item)
      throws SQLException;

    private PreparedStatement createBatchInsertPreparedStatement(int size) {

      final String sql = "INSERT %s INTO %s (%s) VALUES %s;".formatted(
        insertStmtInsertIgnore ? "OR IGNORE" : "",
        insertStmtTableName,
        insertStmtColumnsCsv,
        IntStream.range(0, size).mapToObj(i -> insertStmtValuesPlaceHolder).collect(Collectors.joining(", "))
      );

      try {
        return connection.prepareStatement(sql);
      } catch (SQLException throwables) {
        throw new IllegalStateException("Could not create prepared statement", throwables);
      }
    }

    private void flush(PreparedStatement statement) {
      // todo linespace  暂时注释生成mbtiles逻辑
      try {
        int pos = 1;
        for (T item : batch) {
          pos = setParamsInStatementForItem(pos, statement, item);
        }
        statement.execute();
        batch.clear();
      } catch (Exception throwables) {
        throw new IllegalStateException("Error flushing batch", throwables);
      }
    }

    public long count() {
      return count;
    }

    @Override
    public void close() {
      if (!batch.isEmpty()) {
        try (var lastBatch = createBatchInsertPreparedStatement(batch.size())) {
          flush(lastBatch);
        } catch (SQLException throwables) {
          throw new IllegalStateException("Error flushing batch", throwables);
        }
      }
      try {
        batchStatement.close();
      } catch (SQLException throwables) {
        LOGGER.warn("Error closing prepared statement", throwables);
      }
    }

  }


  private class BatchedTileTableWriter extends BatchedTableWriterBase<Tile> {

    private static final List<String> COLUMNS = List.of(TILES_COL_Z, TILES_COL_X, TILES_COL_Y, TILES_COL_DATA);

    BatchedTileTableWriter() {
      super(TILES_TABLE, COLUMNS, false);
    }

    @Override
    protected int setParamsInStatementForItem(int positionOffset, PreparedStatement statement, Tile tile)
      throws SQLException {

      TileCoord coord = tile.coord();
      int x = coord.x();
      int y = coord.y();
      int z = coord.z();
      statement.setInt(positionOffset++, z);
      statement.setInt(positionOffset++, x);
      // flip Y
      statement.setInt(positionOffset++, (1 << z) - 1 - y);
      statement.setBytes(positionOffset++, tile.bytes());
      return positionOffset;
    }
  }

  private class BatchedTileShallowTableWriter extends BatchedTableWriterBase<TileShallowEntry> {

    private static final List<String> COLUMNS =
      List.of(TILES_SHALLOW_COL_Z, TILES_SHALLOW_COL_X, TILES_SHALLOW_COL_Y, TILES_SHALLOW_COL_DATA_ID);

    BatchedTileShallowTableWriter() {
      super(TILES_SHALLOW_TABLE, COLUMNS, false);
    }

    @Override
    protected int setParamsInStatementForItem(int positionOffset, PreparedStatement statement, TileShallowEntry item)
      throws SQLException {

      TileCoord coord = item.coord();
      int x = coord.x();
      int y = coord.y();
      int z = coord.z();
      statement.setInt(positionOffset++, z);
      statement.setInt(positionOffset++, x);
      // flip Y
      statement.setInt(positionOffset++, (1 << z) - 1 - y);
      statement.setInt(positionOffset++, item.tileDataId());

      return positionOffset;
    }
  }

  private class BatchedTileDataTableWriter extends BatchedTableWriterBase<TileDataEntry> {

    private static final List<String> COLUMNS = List.of(TILES_DATA_COL_DATA_ID, TILES_DATA_COL_DATA);

    BatchedTileDataTableWriter() {
      super(TILES_DATA_TABLE, COLUMNS, true);
    }

    @Override
    protected int setParamsInStatementForItem(int positionOffset, PreparedStatement statement, TileDataEntry item)
      throws SQLException {

      statement.setInt(positionOffset++, item.tileDataId());
      statement.setBytes(positionOffset++, item.tileData());

      return positionOffset;
    }
  }

  private class BatchedNonCompactTileWriter implements TileWriter {

    private final BatchedTileTableWriter tableWriter = new BatchedTileTableWriter();

    @Override
    public void write(TileEncodingResult encodingResult) {
      tableWriter.write(new Tile(encodingResult.coord(), encodingResult.tileData()));
    }

    @Override
    public void close() {
      tableWriter.close();
    }

  }

  private class BatchedCompactTileWriter implements TileWriter {

    private final BatchedTileShallowTableWriter batchedTileShallowTableWriter = new BatchedTileShallowTableWriter();
    private final BatchedTileDataTableWriter batchedTileDataTableWriter = new BatchedTileDataTableWriter();
    private final LongIntHashMap tileDataIdByHash = new LongIntHashMap(1_000);

    private int tileDataIdCounter = 1;

    @Override
    public void write(TileEncodingResult encodingResult) {
      int tileDataId;
      boolean writeData;
      OptionalLong tileDataHashOpt = encodingResult.tileDataHash();

      if (tileDataHashOpt.isPresent()) {
        long tileDataHash = tileDataHashOpt.getAsLong();
        if (tileDataIdByHash.containsKey(tileDataHash)) {
          tileDataId = tileDataIdByHash.get(tileDataHash);
          writeData = false;
        } else {
          tileDataId = tileDataIdCounter++;
          tileDataIdByHash.put(tileDataHash, tileDataId);
          writeData = true;
        }
      } else {
        tileDataId = tileDataIdCounter++;
        writeData = true;
      }
      if (writeData) {
        batchedTileDataTableWriter.write(new TileDataEntry(tileDataId, encodingResult.tileData()));
      }
      batchedTileShallowTableWriter.write(new TileShallowEntry(encodingResult.coord(), tileDataId));
    }

    @Override
    public void close() {
      batchedTileShallowTableWriter.close();
      batchedTileDataTableWriter.close();
    }

    @Override
    public void printStats() {
      if (LOGGER.isDebugEnabled()) {
        var format = Format.defaultInstance();
        LOGGER.debug("Shallow tiles written: {}", format.integer(batchedTileShallowTableWriter.count()));
        LOGGER.debug("Tile data written: {} ({} omitted)", format.integer(batchedTileDataTableWriter.count()),
          format.percent(1d - batchedTileDataTableWriter.count() * 1d / batchedTileShallowTableWriter.count()));
        LOGGER.debug("Unique tile hashes: {}", format.integer(tileDataIdByHash.size()));
      }
    }
  }


  /** Data contained in the metadata table. */
  public class Metadata {

    /** Inserts a row into the metadata table that sets {@code name=value}. */
    public Metadata setMetadata(String name, String value) {
      if (value != null) {
        LOGGER.debug("Set mbtiles metadata: {}={}", name,
          value.length() > 1_000 ?
            (value.substring(0, 1_000) + "... " + (value.length() - 1_000) + " more characters") :
            value);
        try (
          PreparedStatement statement = connection.prepareStatement(
            "INSERT INTO " + METADATA_TABLE + " (" + METADATA_COL_NAME + "," + METADATA_COL_VALUE + ") VALUES(?, ?);")
        ) {
          statement.setString(1, name);
          statement.setString(2, value);
          statement.execute();
        } catch (SQLException throwables) {
          LOGGER.error("Error setting metadata " + name + "=" + value, throwables);
        }
      }
      return this;
    }

    /** Returns all key-value pairs from the metadata table. */
    public Map<String, String> getAll() {
      TreeMap<String, String> result = new TreeMap<>();
      try (Statement statement = connection.createStatement()) {
        var resultSet = statement
          .executeQuery("SELECT " + METADATA_COL_NAME + ", " + METADATA_COL_VALUE + " FROM " + METADATA_TABLE);
        while (resultSet.next()) {
          result.put(
            resultSet.getString(METADATA_COL_NAME),
            resultSet.getString(METADATA_COL_VALUE)
          );
        }
      } catch (SQLException throwables) {
        LOGGER.warn("Error retrieving metadata: {}", throwables.toString());
        LOGGER.trace("Error retrieving metadata details: ", throwables);
      }
      return result;
    }

    /**
     * Inserts rows into the metadata table that set all of the well-known metadata keys from
     * {@code tileArchiveMetadata} and passes through the raw values of any options not explicitly called out in the
     * MBTiles specification.
     *
     * @see <a href="https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#content">MBTiles 1.3
     *      specification</a>
     */
    public Metadata set(TileArchiveMetadata tileArchiveMetadata) {
      TileArchiveMetadataDeSer.mbtilesMapper()
        .convertValue(tileArchiveMetadata, new TypeReference<Map<String, String>>() {})
        .forEach(this::setMetadata);
      return this;
    }

    /**
     * Returns a {@link TileArchiveMetadata} instance parsed from all the rows in the metadata table.
     */
    public TileArchiveMetadata get() {
      Map<String, String> map = new HashMap<>(getAll());
      return TileArchiveMetadataDeSer.mbtilesMapper().convertValue(map, TileArchiveMetadata.class);
    }
  }
}
