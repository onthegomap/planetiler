package com.onthegomap.planetiler.mbtiles;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_ABSENT;

import com.carrotsearch.hppc.LongIntHashMap;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.util.LayerStats;
import com.onthegomap.planetiler.writer.TileArchive;
import com.onthegomap.planetiler.writer.TileArchiveMetadata;
import com.onthegomap.planetiler.writer.TileEncodingResult;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;

/**
 * Interface into an mbtiles sqlite file containing tiles and metadata about the tileset.
 *
 * @see <a href="https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md">MBTiles Specification</a>
 */
public final class Mbtiles implements TileArchive {

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
  private static final ObjectMapper objectMapper = new ObjectMapper()
    .registerModules(new Jdk8Module())
    .setSerializationInclusion(NON_ABSENT);

  // load the sqlite driver
  static {
    try {
      Class.forName("org.sqlite.JDBC");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("JDBC driver not found");
    }
  }

  private final Connection connection;
  private PreparedStatement getTileStatement = null;
  private final boolean compactDb;

  /** Inserts will be ordered to match the MBTiles index (TMS) */
  @Override
  public TileOrder tileOrder() {
    return TileOrder.TMS;
  }

  private Mbtiles(Connection connection, boolean compactDb) {
    this.connection = connection;
    this.compactDb = compactDb;
  }

  /** Returns a new mbtiles file that won't get written to disk. Useful for toy use-cases like unit tests. */
  public static Mbtiles newInMemoryDatabase(boolean compactDb) {
    try {
      SQLiteConfig config = new SQLiteConfig();
      config.setApplicationId(MBTILES_APPLICATION_ID);
      return new Mbtiles(DriverManager.getConnection("jdbc:sqlite::memory:", config.toProperties()), compactDb);
    } catch (SQLException throwables) {
      throw new IllegalStateException("Unable to create in-memory database", throwables);
    }
  }

  /** @see {@link #newInMemoryDatabase(boolean)} */
  public static Mbtiles newInMemoryDatabase() {
    return newInMemoryDatabase(true);
  }

  /** Returns a new connection to an mbtiles file optimized for fast bulk writes. */
  public static Mbtiles newWriteToFileDatabase(Path path, boolean compactDb) {
    try {
      SQLiteConfig config = new SQLiteConfig();
      config.setJournalMode(SQLiteConfig.JournalMode.OFF);
      config.setSynchronous(SQLiteConfig.SynchronousMode.OFF);
      config.setCacheSize(1_000_000); // 1GB
      config.setLockingMode(SQLiteConfig.LockingMode.EXCLUSIVE);
      config.setTempStore(SQLiteConfig.TempStore.MEMORY);
      config.setApplicationId(MBTILES_APPLICATION_ID);
      return new Mbtiles(DriverManager.getConnection("jdbc:sqlite:" + path.toAbsolutePath(), config.toProperties()),
        compactDb);
    } catch (SQLException throwables) {
      throw new IllegalArgumentException("Unable to open " + path, throwables);
    }
  }

  /** Returns a new connection to an mbtiles file optimized for reads. */
  public static Mbtiles newReadOnlyDatabase(Path path) {
    try {
      SQLiteConfig config = new SQLiteConfig();
      config.setReadOnly(true);
      config.setCacheSize(100_000);
      config.setLockingMode(SQLiteConfig.LockingMode.EXCLUSIVE);
      config.setPageSize(32_768);
      // helps with 3 or more threads concurrently accessing:
      // config.setOpenMode(SQLiteOpenMode.NOMUTEX);
      Connection connection = DriverManager
        .getConnection("jdbc:sqlite:" + path.toAbsolutePath(), config.toProperties());
      return new Mbtiles(connection, false /* in read-only mode, it's irrelevant if compact or not */);
    } catch (SQLException throwables) {
      throw new IllegalArgumentException("Unable to open " + path, throwables);
    }
  }

  @Override
  public void initialize(PlanetilerConfig config, TileArchiveMetadata tileArchiveMetadata, LayerStats layerStats) {
    if (config.skipIndexCreation()) {
      createTablesWithoutIndexes();
      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("Skipping index creation. Add later by executing: {}",
          String.join(" ; ", getManualIndexCreationStatements()));
      }
    } else {
      createTablesWithIndexes();
    }

    var metadata = metadata()
      .setName(tileArchiveMetadata.name())
      .setFormat("pbf")
      .setDescription(tileArchiveMetadata.description())
      .setAttribution(tileArchiveMetadata.attribution())
      .setVersion(tileArchiveMetadata.version())
      .setType(tileArchiveMetadata.type())
      .setBoundsAndCenter(config.bounds().latLon())
      .setMinzoom(config.minzoom())
      .setMaxzoom(config.maxzoom())
      .setJson(layerStats.getTileStats());

    for (var entry : tileArchiveMetadata.planetilerSpecific().entrySet()) {
      metadata.setMetadata(entry.getKey(), entry.getValue());
    }
  }

  @Override
  public void finish(PlanetilerConfig config) {
    if (config.optimizeDb()) {
      vacuumAnalyze();
    }
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
  public TileArchive.TileWriter newTileWriter() {
    if (compactDb) {
      return new BatchedCompactTileWriter();
    } else {
      return new BatchedNonCompactTileWriter();
    }
  }

  // TODO: exists for compatibility purposes
  public TileArchive.TileWriter newBatchedTileWriter() {
    return newTileWriter();
  }

  /** Returns the contents of the metadata table. */
  public Metadata metadata() {
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

  public byte[] getTile(TileCoord coord) {
    return getTile(coord.x(), coord.y(), coord.z());
  }

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

  public List<TileCoord> getAllTileCoords() {
    List<TileCoord> result = new ArrayList<>();
    try (Statement statement = connection.createStatement()) {
      ResultSet rs = statement.executeQuery(
        "select %s, %s, %s, %s from %s".formatted(TILES_COL_Z, TILES_COL_X, TILES_COL_Y, TILES_COL_DATA, TILES_TABLE)
      );
      while (rs.next()) {
        int z = rs.getInt(TILES_COL_Z);
        int rawy = rs.getInt(TILES_COL_Y);
        int x = rs.getInt(TILES_COL_X);
        result.add(TileCoord.ofXYZ(x, (1 << z) - 1 - rawy, z));
      }
    } catch (SQLException throwables) {
      throw new IllegalStateException("Could not get all tile coordinates", throwables);
    }
    return result;
  }

  public Connection connection() {
    return connection;
  }

  /**
   * Data contained in the {@code json} row of the metadata table
   *
   * @see <a href="https://github.com/mapbox/mbtiles-spec/blob/master/1.3/spec.md#vector-tileset-metadata">MBtiles
   *      schema</a>
   */
  public record MetadataJson(
    @JsonProperty("vector_layers") List<VectorLayer> vectorLayers
  ) {

    public MetadataJson(VectorLayer... layers) {
      this(List.of(layers));
    }

    public static MetadataJson fromJson(String json) {
      try {
        return objectMapper.readValue(json, MetadataJson.class);
      } catch (JsonProcessingException e) {
        throw new IllegalStateException("Invalid metadata json: " + json, e);
      }
    }

    public String toJson() {
      try {
        return objectMapper.writeValueAsString(this);
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Unable to encode as string: " + this, e);
      }
    }

    public enum FieldType {
      @JsonProperty("Number")
      NUMBER,
      @JsonProperty("Boolean")
      BOOLEAN,
      @JsonProperty("String")
      STRING;

      /**
       * Per the spec: attributes whose type varies between features SHOULD be listed as "String"
       */
      public static FieldType merge(FieldType oldValue, FieldType newValue) {
        return oldValue != newValue ? STRING : newValue;
      }
    }

    public record VectorLayer(
      @JsonProperty("id") String id,
      @JsonProperty("fields") Map<String, FieldType> fields,
      @JsonProperty("description") Optional<String> description,
      @JsonProperty("minzoom") OptionalInt minzoom,
      @JsonProperty("maxzoom") OptionalInt maxzoom
    ) {

      public VectorLayer(String id, Map<String, FieldType> fields) {
        this(id, fields, Optional.empty(), OptionalInt.empty(), OptionalInt.empty());
      }

      public VectorLayer(String id, Map<String, FieldType> fields, int minzoom, int maxzoom) {
        this(id, fields, Optional.empty(), OptionalInt.of(minzoom), OptionalInt.of(maxzoom));
      }

      public static VectorLayer forLayer(String id) {
        return new VectorLayer(id, new HashMap<>());
      }

      public VectorLayer withDescription(String newDescription) {
        return new VectorLayer(id, fields, Optional.of(newDescription), minzoom, maxzoom);
      }

      public VectorLayer withMinzoom(int newMinzoom) {
        return new VectorLayer(id, fields, description, OptionalInt.of(newMinzoom), maxzoom);
      }

      public VectorLayer withMaxzoom(int newMaxzoom) {
        return new VectorLayer(id, fields, description, minzoom, OptionalInt.of(newMaxzoom));
      }
    }
  }

  /** Contents of a row of the tiles table, or in case of compact mode in the tiles view. */
  public record TileEntry(TileCoord tile, byte[] bytes) implements Comparable<TileEntry> {

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      TileEntry tileEntry = (TileEntry) o;

      if (!tile.equals(tileEntry.tile)) {
        return false;
      }
      return Arrays.equals(bytes, tileEntry.bytes);
    }

    @Override
    public int hashCode() {
      int result = tile.hashCode();
      result = 31 * result + Arrays.hashCode(bytes);
      return result;
    }

    @Override
    public String toString() {
      return "TileEntry{" +
        "tile=" + tile +
        ", bytes=" + Arrays.toString(bytes) +
        '}';
    }

    @Override
    public int compareTo(TileEntry o) {
      return tile.compareTo(o.tile);
    }
  }

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
      if (!(obj instanceof TileDataEntry)) {
        return false;
      }
      TileDataEntry other = (TileDataEntry) obj;
      return Arrays.equals(tileData, other.tileData) && tileDataId == other.tileDataId;
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
      insertStmtColumnsCsv = columns.stream().collect(Collectors.joining(","));
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
      try {
        int pos = 1;
        for (T item : batch) {
          pos = setParamsInStatementForItem(pos, statement, item);
        }
        statement.execute();
        batch.clear();
      } catch (SQLException throwables) {
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


  private class BatchedTileTableWriter extends BatchedTableWriterBase<TileEntry> {

    private static final List<String> COLUMNS = List.of(TILES_COL_Z, TILES_COL_X, TILES_COL_Y, TILES_COL_DATA);

    BatchedTileTableWriter() {
      super(TILES_TABLE, COLUMNS, false);
    }

    @Override
    protected int setParamsInStatementForItem(int positionOffset, PreparedStatement statement, TileEntry tile)
      throws SQLException {

      TileCoord coord = tile.tile();
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
      tableWriter.write(new TileEntry(encodingResult.coord(), encodingResult.tileData()));
    }

    // TODO: exists for compatibility purposes
    @Override
    public void write(com.onthegomap.planetiler.mbtiles.TileEncodingResult encodingResult) {
      tableWriter.write(new TileEntry(encodingResult.coord(), encodingResult.tileData()));
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

    // TODO: exists for compatibility purposes
    @Override
    public void write(com.onthegomap.planetiler.mbtiles.TileEncodingResult encodingResult) {
      write(new TileEncodingResult(encodingResult.coord(), encodingResult.tileData(), encodingResult.tileDataHash()));
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

    private static final NumberFormat nf = NumberFormat.getNumberInstance(Locale.US);

    static {
      nf.setMaximumFractionDigits(5);
    }

    private static String join(double... items) {
      return DoubleStream.of(items).mapToObj(nf::format).collect(Collectors.joining(","));
    }

    public Metadata setMetadata(String name, Object value) {
      if (value != null) {
        String stringValue = value.toString();
        LOGGER.debug("Set mbtiles metadata: {}={}", name,
          stringValue.length() > 1_000 ?
            (stringValue.substring(0, 1_000) + "... " + (stringValue.length() - 1_000) + " more characters") :
            stringValue);
        try (
          PreparedStatement statement = connection.prepareStatement(
            "INSERT INTO " + METADATA_TABLE + " (" + METADATA_COL_NAME + "," + METADATA_COL_VALUE + ") VALUES(?, ?);")
        ) {
          statement.setString(1, name);
          statement.setString(2, stringValue);
          statement.execute();
        } catch (SQLException throwables) {
          LOGGER.error("Error setting metadata " + name + "=" + value, throwables);
        }
      }
      return this;
    }

    public Metadata setName(String value) {
      return setMetadata("name", value);
    }

    /** Format of the tile data, should always be pbf {@code pbf}. */
    public Metadata setFormat(String format) {
      return setMetadata("format", format);
    }

    public Metadata setBounds(double left, double bottom, double right, double top) {
      return setMetadata("bounds", join(left, bottom, right, top));
    }

    public Metadata setBounds(Envelope envelope) {
      return setBounds(envelope.getMinX(), envelope.getMinY(), envelope.getMaxX(), envelope.getMaxY());
    }

    public Metadata setCenter(double longitude, double latitude, double zoom) {
      return setMetadata("center", join(longitude, latitude, zoom));
    }

    public Metadata setBoundsAndCenter(Envelope envelope) {
      return setBounds(envelope).setCenter(envelope);
    }

    /** Estimate a reasonable center for the map to fit an envelope. */
    public Metadata setCenter(Envelope envelope) {
      Coordinate center = envelope.centre();
      double zoom = Math.ceil(GeoUtils.getZoomFromLonLatBounds(envelope));
      return setCenter(center.x, center.y, zoom);
    }

    public Metadata setMinzoom(int value) {
      return setMetadata("minzoom", value);
    }

    public Metadata setMaxzoom(int maxZoom) {
      return setMetadata("maxzoom", maxZoom);
    }

    public Metadata setAttribution(String value) {
      return setMetadata("attribution", value);
    }

    public Metadata setDescription(String value) {
      return setMetadata("description", value);
    }

    /** {@code overlay} or {@code baselayer}. */
    public Metadata setType(String value) {
      return setMetadata("type", value);
    }

    public Metadata setTypeIsOverlay() {
      return setType("overlay");
    }

    public Metadata setTypeIsBaselayer() {
      return setType("baselayer");
    }

    public Metadata setVersion(String value) {
      return setMetadata("version", value);
    }

    public Metadata setJson(String value) {
      return setMetadata("json", value);
    }

    public Metadata setJson(MetadataJson value) {
      return value == null ? this : setJson(value.toJson());
    }

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
        LOGGER.warn("Error retrieving metadata: " + throwables);
        LOGGER.trace("Error retrieving metadata details: ", throwables);
      }
      return result;
    }
  }
}
