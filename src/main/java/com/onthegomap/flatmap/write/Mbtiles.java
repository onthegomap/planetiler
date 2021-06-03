package com.onthegomap.flatmap.write;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_ABSENT;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.TileCoord;
import java.io.Closeable;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import org.jetbrains.annotations.NotNull;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.sqlite.SQLiteConfig;

public final class Mbtiles implements Closeable {

  public static final String TILES_TABLE = "tiles";
  public static final String TILES_COL_X = "tile_column";
  public static final String TILES_COL_Y = "tile_row";
  public static final String TILES_COL_Z = "zoom_level";
  public static final String TILES_COL_DATA = "tile_data";

  public static final String METADATA_TABLE = "metadata";
  public static final String METADATA_COL_NAME = "name";
  public static final String METADATA_COL_VALUE = "value";

  private static final Logger LOGGER = LoggerFactory.getLogger(Mbtiles.class);
  private static final ObjectMapper objectMapper = new ObjectMapper()
    .registerModules(new Jdk8Module())
    .setSerializationInclusion(NON_ABSENT);
  private final Connection connection;

  public Mbtiles(Connection connection) {
    this.connection = connection;
  }

  static {
    try {
      Class.forName("org.sqlite.JDBC");
    } catch (ClassNotFoundException e) {
      throw new IllegalStateException("JDBC driver not found");
    }
  }

  public static Mbtiles newInMemoryDatabase() {
    try {
      return new Mbtiles(DriverManager.getConnection("jdbc:sqlite::memory:")).init();

    } catch (SQLException throwables) {
      throw new IllegalStateException("Unable to create in-memory database", throwables);
    }
  }

  public static Mbtiles newFileDatabase(Path path) {
    try {
      return new Mbtiles(DriverManager.getConnection("jdbc:sqlite:" + path.toAbsolutePath())).init();
    } catch (SQLException throwables) {
      throw new IllegalArgumentException("Unable to open " + path, throwables);
    }
  }

  public static Mbtiles newReadOnlyDatabase(Path path) {
    try {
      SQLiteConfig config = new SQLiteConfig();
      config.setReadOnly(true);
      Connection connection = DriverManager
        .getConnection("jdbc:sqlite:" + path.toAbsolutePath(), config.toProperties());
      return new Mbtiles(connection);
    } catch (SQLException throwables) {
      throw new IllegalArgumentException("Unable to open " + path, throwables);
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

  private String pragma(SQLiteConfig.Pragma pragma, Object value) {
    return "PRAGMA " + pragma.pragmaName + " = " + value + ";";
  }

  private Mbtiles init() {
    // https://www.sqlite.org/src/artifact?ci=trunk&filename=magic.txt
    return execute(pragma(SQLiteConfig.Pragma.APPLICATION_ID, "0x4d504258"));
  }

  private Mbtiles execute(String... queries) {
    for (String query : queries) {
      try (var statement = connection.createStatement()) {
        LOGGER.info("Execute mbtiles: " + query);
        statement.execute(query);
      } catch (SQLException throwables) {
        throw new IllegalStateException("Error executing queries " + Arrays.toString(queries), throwables);
      }
    }
    return this;
  }

  public Mbtiles addIndex() {
    return execute(
      "create unique index tile_index on " + TILES_TABLE
        + " ("
        + TILES_COL_Z + ", " + TILES_COL_X + ", " + TILES_COL_Y
        + ");"
    );
  }

  public Mbtiles setupSchema() {
    return execute(
      "create table " + METADATA_TABLE + " (" + METADATA_COL_NAME + " text, " + METADATA_COL_VALUE + " text);",
      "create unique index name on " + METADATA_TABLE + " (" + METADATA_COL_NAME + ");",
      "create table " + TILES_TABLE + " (" + TILES_COL_Z + " integer, " + TILES_COL_X + " integer, " + TILES_COL_Y
        + ", " + TILES_COL_DATA + " blob);"
    );
  }

  public Mbtiles tuneForWrites() {
    return execute(
      pragma(SQLiteConfig.Pragma.SYNCHRONOUS, SQLiteConfig.SynchronousMode.OFF),
      pragma(SQLiteConfig.Pragma.JOURNAL_MODE, SQLiteConfig.JournalMode.OFF),
      pragma(SQLiteConfig.Pragma.LOCKING_MODE, SQLiteConfig.LockingMode.EXCLUSIVE),
      pragma(SQLiteConfig.Pragma.PAGE_SIZE, 8192),
      pragma(SQLiteConfig.Pragma.MMAP_SIZE, 30000000000L)
    );
  }

  public Mbtiles vacuumAnalyze() {
    return execute(
      "VACUUM;",
      "ANALYZE;"
    );
  }

  public BatchedTileWriter newBatchedTileWriter() {
    return new BatchedTileWriter();
  }

  public Metadata metadata() {
    return new Metadata();
  }

  private PreparedStatement newGetTileStatement() {
    try {
      return connection.prepareStatement("""
        SELECT tile_data FROM %s
        WHERE tile_column=?
        AND tile_row=?
        AND zoom_level=?
        """.formatted(TILES_TABLE));
    } catch (SQLException throwables) {
      throw new IllegalStateException(throwables);
    }
  }

  private ThreadLocal<PreparedStatement> getTileStatementCache = ThreadLocal.withInitial(this::newGetTileStatement);

  public byte[] getTile(int x, int y, int z) {
    try {
      PreparedStatement stmt = getTileStatementCache.get();
      stmt.setInt(1, x);
      stmt.setInt(2, (1 << z) - 1 - y);
      stmt.setInt(3, z);
      try (ResultSet rs = stmt.executeQuery()) {
        return rs.next() ? rs.getBytes("tile_data") : null;
      }
    } catch (SQLException throwables) {
      throw new IllegalStateException("Could not get tile", throwables);
    }
  }

  public Connection connection() {
    return connection;
  }

  public static record MetadataJson(
    @JsonProperty("vector_layers")
    List<VectorLayer> vectorLayers
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
      @JsonProperty("Number") NUMBER,
      @JsonProperty("Boolean") BOOLEAN,
      @JsonProperty("String") STRING;

      public static FieldType merge(FieldType oldValue, FieldType newValue) {
        return oldValue != newValue ? STRING : newValue;
      }
    }

    public static record VectorLayer(
      @JsonProperty("id") String id,
      @JsonProperty("fields") Map<String, FieldType> fields,
      @JsonProperty("description") Optional<String> description,
      @JsonProperty("minzoom") OptionalInt minzoom,
      @JsonProperty("maxzoom") OptionalInt maxzoom
    ) {

      public static VectorLayer forLayer(String id) {
        return new VectorLayer(id, new HashMap<>());
      }

      public VectorLayer(String id, Map<String, FieldType> fields) {
        this(id, fields, Optional.empty(), OptionalInt.empty(), OptionalInt.empty());
      }

      public VectorLayer(String id, Map<String, FieldType> fields, int minzoom, int maxzoom) {
        this(id, fields, Optional.empty(), OptionalInt.of(minzoom), OptionalInt.of(maxzoom));
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

  public class BatchedTileWriter implements AutoCloseable {

    private final List<TileEntry> batch;
    private final PreparedStatement batchStatement;
    private final int batchLimit;

    private BatchedTileWriter() {
      batchLimit = 999 / 4;
      batch = new ArrayList<>(batchLimit);
      batchStatement = createBatchStatement(batchLimit);
    }

    private PreparedStatement createBatchStatement(int size) {
      List<String> groups = new ArrayList<>();
      for (int i = 0; i < size; i++) {
        groups.add("(?,?,?,?)");
      }
      try {
        return connection.prepareStatement(
          "INSERT INTO " + TILES_TABLE + " (" + TILES_COL_Z + "," + TILES_COL_X + "," + TILES_COL_Y + ","
            + TILES_COL_DATA
            + ") VALUES " + String.join(", ", groups) + ";");
      } catch (SQLException throwables) {
        throw new IllegalStateException("Could not create prepared statement", throwables);
      }
    }

    public void write(TileCoord tile, byte[] data) {
      batch.add(new TileEntry(tile, data));
      if (batch.size() >= batchLimit) {
        flush(batchStatement);
      }
    }

    private void flush(PreparedStatement statement) {
      try {
        int pos = 1;
        for (TileEntry tile : batch) {
          TileCoord coord = tile.tile();
          int x = coord.x();
          int y = coord.y();
          int z = coord.z();
          statement.setInt(pos++, z);
          statement.setInt(pos++, x);
          // flip Y
          statement.setInt(pos++, (1 << z) - 1 - y);
          statement.setBytes(pos++, tile.bytes());
        }
        statement.execute();
        batch.clear();
      } catch (SQLException throwables) {
        throw new IllegalStateException("Error flushing batch", throwables);
      }
    }

    @Override
    public void close() {
      try {
        if (batch.size() > 0) {
          try (var lastBatch = createBatchStatement(batch.size())) {
            flush(lastBatch);
          }
        }
        batchStatement.close();
      } catch (SQLException throwables) {
        LOGGER.warn("Error closing prepared statement", throwables);
      }
    }
  }

  public class Metadata {

    private static final NumberFormat nf = NumberFormat.getNumberInstance();

    static {
      nf.setMaximumFractionDigits(5);
    }

    private static String join(double... items) {
      return DoubleStream.of(items).mapToObj(nf::format).collect(Collectors.joining(","));
    }

    public Metadata setMetadata(String name, Object value) {
      if (value != null) {
        LOGGER.info("Set mbtiles metadata: " + name + "=" + value);
        try (PreparedStatement statement = connection.prepareStatement(
          "INSERT INTO " + METADATA_TABLE + " (" + METADATA_COL_NAME + "," + METADATA_COL_VALUE + ") VALUES(?, ?);")) {
          statement.setString(1, name);
          statement.setString(2, value.toString());
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
        LOGGER.warn("Error retrieving metadata", throwables);
      }
      return result;
    }
  }

  public static record TileEntry(TileCoord tile, byte[] bytes) implements Comparable<TileEntry> {

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
    public int compareTo(@NotNull TileEntry o) {
      return tile.compareTo(o.tile);
    }
  }
}
