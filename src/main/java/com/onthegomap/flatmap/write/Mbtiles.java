package com.onthegomap.flatmap.write;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.TileCoord;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Mbtiles implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(Mbtiles.class);

  private static final ObjectMapper objectMapper = new ObjectMapper();

  private final Connection connection;

  private Mbtiles(Connection connection) {
    this.connection = connection;
  }

  public static Mbtiles newInMemoryDatabase() {
    try {
      return new Mbtiles(DriverManager.getConnection("jdbc:sqlite::memory:"));
    } catch (SQLException throwables) {
      throw new IllegalStateException("Unable to create in-memory database", throwables);
    }
  }

  public static Mbtiles newFileDatabase(Path path) {
    try {
      return new Mbtiles(DriverManager.getConnection("jdbc:sqlite:" + path.toAbsolutePath()));
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

  public void addIndex() {
  }

  public void setupSchema() {
  }

  public void tuneForWrites() {
  }

  public void vacuumAnalyze() {
  }

  public BatchedTileWriter newBatchedTileWriter() {
    return new BatchedTileWriter();
  }

  public class BatchedTileWriter implements AutoCloseable {

    public void write(TileCoord tile, byte[] data) {

    }

    @Override
    public void close() {

    }
  }

  public static record MetadataRow(String name, String value) {

  }

  public static record MetadataJson(List<VectorLayer> vectorLayers) {

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
      NUMBER("Number"),
      BOOLEAN("Boolean"),
      STRING("String");

      private final String name;

      FieldType(String name) {
        this.name = name;
      }

      @Override
      public String toString() {
        return name;
      }
    }

    public static record VectorLayer(
      String id,
      Map<String, FieldType> fields,
      Optional<String> description,
      OptionalInt minzoom,
      OptionalInt maxzoom
    ) {

      public VectorLayer(String id, Map<String, FieldType> fields) {
        this(id, fields, Optional.empty(), OptionalInt.empty(), OptionalInt.empty());
      }

      public VectorLayer copyWithDescription(String newDescription) {
        return new VectorLayer(id, fields, Optional.of(newDescription), minzoom, maxzoom);
      }

      public VectorLayer copyWithMinzoom(int newMinzoom) {
        return new VectorLayer(id, fields, description, OptionalInt.of(newMinzoom), maxzoom);
      }

      public VectorLayer copyWithMaxzoom(int newMaxzoom) {
        return new VectorLayer(id, fields, description, minzoom, OptionalInt.of(newMaxzoom));
      }
    }
  }

  public Metadata metadata() {
    return new Metadata();
  }

  public class Metadata {

    private static String join(double... items) {
      return DoubleStream.of(items).mapToObj(Double::toString).collect(Collectors.joining(","));
    }

    public Metadata setMetadata(String name, Object value) {
      if (value != null) {

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
      double zoom = GeoUtils.getZoomFromLonLatBounds(envelope);
      return setCenter(center.x, center.y, zoom);
    }

    public Metadata setMinzoom(int value) {
      return setMetadata("minzoom", value);
    }

    public Metadata setMaxzoom(int maxZoom) {
      return setMetadata("minzoom", maxZoom);
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
      return setJson(value.toJson());
    }

    public Map<String, String> getAll() {
      return Map.of();
    }
  }
}
