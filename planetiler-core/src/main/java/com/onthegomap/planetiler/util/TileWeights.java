package com.onthegomap.planetiler.util;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.RuntimeJsonMappingException;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileCoord;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Holds tile weights to compute weighted average tile sizes.
 * <p>
 * {@link TopOsmTiles} can be used to get tile weights from 90 days of openstreetmap.org tile traffic.
 */
public class TileWeights {
  private static final Logger LOGGER = LoggerFactory.getLogger(TileWeights.class);
  private static final CsvMapper MAPPER = new CsvMapper();
  private static final CsvSchema SCHEMA = MAPPER
    .schemaFor(Row.class)
    .withHeader()
    .withColumnSeparator('\t')
    .withLineSeparator("\n");
  private static final ObjectWriter WRITER = MAPPER.writer(SCHEMA);
  private static final ObjectReader READER = MAPPER.readerFor(Row.class).with(SCHEMA);
  private final Map<Integer, Long> byZoom = new HashMap<>();
  private final Map<TileCoord, Long> weights = new HashMap<>();

  public long getWeight(TileCoord coord) {
    return weights.getOrDefault(coord, 0L);
  }

  /** Returns the sum of all tile weights at a specific zoom */
  public long getZoomWeight(int zoom) {
    return byZoom.getOrDefault(zoom, 0L);
  }

  /** Adds {@code weight} to the current weight for {@code coord} and returns this modified instance. */
  public TileWeights put(TileCoord coord, long weight) {
    weights.merge(coord, weight, Long::sum);
    byZoom.merge(coord.z(), weight, Long::sum);
    return this;
  }

  /**
   * Write tile weights to a gzipped TSV file with {@code z, x, y, loads} columns.
   */
  public void writeToFile(Path path) throws IOException {
    try (
      var output = new GZIPOutputStream(
        new BufferedOutputStream(Files.newOutputStream(path, CREATE, TRUNCATE_EXISTING, WRITE)));
      var writer = WRITER.writeValues(output)
    ) {
      var sorted = weights.entrySet().stream()
        .sorted(Comparator.comparingInt(e -> e.getKey().encoded()))
        .iterator();
      while (sorted.hasNext()) {
        var entry = sorted.next();
        TileCoord coord = entry.getKey();
        writer.write(new Row(coord.z(), coord.x(), coord.y(), entry.getValue()));
      }
    }
  }

  /**
   * Load tile weights from a gzipped TSV file with {@code z, x, y, loads} columns.
   * <p>
   * Duplicate entries will be added together.
   */
  public static TileWeights readFromFile(Path path) {
    TileWeights result = new TileWeights();
    try (
      var input = new GZIPInputStream(new BufferedInputStream(Files.newInputStream(path)));
      var reader = READER.<Row>readValues(input)
    ) {
      while (reader.hasNext()) {
        var row = reader.next();
        if (row.z >= PlanetilerConfig.MIN_MINZOOM && row.z <= PlanetilerConfig.MAX_MAXZOOM) {
          int x = row.x % (1 << row.z);
          int y = row.y % (1 << row.z);
          result.put(TileCoord.ofXYZ(x, y, row.z()), row.loads());
        }
      }
    } catch (IOException | RuntimeJsonMappingException e) {
      LOGGER.warn("Unable to load tile weights from {}, will fall back to unweighted average: {}", path, e);
      return new TileWeights();
    }
    return result;
  }

  public boolean isEmpty() {
    return byZoom.entrySet().stream().anyMatch(e -> e.getValue() > 0);
  }

  @JsonPropertyOrder({"z", "x", "y", "loads"})
  private record Row(int z, int x, int y, long loads) {}

  @Override
  public String toString() {
    return "TileWeights{\n" +
      weights.entrySet().stream()
        .map(result -> result.getKey() + ": " + result.getValue())
        .collect(Collectors.joining("\n"))
        .indent(2) +
      '}';
  }

  @Override
  public boolean equals(Object o) {
    return o == this || (o instanceof TileWeights other && Objects.equals(other.weights, weights));
  }

  @Override
  public int hashCode() {
    return weights.hashCode();
  }
}
