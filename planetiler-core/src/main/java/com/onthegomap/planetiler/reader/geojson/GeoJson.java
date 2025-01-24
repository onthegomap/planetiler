package com.onthegomap.planetiler.reader.geojson;

import com.onthegomap.planetiler.reader.FileFormatException;
import com.onthegomap.planetiler.util.CloseableIterator;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A streaming geojson parser that can handle arbitrarily large regular or newline-delimited geojson files without
 * loading the whole thing in memory.
 * <p>
 * This emits every top-level feature or feature contained within a {@code FeatureCollection}. Invalid json syntax will
 * throw an exception, but unexpected geojson objects will just log a warning and emit empty geometries.
 *
 * @see <a href="https://stevage.github.io/ndgeojson/">Newline-delimted geojson</a>
 * @see <a href="https://datatracker.ietf.org/doc/html/rfc7946">GeoJSON specification (RFC 7946)</a>
 */
public class GeoJson implements Iterable<GeoJsonFeature> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoJson.class);

  private final InputStreamSupplier inputStreamSupplier;
  private final String name;

  private GeoJson(String name, InputStreamSupplier inputStreamSupplier) {
    this.inputStreamSupplier = inputStreamSupplier;
    this.name = name;
  }

  public static GeoJson from(Path path) {
    return new GeoJson(path.toString(), () -> Files.newInputStream(path));
  }

  public static GeoJson from(byte[] json) {
    return new GeoJson(null, () -> new ByteArrayInputStream(json));
  }

  public static GeoJson from(String json) {
    return from(json.getBytes(StandardCharsets.UTF_8));
  }

  public static GeoJson from(InputStreamSupplier inputStreamSupplier) {
    return new GeoJson(null, inputStreamSupplier);
  }

  public Stream<GeoJsonFeature> stream() {
    return iterator().stream();
  }

  @Override
  public CloseableIterator<GeoJsonFeature> iterator() {
    try {
      return new GeoJsonFeatureIterator(inputStreamSupplier.get(), name);
    } catch (IOException e) {
      throw new FileFormatException("Unable to read geojson file", e);
    }
  }

  /** Returns the number of geojson features in this document. */
  public long count() {
    try {
      LOGGER.info("Counting geojson features in {}", name);
      return GeoJsonFeatureCounter.count(inputStreamSupplier.get());
    } catch (IOException e) {
      LOGGER.warn("Unable to feature count", e);
      return 0;
    }
  }

  @FunctionalInterface
  public interface InputStreamSupplier {
    InputStream get() throws IOException;
  }
}
