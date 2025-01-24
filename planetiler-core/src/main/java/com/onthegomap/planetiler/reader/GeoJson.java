package com.onthegomap.planetiler.reader;

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

  public static GeoJson from(String json) {
    return new GeoJson(null, () -> new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8)));
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

  public long count() {
    try {
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
