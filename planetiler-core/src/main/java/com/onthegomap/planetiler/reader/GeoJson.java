package com.onthegomap.planetiler.reader;

import java.io.IOException;
import java.io.InputStream;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GeoJson implements Iterable<GeoJsonFeature> {
  private static final Logger LOGGER = LoggerFactory.getLogger(GeoJson.class);

  private final InputStreamSupplier inputStreamSupplier;

  public GeoJson(InputStreamSupplier inputStreamSupplier) {
    this.inputStreamSupplier = inputStreamSupplier;
  }

  public Stream<GeoJsonFeature> stream() {
    return iterator().stream();
  }

  @Override
  public GeoJsonFeatureIterator iterator() {
    try {
      return new GeoJsonFeatureIterator(inputStreamSupplier.get());
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
