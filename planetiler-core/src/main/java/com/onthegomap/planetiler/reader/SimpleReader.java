package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.reader.osm.OsmReader;
import com.onthegomap.planetiler.reader.shapefile.ShapefileReader;
import java.io.Closeable;
import java.util.function.Consumer;


/**
 * Base class for utilities that read {@link SourceFeature SourceFeatures} from a simple data source where geometries
 * can be read in a single pass, like {@link ShapefileReader} but not {@link OsmReader} which requires complex
 * multi-pass processing.
 * <p>
 * Implementations provide features through {@link #readFeatures(Consumer)}} and {@link #getFeatureCount()}}.
 */
public abstract class SimpleReader<F extends SourceFeature> implements Closeable {

  protected final String sourceName;

  protected SimpleReader(String sourceName) {
    this.sourceName = sourceName;
  }

  /** Returns the number of features to be read from this reader to use for displaying progress. */
  public abstract long getFeatureCount();

  /** Assign feature IDs in the order they are read starting from {@code idOffset}. */
  public void setFeatureIdOffset(long idOffset) {}

  /** Returns true if {@link #readFeatures(Consumer)} omits features outside the bounds */
  public boolean filtersToBounds() {
    return false;
  }

  /** Reads all features in this data provider, submitting each to {@code next} for further processing. */
  @SuppressWarnings("java:S112")
  public abstract void readFeatures(Consumer<F> next) throws Exception;
}
