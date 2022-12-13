package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.reader.osm.OsmReader;
import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;


/**
 * Base class for utilities that read {@link SourceFeature SourceFeatures} from a simple data source where geometries
 * can be read in a single pass, like {@link ShapefileReader} but not {@link OsmReader} which requires complex
 * multi-pass processing.
 * <p>
 * Implementations provide features through {@link #readFeatures(Consumer)}} and {@link #getFeatureCount()}}.
 */
public abstract class SimpleReader<F extends SourceFeature> implements Closeable {

  /** Shared monotonically increasing counter to assign unique IDs to features of the same source */
  protected static AtomicLong featureId = new AtomicLong(0L);
  protected final String sourceName;

  protected SimpleReader(String sourceName) {
    this.sourceName = sourceName;
  }

  /** Returns the number of features to be read from this reader to use for displaying progress. */
  public abstract long getFeatureCount();

  /** Reads all features in this data provider, submitting each to {@code next} for further processing. */
  @SuppressWarnings("java:S112")
  public abstract void readFeatures(Consumer<F> next) throws Exception;
}
