package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.collection.SortableFeature;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import com.onthegomap.planetiler.render.FeatureRenderer;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Base class for utilities that read {@link SourceFeature SourceFeatures} from a simple data source where geometries
 * can be read in a single pass, like {@link ShapefileReader} but not {@link OsmReader} which requires complex
 * multi-pass processing.
 * <p>
 * Implementations provide features through {@link #read()} and {@link #getCount()} and this class handles processing
 * them in parallel according to the profile in {@link #process(FeatureGroup, PlanetilerConfig)}.
 */
public abstract class SimpleReader implements Closeable {

  private static final Logger LOGGER = LoggerFactory.getLogger(SimpleReader.class);

  protected final Stats stats;
  protected final String sourceName;
  private final Profile profile;

  protected SimpleReader(Profile profile, Stats stats, String sourceName) {
    this.stats = stats;
    this.profile = profile;
    this.sourceName = sourceName;
  }

  /**
   * Renders map features for all elements from this data source based on the mapping logic defined in {@code profile}.
   *
   * @param writer consumer for rendered features
   * @param config user-defined parameters controlling number of threads and log interval
   */
  public final void process(FeatureGroup writer, PlanetilerConfig config) {
    var timer = stats.startStage(sourceName);
    long featureCount = getCount();
    int writers = config.featureWriteThreads();
    int processThreads = config.featureProcessThreads();
    Envelope latLonBounds = config.bounds().latLon();
    AtomicLong featuresRead = new AtomicLong(0);
    AtomicLong featuresWritten = new AtomicLong(0);

    var pipeline = WorkerPipeline.start(sourceName, stats)
      .fromGenerator("read", read())
      .addBuffer("read_queue", 1000)
      .<SortableFeature>addWorker("process", processThreads, (prev, next) -> {
        var featureCollectors = new FeatureCollector.Factory(config, stats);
        try (FeatureRenderer renderer = newFeatureRenderer(writer, config, next)) {
          for (SourceFeature sourceFeature : prev) {
            featuresRead.incrementAndGet();
            FeatureCollector features = featureCollectors.get(sourceFeature);
            if (sourceFeature.latLonGeometry().getEnvelopeInternal().intersects(latLonBounds)) {
              try {
                profile.processFeature(sourceFeature, features);
                for (FeatureCollector.Feature renderable : features) {
                  renderer.accept(renderable);
                }
              } catch (Exception e) {
                LOGGER.error("Error processing " + sourceFeature, e);
              }
            }
          }
        }
      })
      // output large batches since each input may map to many tiny output features (i.e. slicing ocean tiles)
      // which turns enqueueing into the bottleneck
      .addBuffer("write_queue", 50_000, 1_000)
      .sinkTo("write", writers, prev -> {
        try (var threadLocalWriter = writer.writerForThread()) {
          for (var item : prev) {
            featuresWritten.incrementAndGet();
            threadLocalWriter.accept(item);
          }
        }
      });

    var loggers = ProgressLoggers.create()
      .addRatePercentCounter("read", featureCount, featuresRead, true)
      .addRateCounter("write", featuresWritten)
      .addFileSize(writer)
      .newLine()
      .addProcessStats()
      .newLine()
      .addPipelineStats(pipeline);

    pipeline.awaitAndLog(loggers, config.logInterval());

    // hook for profile to do any post-processing after this source is read
    try (
      var threadLocalWriter = writer.writerForThread();
      var featureRenderer = newFeatureRenderer(writer, config, threadLocalWriter)
    ) {
      profile.finish(sourceName, new FeatureCollector.Factory(config, stats), featureRenderer);
    } catch (IOException e) {
      LOGGER.warn("Error closing writer", e);
    }
    timer.stop();
  }

  private FeatureRenderer newFeatureRenderer(FeatureGroup writer, PlanetilerConfig config,
    Consumer<SortableFeature> next) {
    @SuppressWarnings("java:S2095") // closed by FeatureRenderer
    var encoder = writer.newRenderedFeatureEncoder();
    return new FeatureRenderer(
      config,
      rendered -> next.accept(encoder.apply(rendered)),
      stats,
      encoder
    );
  }

  /** Returns the number of features to be read from this source to use for displaying progress. */
  public abstract long getCount();

  /** Returns a source that initiates a {@link WorkerPipeline} with elements from this data provider. */
  public abstract WorkerPipeline.SourceStep<? extends SourceFeature> read();
}
