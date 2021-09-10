package com.onthegomap.flatmap.reader;

import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.Profile;
import com.onthegomap.flatmap.collection.FeatureGroup;
import com.onthegomap.flatmap.collection.SortableFeature;
import com.onthegomap.flatmap.config.FlatmapConfig;
import com.onthegomap.flatmap.reader.osm.OsmReader;
import com.onthegomap.flatmap.render.FeatureRenderer;
import com.onthegomap.flatmap.stats.ProgressLoggers;
import com.onthegomap.flatmap.stats.Stats;
import com.onthegomap.flatmap.worker.WorkerPipeline;
import java.io.Closeable;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import org.locationtech.jts.geom.Envelope;

/**
 * Base class for utilities that read {@link SourceFeature SourceFeatures} from a simple data source where geometries
 * can be read in a single pass, like {@link ShapefileReader} but not {@link OsmReader} which requires complex
 * multi-pass processing.
 * <p>
 * Implementations provide features through {@link #read()} and {@link #getCount()} and this class handles processing
 * them in parallel according to the profile in {@link #process(FeatureGroup, FlatmapConfig)}.
 */
public abstract class SimpleReader implements Closeable {

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
  public final void process(FeatureGroup writer, FlatmapConfig config) {
    var timer = stats.startStage(sourceName);
    long featureCount = getCount();
    int threads = config.threads();
    Envelope latLonBounds = config.bounds().latLon();
    AtomicLong featuresRead = new AtomicLong(0);
    AtomicLong featuresWritten = new AtomicLong(0);

    var pipeline = WorkerPipeline.start(sourceName, stats)
      .fromGenerator("read", read())
      .addBuffer("read_queue", 1000)
      .<SortableFeature>addWorker("process", threads, (prev, next) -> {
        SourceFeature sourceFeature;
        var featureCollectors = new FeatureCollector.Factory(config, stats);
        FeatureRenderer renderer = newFeatureRenderer(writer, config, next);
        while ((sourceFeature = prev.get()) != null) {
          featuresRead.incrementAndGet();
          FeatureCollector features = featureCollectors.get(sourceFeature);
          if (sourceFeature.latLonGeometry().getEnvelopeInternal().intersects(latLonBounds)) {
            profile.processFeature(sourceFeature, features);
            for (FeatureCollector.Feature renderable : features) {
              renderer.accept(renderable);
            }
          }
        }
      })
      // output large batches since each input may map to many tiny output features (i.e. slicing ocean tiles)
      // which turns enqueueing into the bottleneck
      .addBuffer("write_queue", 50_000, 1_000)
      .sinkToConsumer("write", 1, (item) -> {
        featuresWritten.incrementAndGet();
        writer.accept(item);
      });

    var loggers = ProgressLoggers.create()
      .addRatePercentCounter("read", featureCount, featuresRead)
      .addRateCounter("write", featuresWritten)
      .addFileSize(writer)
      .newLine()
      .addProcessStats()
      .newLine()
      .addPipelineStats(pipeline);

    pipeline.awaitAndLog(loggers, config.logInterval());

    // hook for profile to do any post-processing after this source is read
    profile.finish(sourceName,
      new FeatureCollector.Factory(config, stats),
      newFeatureRenderer(writer, config, writer)
    );
    timer.stop();
  }


  private FeatureRenderer newFeatureRenderer(FeatureGroup writer, FlatmapConfig config,
    Consumer<SortableFeature> next) {
    var encoder = writer.newRenderedFeatureEncoder();
    return new FeatureRenderer(
      config,
      rendered -> next.accept(encoder.apply(rendered)),
      stats
    );
  }

  /** Returns the number of features to be read from this source to use for displaying progress. */
  public abstract long getCount();

  /** Returns a source that initiates a {@link WorkerPipeline} with elements from this data provider. */
  public abstract WorkerPipeline.SourceStep<? extends SourceFeature> read();
}
