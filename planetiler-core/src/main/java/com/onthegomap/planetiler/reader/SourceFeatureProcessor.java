package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.collection.SortableFeature;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.render.FeatureRenderer;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class coordinates reading and processing of one or more Path objects that are grouped under a single source
 * name.
 * <p>
 * The paths will be processed in parallel according to the the {@link #profile} using {@link SimpleReader} objects
 * constructed by {@link #readerFactory}.
 */
public class SourceFeatureProcessor<F extends SourceFeature> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SourceFeatureProcessor.class);

  private final Profile profile;
  private final Stats stats;
  private final String sourceName;
  private final Function<Path, SimpleReader<F>> readerFactory;

  protected SourceFeatureProcessor(String sourceName, Function<Path, SimpleReader<F>> readerFactory, Profile profile,
    Stats stats) {
    this.profile = profile;
    this.stats = stats;
    this.sourceName = sourceName;
    this.readerFactory = readerFactory;
  }

  /**
   * Renders map features for all elements contained within {@param sourcePaths}, based on the mapping logic defined in
   * {@param profile}.
   *
   * @param sourceName    string ID for this reader to use in logs and stats
   * @param sourcePaths   paths to files used for this source
   * @param readerFactory function to construct a {@link SimpleReader} for a specific path
   * @param writer        consumer for rendered features
   * @param config        user-defined parameters controlling number of threads and log interval
   * @param profile       logic that defines what map features to emit for each source feature
   * @param stats         to keep track of counters and timings
   * @throws IllegalArgumentException if a problem occurs reading the input file
   */
  public static <F extends SourceFeature> void processFiles(
    String sourceName, List<Path> sourcePaths, Function<Path, SimpleReader<F>> readerFactory, FeatureGroup writer,
    PlanetilerConfig config,
    Profile profile, Stats stats) {

    var processor = new SourceFeatureProcessor<>(sourceName, readerFactory, profile, stats);
    processor.processFiles(sourcePaths, writer, config);
  }


  /**
   * Renders map features for all elements from this data source based on the mapping logic defined in {@code profile}.
   *
   * @param sourcePaths list of paths associated with this source
   * @param writer      consumer for rendered features
   * @param config      user-defined parameters controlling number of threads and log interval
   */
  public final void processFiles(List<Path> sourcePaths, FeatureGroup writer, PlanetilerConfig config) {
    var timer = stats.startStage(sourceName);
    long featureCount = getFeatureCount(sourcePaths);
    int readThreads = config.featureReadThreads();
    int writeThreads = config.featureWriteThreads();
    int processThreads = config.featureProcessThreads();
    Envelope latLonBounds = config.bounds().latLon();
    AtomicLong featuresRead = new AtomicLong(0);
    AtomicLong featuresWritten = new AtomicLong(0);

    var pipeline = WorkerPipeline.start(sourceName, stats)
      .readFromTiny("source_paths", sourcePaths)
      .addWorker("read", readThreads, readPaths())
      .addBuffer("process_queue", 1000, 1)
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
      .sinkTo("write", writeThreads, prev -> {
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

  private long getFeatureCount(List<Path> sourcePaths) {
    long featureCount = 0;
    for (var path : sourcePaths) {
      try (var reader = readerFactory.apply(path)) {
        featureCount += reader.getFeatureCount();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    return featureCount;
  }

  /** Returns a source that initiates a {@link WorkerPipeline} with elements from this data provider. */
  private WorkerPipeline.WorkerStep<Path, F> readPaths() {
    return (paths, consumer) -> {
      for (var path : paths) {
        try (var reader = readerFactory.apply(path)) {
          reader.readFeatures(consumer);
        }
      }
    };
  }
}
