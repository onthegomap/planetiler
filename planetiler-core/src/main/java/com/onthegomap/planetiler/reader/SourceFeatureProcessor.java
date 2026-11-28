package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.collection.SortableFeature;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.render.FeatureRenderer;
import com.onthegomap.planetiler.stats.Counter;
import com.onthegomap.planetiler.stats.ProcessInfo;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.SourceProgress;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.CompletableFutures;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.util.LogUtil;
import com.onthegomap.planetiler.util.RepeatedErrorLogger;
import com.onthegomap.planetiler.worker.Distributor;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Coordinates reading and processing of one or more sources that can be read in one pass (ie. anything but
 * OpenStreetMap data) in one shared pipeline where a features from each source are sent into a pool of shared workers
 * as soon as they are ready.
 * <p>
 * {@link #process} reads several {@link SourceReader sources} together. An instance reads the files of one source with
 * {@link SimpleReader} objects constructed by {@link #readerFactory}.
 */
public class SourceFeatureProcessor<F extends SourceFeature> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SourceFeatureProcessor.class);
  private static final Format FORMAT = Format.defaultInstance();
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
    PlanetilerConfig config, Profile profile, Stats stats) {

    var processor = new SourceFeatureProcessor<>(sourceName, readerFactory, profile, stats);
    processor.processFiles(sourcePaths, writer, config);
  }


  /**
   * Renders map features for all elements from this data source based on the mapping logic defined in {@code profile},
   * in a stage of its own. To read it together with other sources, use {@link #process} with a
   * {@link SourceReader#ofFiles} source instead.
   *
   * @param sourcePaths list of paths associated with this source
   * @param writer      consumer for rendered features
   * @param config      user-defined parameters controlling number of threads and log interval
   */
  public final void processFiles(List<Path> sourcePaths, FeatureGroup writer, PlanetilerConfig config) {
    var source = SourceReader.ofFiles(sourceName, () -> sourcePaths, readerFactory,
      CompletableFutures.NULL, config.bounds());
    process(sourceName, List.of(source), List.of(), writer, config, profile, stats);
  }

  /**
   * Renders map features for all elements of {@code sources} in one shared pipeline, based on the mapping logic defined
   * in {@code profile}, then closes the sources.
   * <p>
   * Each source gets read once it is ready, in the order they become ready, so a source that is still downloading does
   * not hold up the others. {@link Profile#finish} runs for each source in the order of {@code sources} after all of
   * them have been processed.
   *
   * @param stageName   string ID for this group of sources to use in logs and stats
   * @param sources     the sources to read
   * @param alsoRunning progress of other work going on meanwhile to show in the logs, like downloads for later stages
   * @param writer      consumer for rendered features
   * @param config      user-defined parameters controlling number of threads and log interval
   * @param profile     logic that defines what map features to emit for each source feature
   * @param stats       to keep track of counters and timings
   */
  public static void process(String stageName, List<SourceReader> sources, List<SourceProgress> alsoRunning,
    FeatureGroup writer, PlanetilerConfig config, Profile profile, Stats stats) {
    boolean readTogether = sources.size() > 1;
    var timer = stats.startStage(stageName, readTogether ?
      "Running in parallel: " + sources.stream().map(it -> it.name()).collect(Collectors.joining(", ")) :
      "Starting...");
    try {
      // Use one reader per source so that a source that waits on range requests doesn't hold up other ones.
      int readThreads = Math.min(sources.size(), config.threads());
      AtomicLong sourcesRead = new AtomicLong();
      var featureErrors = new RepeatedErrorLogger(LOGGER, 10);
      // lets finished workers divvy up any remaining work from busy ones
      Distributor<SourceFeature> distributor = Distributor.createWithCapacity(1_000);
      Map<String, SourceProgress> progressBySource =
        sources.stream().collect(Collectors.toMap(SourceReader::name, source -> source.progress().setTrackWritten()));

      var pipeline = WorkerPipeline.start(stageName, stats)
        .<SourceReader>fromGenerator("wait",
          next -> CompletableFutures.inCompletionOrder(sources, sources.size(), SourceReader::whenReady, next))
        .addBuffer("ready", sources.size())
        .<SourceBatch>addWorker("read", readThreads, (ready, next) -> {
          for (SourceReader source : ready) {
            // rethrow failed downloads
            source.whenReady().join();
            var progress = source.progress();
            var workLeft = progress.workLeftCounterForThread();
            String workerStage = LogUtil.getStage();
            var fetch = source.fetchWhileReading().join();
            String fetchMessage = fetch.bytes() == 0 ? null : "only data in bounds (" +
              FORMAT.storage(fetch.bytes()) + "B) from " + fetch.host() + " while processing";
            // While a source gets read, its logs show [source], not [stage:read].
            if (readTogether) {
              // sources read together start one by one as their data arrives
              LogUtil.setStage(source.name());
              LOGGER.info(fetchMessage == null ? "Starting..." : "Downloading " + fetchMessage + "...");
            } else {
              LogUtil.setStage(stageName);
              if (fetchMessage != null) {
                LOGGER.info("Downloading {}", fetchMessage);
              }
            }
            try {
              progress.start();
              source.read(features -> {
                workLeft.inc();
                next.accept(new SourceBatch(progress, features));
              });
              progress.finish();
            } finally {
              LogUtil.setStage(workerStage);
            }
            sourcesRead.incrementAndGet();
          }
        })
        .addBuffer("process_queue", 100)
        .processAndWrite(config.parallelTempIO(), config.featureProcessThreads(), config.featureWriteThreads(), writer,
          (batches, next) -> {
            var featureCollectors = new FeatureCollector.Factory(config, stats);
            var sourceStats = new ProcessStatsBySource(progressBySource);
            try (
              FeatureRenderer renderer = newFeatureRenderer(writer, config, stats, rendered -> {
                sourceStats.featureWritten();
                next.accept(rendered);
              });
              var features = distributor.forThread(sourceFeature -> {
                sourceStats.startFeature(sourceFeature.getSource());
                FeatureCollector collector = featureCollectors.get(sourceFeature);
                try {
                  profile.processFeature(sourceFeature, collector);
                  for (FeatureCollector.Feature renderable : collector) {
                    renderer.accept(renderable);
                  }
                } catch (Exception e) {
                  featureErrors.log("Error processing " + sourceFeature, e);
                }
                sourceStats.featureProcessed();
              })
            ) {
              for (var batch : batches) {
                var workLeft = sourceStats.workLeftCounter(batch.progress());
                for (SourceFeature sourceFeature : batch.features()) {
                  // counted before handing it off, since another worker may take it from the distributor
                  workLeft.inc();
                  features.accept(sourceFeature);
                }
                workLeft.incBy(-1);
              }
            }
            sourceStats.finish();
          });

      var loggers = ProgressLoggers.create();
      List<SourceProgress> rows = new ArrayList<>();
      if (sources.size() == 1) {
        // a source that has the stage to itself gets its cells on the first line, like other stages' logs
        loggers.addSourceProgress(sources.getFirst().progress());
      } else {
        loggers.addPercentCounter("sources read", sources.size(), sourcesRead)
          .addRateCounter("features written", writer::numFeaturesWritten);
        sources.forEach(source -> rows.add(source.progress()));
      }
      rows.addAll(alsoRunning);
      loggers.addFileSize(writer).newLine()
        .addSourceProgress(rows).newLine()
        .addProcessStats().newLine()
        .addPipelineStats(pipeline);

      pipeline.awaitAndLog(loggers, config.logInterval());

      for (SourceReader source : sources) {
        if (!source.name().equals(stageName)) {
          LogUtil.setStage(source.name());
        }
        LOGGER.atDebug().setMessage("Finished {}").addArgument(source.progress()::summary).log();
        // hook for profile to do any post-processing after this source is read
        try (
          var threadLocalWriter = writer.writerForThread();
          var featureRenderer = newFeatureRenderer(writer, config, stats, threadLocalWriter)
        ) {
          profile.finish(source.name(), new FeatureCollector.Factory(config, stats), featureRenderer);
        } catch (IOException e) {
          LOGGER.warn("Error closing writer", e);
        }
      }
      LogUtil.setStage(stageName);
    } finally {
      FileUtils.closeAll(sources);
    }
    timer.stop();
  }

  /** Features from one source, which the read workers hand to the process workers. */
  private record SourceBatch(SourceProgress progress, Iterable<? extends SourceFeature> features) {}

  // tracks CPU time used in process threads by source to report at the end of a run
  private static class ProcessStatsBySource {
    private final Map<String, SourceProgress> progressBySource;
    private final Map<String, ForSource> seen = new HashMap<>();
    private String currentSource;
    private ForSource current;
    private long currentSourceStartCpuNanos;

    ProcessStatsBySource(Map<String, SourceProgress> progressBySource) {
      this.progressBySource = progressBySource;
    }

    void startFeature(String source) {
      if (!Objects.equals(source, currentSource)) {
        long now = ProcessInfo.getCurrentThreadCpuNanos();
        if (current != null) {
          current.cpuNanos += now - currentSourceStartCpuNanos;
        }
        currentSource = source;
        var progress = source == null ? null : progressBySource.get(source);
        // features that a custom reader names after some other source don't count toward any source
        current = progress == null ? null : forSource(progress);
        currentSourceStartCpuNanos = now;
      }
    }

    Counter workLeftCounter(SourceProgress progress) {
      return forSource(progress).workLeft;
    }

    private ForSource forSource(SourceProgress progress) {
      return seen.computeIfAbsent(progress.name(), name -> new ForSource(progress));
    }

    void featureProcessed() {
      if (current != null) {
        current.workLeft.incBy(-1);
      }
    }

    void featureWritten() {
      if (current != null) {
        current.written.inc();
      }
    }

    void finish() {
      startFeature(null);
      seen.values().forEach(forSource -> forSource.progress.addProcessCpu(forSource.cpuNanos));
    }

    private static final class ForSource {
      private final SourceProgress progress;
      private final Counter written;
      private final Counter workLeft;
      private long cpuNanos = 0;

      ForSource(SourceProgress progress) {
        this.progress = progress;
        this.written = progress.writtenCounterForThread();
        this.workLeft = progress.workLeftCounterForThread();
      }
    }
  }

  private static FeatureRenderer newFeatureRenderer(FeatureGroup writer, PlanetilerConfig config, Stats stats,
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
}
