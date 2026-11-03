package com.onthegomap.planetiler.reader.parquet;

import static io.prometheus.client.Collector.NANOSECONDS_PER_SECOND;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.collection.SortableFeature;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.remote.DownloadMode;
import com.onthegomap.planetiler.reader.remote.RangeFile;
import com.onthegomap.planetiler.render.FeatureRenderer;
import com.onthegomap.planetiler.stats.Counter;
import com.onthegomap.planetiler.stats.ProcessInfo;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.stats.Timers;
import com.onthegomap.planetiler.util.CompletableFutures;
import com.onthegomap.planetiler.util.Downloader;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.util.FunctionThatThrows;
import com.onthegomap.planetiler.worker.Distributor;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads {@link SourceFeature SourceFeatures} from one or more
 * <a href="https://github.com/opengeospatial/geoparquet/blob/main/format-specs/geoparquet.md">geoparquet</a> files.
 * <p>
 * If files don't contain geoparquet metadata then try to get geometry from "geometry" "wkb_geometry" or "wkt_geometry"
 * fields.
 */
public class ParquetReader {
  public static final String DEFAULT_LAYER = "features";

  private static final Logger LOGGER = LoggerFactory.getLogger(ParquetReader.class);
  private final String sourceName;
  private final Function<Map<String, Object>, Object> idGenerator;
  private final Function<Map<String, Object>, Object> layerGenerator;
  private final Profile profile;
  private final Stats stats;
  private final boolean hivePartitioning;

  public ParquetReader(
    String sourceName,
    Profile profile,
    Stats stats
  ) {
    this(sourceName, profile, stats, null, null, false);
  }

  public ParquetReader(
    String sourceName,
    Profile profile,
    Stats stats,
    Function<Map<String, Object>, Object> getId,
    Function<Map<String, Object>, Object> getLayer,
    boolean hivePartitioning
  ) {
    this.sourceName = sourceName;
    this.layerGenerator = getLayer;
    this.idGenerator = getId;
    this.profile = profile;
    this.stats = stats;
    this.hivePartitioning = hivePartitioning;
  }

  static Map<String, Object> getHivePartitionFields(Path path) {
    Map<String, Object> fields = new HashMap<>();
    for (var part : path) {
      var string = part.toString();
      if (string.contains("=")) {
        var parts = string.split("=");
        fields.put(parts[0], parts[1]);
      }
    }
    return fields.isEmpty() ? null : fields;
  }

  public void process(List<Path> sourcePath, FeatureGroup writer, PlanetilerConfig config) {
    try {
      processLocal(sourcePath, writer, config);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private void processLocal(List<Path> sourcePath, FeatureGroup writer, PlanetilerConfig config) throws IOException {
    var paths = sourcePath.stream().filter(path -> !"_SUCCESS".equals(path.getFileName().toString())).toList();
    // local files open quickly, so open them on this thread
    processAll(paths, RangeFile::openLocal, task -> CompletableFutures.supplyAsync(task, Runnable::run), writer, config);
  }

  /**
   * Reads features from local or remote files and writes them to {@code writer}.
   * <p>
   * {@code pathToUrl} maps where each file is (or would be) on local disk to its URL. Files that are complete on local
   * disk get read from there, the others from their URL according to {@code mode} (see
   * {@link RangeFile#open(Path, String, DownloadMode, boolean, Downloader)}).
   */
  public void processRemote(Map<Path, String> pathToUrl, DownloadMode mode, boolean refresh, Downloader downloader,
    FeatureGroup writer, PlanetilerConfig config) throws IOException {
    // opening a remote file and reading its footer take a round trip or two each, so open them all at once
    processAll(pathToUrl.keySet().stream().sorted().toList(),
      path -> RangeFile.open(path, pathToUrl.get(path), mode, refresh, downloader), downloader::supplyAsync, writer,
      config);
  }

  private void processAll(List<Path> paths, FunctionThatThrows<Path, RangeFile> open,
    Function<Callable<ParquetInputFile>, CompletableFuture<ParquetInputFile>> start, FeatureGroup writer,
    PlanetilerConfig config) throws IOException {
    var timer = stats.startStage(sourceName);
    var opening = paths.stream().map(path -> start.apply(() -> newInputFile(path, open.apply(path), config))).toList();
    var inputFiles = CompletableFutures.joinAllOrClose(opening);
    try {
      process(inputFiles, writer, config, timer);
    } finally {
      closeAll(inputFiles);
    }
  }

  private ParquetInputFile newInputFile(Path path, RangeFile file, PlanetilerConfig config) throws IOException {
    try {
      return new ParquetInputFile(sourceName, getLayerName(path), path, file, null, config.bounds(),
        hivePartitioning ? getHivePartitionFields(path) : null, idGenerator);
    } catch (RuntimeException e) {
      // not try-with-resources: on success the ParquetInputFile owns the file and closes it later
      file.close();
      throw e;
    }
  }

  private static void closeAll(List<ParquetInputFile> files) throws IOException {
    for (var file : files) {
      file.close();
    }
  }

  private void process(List<ParquetInputFile> files, FeatureGroup writer, PlanetilerConfig config,
    Timers.Finishable timer) {
    var inputFiles = files.stream().filter(file -> !file.shouldSkip(profile)).toList();
    // don't show % complete on features when a filter is present because to determine total # elements would
    // take an expensive initial query, and % complete on blocks gives a good enough proxy
    long featureCount = inputFiles.stream().anyMatch(ParquetInputFile::hasFilter) ? 0 :
      inputFiles.stream().mapToLong(ParquetInputFile::getCount).sum();
    long blockCount = inputFiles.stream().mapToLong(ParquetInputFile::getBlockCount).sum();
    int processThreads = config.featureProcessThreads();
    int writeThreads = config.featureWriteThreads();
    var blocksRead = Counter.newMultiThreadCounter();
    var featuresRead = Counter.newMultiThreadCounter();
    Map<String, Integer> workingOn = new ConcurrentHashMap<>();
    var inputBlocks = inputFiles.stream().<ParquetInputFile.Block>mapMulti((file, next) -> {
      try (var blockReader = file.get()) {
        for (var block : blockReader) {
          next.accept(block);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }).toList();
    Distributor<ParquetFeature> distributor = Distributor.createWithCapacity(1_000);

    int lookAhead = lookAheadBlocks(blockCount, inputFiles.stream().mapToLong(ParquetInputFile::getBytesToRead).sum(),
      processThreads, config);

    var rangeFiles = inputFiles.stream().map(ParquetInputFile::file).toList();
    // files complete on local disk fetch nothing, and opening a remote one always makes a request
    boolean remote = rangeFiles.stream().anyMatch(file -> file.requestsMade() > 0);
    // before the pipeline starts fetching: progress counts the row group bytes only, not the footers fetched already
    long bytesToFetch = inputFiles.stream().mapToLong(ParquetInputFile::getBytesToFetch).sum();
    long requestsToFetch = inputFiles.stream().mapToLong(ParquetInputFile::getRequestsToFetch).sum();
    long bytesFetchedBefore = rangeFiles.stream().mapToLong(RangeFile::bytesFetched).sum();
    long requestsMadeBefore = rangeFiles.stream().mapToLong(RangeFile::requestsMade).sum();

    var pipeline = WorkerPipeline.start(sourceName, stats).<ParquetInputFile.Block>fromGenerator("fetch", next -> {
      for (var block : inputBlocks) {
        block.startFetchIfNecessary();
        next.accept(block);
      }
    })
      // remote blocks start fetching once they enter this queue, so its size bounds the requests in flight and the
      // fetched bytes held in memory
      .addBuffer("blocks", lookAhead, 1)
      .processAndWrite(config.parallelTempIO(), processThreads, writeThreads, writer, (prev, next) -> {
        var blocks = blocksRead.counterForThread();
        var elements = featuresRead.counterForThread();
        var featureCollectors = new FeatureCollector.Factory(config, stats);
        try (FeatureRenderer renderer = newFeatureRenderer(writer, config, next)) {
          var consumer = distributor.forThread(sourceFeature -> {
            FeatureCollector features = featureCollectors.get(sourceFeature);
            try {
              profile.processFeature(sourceFeature, features);
              for (FeatureCollector.Feature renderable : features) {
                renderer.accept(renderable);
              }
            } catch (Exception e) {
              LOGGER.error("Error processing {}", sourceFeature, e);
            }
            elements.inc();
          });
          for (var block : prev) {
            String layer = block.layer();
            workingOn.merge(layer, 1, Integer::sum);
            for (var sourceFeature : block) {
              consumer.accept(sourceFeature);
            }
            blocks.inc();
            workingOn.merge(layer, -1, Integer::sum);
          }
          consumer.close();
        }
      });

    var loggers = ProgressLoggers.create()
      .addRatePercentCounter("read", featureCount, featuresRead, true)
      .addRatePercentCounter("blocks", blockCount, blocksRead, false);

    if (remote) {
      var format = loggers.getFormat();
      loggers = loggers
        .addStorageRatePercentCounter("download", bytesToFetch,
          () -> rangeFiles.stream().mapToLong(RangeFile::bytesFetched).sum() - bytesFetchedBefore, false)
        .add(() -> " (" +
          format.numeric(rangeFiles.stream().mapToLong(RangeFile::requestsMade).sum() - requestsMadeBefore) + "/" +
          format.numeric(requestsToFetch) + ")");
    }

    loggers = loggers
      .addRateCounter("write", writer::numFeaturesWritten)
      .addFileSize(writer)
      .newLine()
      .add(() -> workingOn.entrySet().stream()
        .sorted(Map.Entry.<String, Integer>comparingByValue().reversed())
        .filter(d -> d.getValue() > 0)
        .map(d -> d.getKey() + ": " + d.getValue())
        .collect(Collectors.joining(", ")))
      .newLine()
      .addProcessStats()
      .newLine()
      .addPipelineStats(pipeline);

    pipeline.awaitAndLog(loggers, config.logInterval());

    if (LOGGER.isInfoEnabled()) {
      var format = Format.defaultInstance();
      long count = featuresRead.get();
      var elapsed = timer.elapsed();
      LOGGER.info("Processed {} parquet features ({}/s, {} blocks, {} files{}) in {}",
        format.integer(count),
        format.numeric(count * NANOSECONDS_PER_SECOND / elapsed.wall().toNanos()),
        format.integer(blocksRead.get()),
        format.integer(inputFiles.size()),
        remote ? ", downloaded " + fetched(rangeFiles) : "",
        elapsed
      );
    }
    timer.stop();

    // hook for profile to do any post-processing after this source is read
    try (
      var threadLocalWriter = writer.writerForThread();
      var featureRenderer = newFeatureRenderer(writer, config, threadLocalWriter)
    ) {
      profile.finish(sourceName, new FeatureCollector.Factory(config, stats), featureRenderer);
    } catch (IOException e) {
      LOGGER.warn("Error closing writer", e);
    }
  }

  private static String fetched(List<RangeFile> files) {
    var format = Format.defaultInstance();
    return format.storage(files.stream().mapToLong(RangeFile::bytesFetched).sum(), false) + "B in " +
      format.integer(files.stream().mapToLong(RangeFile::requestsMade).sum()) + " requests";
  }

  /**
   * Returns how many blocks to fetch ahead of the workers: enough to keep every range request busy twice over (so the
   * next requests start as soon as some finish) and at least one per worker, but no more than 1/8 of the heap.
   */
  static int lookAheadBlocks(long blocks, long bytes, int processThreads, PlanetilerConfig config) {
    long averageBlockBytes = blocks == 0 ? 1 : Math.max(1, bytes / blocks);
    long rangeBytesInFlight = config.downloadRangeConcurrency() * config.downloadRangeChunkSizeMB() * 1_000_000L;
    long wanted = Math.max(processThreads, Math.ceilDiv(2 * rangeBytesInFlight, averageBlockBytes));
    long memoryLimit = ProcessInfo.getMaxMemoryBytes() / 8;
    return Math.clamp(Math.min(wanted, memoryLimit / averageBlockBytes), 1, Integer.MAX_VALUE);
  }

  private String getLayerName(Path path) {
    String layer = DEFAULT_LAYER;
    if (hivePartitioning) {
      var fields = getHivePartitionFields(path);
      layer = layerGenerator.apply(fields == null ? Map.of() : fields) instanceof Object o ? o.toString() : layer;
    }
    return layer;
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
}
