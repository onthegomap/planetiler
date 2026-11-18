package com.onthegomap.planetiler.reader.parquet;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.SourceFeatureProcessor;
import com.onthegomap.planetiler.reader.SourceReader;
import com.onthegomap.planetiler.reader.remote.DownloadMode;
import com.onthegomap.planetiler.reader.remote.RangeFile;
import com.onthegomap.planetiler.stats.ProcessInfo;
import com.onthegomap.planetiler.stats.SourceProgress;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.CompletableFutures;
import com.onthegomap.planetiler.util.Downloader;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.util.FunctionThatThrows;
import com.onthegomap.planetiler.util.RequestPlanner;
import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.LongAdder;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;

/**
 * Reads {@link SourceFeature SourceFeatures} from one or more
 * <a href="https://github.com/opengeospatial/geoparquet/blob/main/format-specs/geoparquet.md">geoparquet</a> files.
 * <p>
 * If files don't contain geoparquet metadata then try to get geometry from "geometry" "wkb_geometry" or "wkt_geometry"
 * fields.
 */
public class ParquetReader {
  public static final String DEFAULT_LAYER = "features";

  // parquet files keep their metadata in a footer, so fetching a generous chunk of the end of a remote file with the
  // request that opens it avoids a round trip per metadata read
  private static final int FOOTER_PREFETCH_BYTES = 1 << 20;
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
    process(source(() -> sourcePath, CompletableFutures.NULL, config), writer, config);
  }

  /**
   * Reads features from local or remote files and writes them to {@code writer}.
   * <p>
   * {@code pathToUrl} maps where each file is (or would be) on local disk to its URL. Files that are complete on local
   * disk get read from there, the others from their URL according to {@code mode} (see
   * {@link RangeFile#open(Path, String, int, DownloadMode, boolean, Downloader)}).
   */
  public void processRemote(Map<Path, String> pathToUrl, DownloadMode mode, boolean refresh, Downloader downloader,
    FeatureGroup writer, PlanetilerConfig config) {
    SourceReader source = remoteSource(CompletableFuture.completedFuture(pathToUrl), mode, refresh, downloader, config);
    process(source, writer, config);
  }

  private void process(SourceReader source, FeatureGroup writer, PlanetilerConfig config) {
    SourceFeatureProcessor.process(sourceName, List.of(source), List.of(), writer, config, profile, stats);
  }

  /**
   * Returns a source that reads the local files listed by {@code paths} once {@code downloaded} completes, to process
   * together with other sources in {@link SourceFeatureProcessor}.
   */
  public SourceReader source(Supplier<List<Path>> paths, CompletableFuture<Void> downloaded, PlanetilerConfig config) {
    // local files open quickly, so the read worker opens them when it gets to this source
    Callable<List<ParquetInputFile>> open = () -> {
      // opens them one after another on this thread, and closes the ones that opened if one fails
      var opening = paths.get().stream()
        .filter(path -> !"_SUCCESS".equals(path.getFileName().toString()))
        .map(path -> CompletableFutures.supplyAsync(() -> newInputFile(path, RangeFile.openLocal(path), config),
          Runnable::run))
        .toList();
      return CompletableFutures.joinAllOrClose(opening);
    };
    Runnable closeUnread = () -> {
    };
    return new Source(downloaded, open, closeUnread, config);
  }

  /**
   * Returns a source that reads local or remote files like {@link #processRemote}, to process together with other
   * sources in {@link SourceFeatureProcessor}.
   * <p>
   * Starts opening the files as soon as {@code pathToUrl} completes, so they are usually open long before a read worker
   * gets to this source.
   */
  public SourceReader remoteSource(CompletableFuture<Map<Path, String>> pathToUrl, DownloadMode mode, boolean refresh,
    Downloader downloader, PlanetilerConfig config) {
    var paths = pathToUrl.thenApply(files -> files.keySet().stream().sorted().toList());
    return remoteSource(paths,
      path -> RangeFile.open(path, pathToUrl.join().get(path), FOOTER_PREFETCH_BYTES, mode, refresh, downloader),
      downloader, config);
  }

  /** Returns a source that opens each of {@code paths} with {@code openFile} on {@code downloader}'s threads. */
  SourceReader remoteSource(CompletableFuture<List<Path>> paths, FunctionThatThrows<Path, RangeFile> openFile,
    Downloader downloader, PlanetilerConfig config) {
    // opening a remote file and reading its footer take a round trip or two each, so open them all at once
    var opening = paths.thenApply(files -> files.stream()
      .map(path -> downloader.async(() -> newInputFile(path, openFile.apply(path), config)))
      .toList());
    var opened = opening.thenCompose(files -> CompletableFuture.allOf(files.toArray(CompletableFuture[]::new)));
    return new Source(
      opened,
      () -> CompletableFutures.joinAllOrClose(opening.join()),
      () -> opening.thenAccept(CompletableFutures::closeWhenDone),
      config);
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

  /** The files of this reader as one source in the shared source pipeline. */
  private class Source implements SourceReader {

    private final CompletableFuture<Void> ready;
    private final Callable<List<ParquetInputFile>> open;
    private final Runnable onClose;
    private final PlanetilerConfig config;
    private final SourceProgress progress = new SourceProgress(sourceName);
    // process workers count these from the iterators of the blocks they work on, with no per-worker state to keep a
    // counter for each thread in
    private final LongAdder featuresRead = new LongAdder();
    private final LongAdder blocksRead = new LongAdder();
    private boolean readStarted = false;
    private List<ParquetInputFile> files = List.of();

    Source(CompletableFuture<Void> ready, Callable<List<ParquetInputFile>> open, Runnable onClose,
      PlanetilerConfig config) {
      this.ready = ready;
      this.open = open;
      this.onClose = onClose;
      this.config = config;
    }

    @Override
    public String name() {
      return sourceName;
    }

    @Override
    public CompletableFuture<Void> whenReady() {
      return ready;
    }

    @Override
    public SourceProgress progress() {
      return progress;
    }

    @Override
    public void read(Consumer<Iterable<? extends SourceFeature>> next) throws Exception {
      readStarted = true;
      files = open.call();
      var inputFiles = files.stream().filter(file -> !file.shouldSkip(profile)).toList();
      // don't show % complete on features when a filter is present because to determine total # elements would
      // take an expensive initial query, and % complete on blocks gives a good enough proxy
      boolean readingWithFilter = inputFiles.stream().anyMatch(ParquetInputFile::hasFilter);
      long featureCount = readingWithFilter ? 0 : inputFiles.stream().mapToLong(ParquetInputFile::getCount).sum();
      long blockCount = inputFiles.stream().mapToLong(ParquetInputFile::getBlockCount).sum();
      List<ParquetInputFile.Block> inputBlocks = new ArrayList<>();
      for (var file : inputFiles) {
        try (var blockReader = file.get()) {
          blockReader.forEach(inputBlocks::add);
        }
      }

      var rangeFiles = inputFiles.stream().map(ParquetInputFile::file).toList();
      // files complete on local disk fetch nothing, and opening a remote one always makes a request
      boolean requiresAnyFetch = rangeFiles.stream().anyMatch(file -> file.requestsMade() > 0);
      var plans = inputFiles.stream().map(ParquetInputFile::planFetch).toList();
      long bytesToFetch = plans.stream().mapToLong(RequestPlanner.Plan::bytes).sum();
      long bytesInFlight = plans.stream().mapToLong(RequestPlanner.Plan::bytesInFlight).max().orElse(0);
      long bytesToRead = inputFiles.stream().mapToLong(ParquetInputFile::getBytesToRead).sum();
      int lookAhead = lookAheadBlocks(blockCount, bytesToRead, bytesInFlight, config.featureProcessThreads());
      long bytesFetchedBefore = rangeFiles.stream().mapToLong(RangeFile::bytesFetched).sum();

      if (readingWithFilter) {
        // use block % complete as a proxy when using a filter since we skipped getting the full count upfront
        progress.setRead(featuresRead::sum, () -> blockCount > 0 ? 1d * blocksRead.sum() / blockCount : Double.NaN);
      } else {
        progress.setRead(featuresRead::sum, featureCount);
      }
      if (requiresAnyFetch) {
        LongSupplier fetched = () -> rangeFiles.stream().mapToLong(RangeFile::bytesFetched).sum() - bytesFetchedBefore;
        progress.setDownload(fetched, () -> bytesToFetch, () -> fetched.getAsLong() >= bytesToFetch);
      }

      // Pass on a block as soon as it's fetched, which may not be in file-order. Reading in file-order would
      // block streaming/cached downloads on a previous slow request
      CompletableFutures.inCompletionOrder(inputBlocks, lookAhead, block -> {
        block.startFetchIfNecessary();
        return block.whenFetched();
      }, block -> next.accept(counted(block)));
    }

    /**
     * Returns {@code block} as a batch that decodes its features, and counts them from the worker that processes it.
     */
    private Iterable<ParquetFeature> counted(ParquetInputFile.Block block) {
      return () -> new Iterator<>() {
        private final Iterator<ParquetFeature> features = block.iterator();
        private boolean done = false;

        @Override
        public boolean hasNext() {
          boolean hasNext = features.hasNext();
          if (!hasNext && !done) {
            done = true;
            blocksRead.increment();
          }
          return hasNext;
        }

        @Override
        public ParquetFeature next() {
          featuresRead.increment();
          return features.next();
        }
      };
    }

    @Override
    public void close() {
      if (!readStarted) {
        onClose.run();
      }
      // process workers read blocks from the files until the pipeline is done, so they stay open until here
      FileUtils.closeAll(files);
    }
  }

  /**
   * Returns how many blocks to fetch ahead of the workers: enough to keep the {@code bytesInFlight} of requests made at
   * once busy twice over (so the next requests start as soon as some finish) and at least one per worker, but no more
   * than 1/8 of the heap.
   */
  static int lookAheadBlocks(long blocks, long bytes, long bytesInFlight, int processThreads) {
    long averageBlockBytes = blocks == 0 ? 1 : Math.max(1, bytes / blocks);
    long wanted = Math.max(processThreads, Math.ceilDiv(2 * bytesInFlight, averageBlockBytes));
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
}
