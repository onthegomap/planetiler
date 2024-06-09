package com.onthegomap.planetiler.reader.parquet;

import static io.prometheus.client.Collector.NANOSECONDS_PER_SECOND;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.collection.SortableFeature;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.render.FeatureRenderer;
import com.onthegomap.planetiler.stats.Counter;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
    var timer = stats.startStage(sourceName);
    var inputFiles = sourcePath.stream()
      .filter(d -> !"_SUCCESS".equals(d.getFileName().toString()))
      .map(path -> {
        var hivePartitionFields = hivePartitioning ? getHivePartitionFields(path) : null;
        String layer = getLayerName(path);
        return new ParquetInputFile(sourceName, layer, path, null, config.bounds(), hivePartitionFields, idGenerator);
      })
      .filter(file -> !file.shouldSkip(profile))
      .toList();
    // don't show % complete on features when a filter is present because to determine total # elements would
    // take an expensive initial query, and % complete on blocks gives a good enough proxy
    long featureCount = inputFiles.stream().anyMatch(ParquetInputFile::hasFilter) ? 0 :
      inputFiles.stream().mapToLong(ParquetInputFile::getCount).sum();
    long blockCount = inputFiles.stream().mapToLong(ParquetInputFile::getBlockCount).sum();
    int processThreads = config.featureProcessThreads();
    int writeThreads = config.featureWriteThreads();
    var blocksRead = Counter.newMultiThreadCounter();
    var featuresRead = Counter.newMultiThreadCounter();
    var featuresWritten = Counter.newMultiThreadCounter();
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

    var pipeline = WorkerPipeline.start(sourceName, stats)
      .readFromTiny("blocks", inputBlocks)
      .<SortableFeature>addWorker("process", processThreads, (prev, next) -> {
        var blocks = blocksRead.counterForThread();
        var elements = featuresRead.counterForThread();
        var featureCollectors = new FeatureCollector.Factory(config, stats);
        try (FeatureRenderer renderer = newFeatureRenderer(writer, config, next)) {
          for (var block : prev) {
            String layer = block.layer();
            workingOn.merge(layer, 1, Integer::sum);
            for (var sourceFeature : block) {
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
            }
            blocks.inc();
            workingOn.merge(layer, -1, Integer::sum);
          }
        }
      })
      .addBuffer("write_queue", 50_000, 1_000)
      .sinkTo("write", writeThreads, prev -> {
        var features = featuresWritten.counterForThread();
        try (var threadLocalWriter = writer.writerForThread()) {
          for (var item : prev) {
            features.inc();
            threadLocalWriter.accept(item);
          }
        }
      });

    var loggers = ProgressLoggers.create()
      .addRatePercentCounter("read", featureCount, featuresRead, true)
      .addRatePercentCounter("blocks", blockCount, blocksRead, false)
      .addRateCounter("write", featuresWritten)
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
      LOGGER.info("Processed {} parquet features ({}/s, {} blocks, {} files) in {}",
        format.integer(count),
        format.numeric(count * NANOSECONDS_PER_SECOND / elapsed.wall().toNanos()),
        format.integer(blocksRead.get()),
        format.integer(inputFiles.size()),
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
