package com.onthegomap.planetiler.reader.parquet;

import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.collection.FeatureGroup;
import com.onthegomap.planetiler.collection.SortableFeature;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.overture.Struct;
import com.onthegomap.planetiler.render.FeatureRenderer;
import com.onthegomap.planetiler.stats.Counter;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Hashing;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import org.apache.avro.generic.GenericRecord;
import org.apache.parquet.filter2.compat.FilterCompat;
import org.apache.parquet.filter2.predicate.FilterApi;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKBReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroParquetReader {

  private static final Logger LOGGER = LoggerFactory.getLogger(AvroParquetReader.class);
  private final long count = 0;
  private final String sourceName;
  private final Function<Struct, Geometry> geometryParser;
  private final ToLongFunction<GenericRecord> idParser;
  private final Profile profile;
  private final Stats stats;
  private final Function<Path, String> layerParser;

  public AvroParquetReader(
    String sourceName,
    Profile profile,
    Stats stats
  ) {
    this(sourceName, profile, stats, struct -> {
      byte[] bytes = struct.get("geometry").asBytes();
      try {
        return new WKBReader().read(bytes);
      } catch (ParseException e) {
        throw new RuntimeException(e);
      }
    }, genericRecord -> {
      CharSequence idString = (CharSequence) genericRecord.get("id");
      return Hashing.fnv1a64(idString.toString().getBytes(StandardCharsets.UTF_8));
    }, path -> path.toString().replaceAll("^.*theme=([^/]*)/type=([^/]*)/.*$", "$1/$2"));
  }

  public AvroParquetReader(
    String sourceName,
    Profile profile,
    Stats stats,
    Function<Struct, Geometry> getGeometry,
    ToLongFunction<GenericRecord> getId,
    Function<Path, String> getLayer
  ) {
    this.sourceName = sourceName;
    this.layerParser = getLayer;
    this.geometryParser = getGeometry;
    this.idParser = getId;
    this.profile = profile;
    this.stats = stats;
  }

  private static Map<String, Object> toMap(GenericRecord item) {
    Map<String, Object> map = new HashMap<>(item.getSchema().getFields().size());
    for (var field : item.getSchema().getFields()) {
      map.put(field.name(), item.get(field.pos()));
    }
    return map;
  }

  public void process(List<Path> sourcePath, FeatureGroup writer, PlanetilerConfig config) {
    var timer = stats.startStage(sourceName);
    Envelope latLonBounds = config.bounds().latLon();
    FilterCompat.Filter filter = config.bounds().isWorld() ? FilterCompat.NOOP : FilterCompat.get(FilterApi.and(
      FilterApi.and(
        FilterApi.gtEq(FilterApi.doubleColumn("bbox.maxx"), latLonBounds.getMinX()),
        FilterApi.ltEq(FilterApi.doubleColumn("bbox.minx"), latLonBounds.getMaxX())
      ),
      FilterApi.and(
        FilterApi.gtEq(FilterApi.doubleColumn("bbox.maxy"), latLonBounds.getMinY()),
        FilterApi.ltEq(FilterApi.doubleColumn("bbox.miny"), latLonBounds.getMaxY())
      )
    ));
    var inputFiles = sourcePath.stream()
      .filter(d -> !"_SUCCESS".equals(d.getFileName().toString()))
      .map(path -> new ParquetInputFile(path, filter)).toList();
    long featureCount = inputFiles.stream().mapToLong(ParquetInputFile::getCount).sum();
    long blockCount = inputFiles.stream().mapToLong(ParquetInputFile::getBlockCount).sum();
    int readThreads = config.featureReadThreads();
    int processThreads = config.featureProcessThreads();
    int writeThreads = config.featureWriteThreads();
    var blocksRead = Counter.newMultiThreadCounter();
    var featuresRead = Counter.newMultiThreadCounter();
    var featuresWritten = Counter.newMultiThreadCounter();
    Map<String, Integer> workingOn = new ConcurrentHashMap<>();

    var pipeline = WorkerPipeline.start(sourceName, stats)
      .readFromTiny("inputFiles", inputFiles).<ParquetInputFile.Block>addWorker("read", readThreads, (prev, next) -> {
        for (var file : prev) {
          try (var blockReader = file.get()) {
            for (var block : blockReader) {
              next.accept(block);
            }
          }
        }
      })
      .addBuffer("row_groups", 10)
      .<SortableFeature>addWorker("process", processThreads, (prev, next) -> {
        var blocks = blocksRead.counterForThread();
        var elements = featuresRead.counterForThread();
        var featureCollectors = new FeatureCollector.Factory(config, stats);
        try (FeatureRenderer renderer = newFeatureRenderer(writer, config, next)) {
          for (var block : prev) {
            String layer = layerParser.apply(block.getFileName());
            workingOn.merge(layer, 1, Integer::sum);
            for (var item : block) {
              if (item != null) {
                var sourceFeature = new AvroParquetFeature(
                  item,
                  sourceName,
                  layer,
                  block.getFileName(),
                  idParser.applyAsLong(item),
                  geometryParser,
                  toMap(item)
                );
                FeatureCollector features = featureCollectors.get(sourceFeature);
                try {
                  profile.processFeature(sourceFeature, features);
                  for (FeatureCollector.Feature renderable : features) {
                    renderer.accept(renderable);
                  }
                } catch (Exception e) {
                  e.printStackTrace();
                  LOGGER.error("Error processing {}", sourceFeature, e);
                }
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
}
