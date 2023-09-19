package com.onthegomap.planetiler.util;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.databind.ObjectReader;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.google.common.io.LineReader;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tukaani.xz.XZInputStream;

public class TopOsmTiles {
  private static final String DOWLOAD_URL =
    "https://raw.githubusercontent.com/onthegomap/planetiler/main/top_osm_tiles.tsv.gz";
  private static final CsvMapper MAPPER = new CsvMapper();
  private static final CsvSchema SCHEMA = MAPPER
    .schemaFor(Row.class)
    .withHeader()
    .withColumnSeparator('\t')
    .withLineSeparator("\n");
  public static final ObjectWriter WRITER = MAPPER.writer(SCHEMA);
  public static final ObjectReader READER = MAPPER.readerFor(Row.class).with(SCHEMA);
  private static final Logger LOGGER = LoggerFactory.getLogger(TopOsmTiles.class);

  public static void main(String[] args) {
    Arguments arguments = Arguments.fromArgsOrConfigFile(args).orElse(Arguments.of(Map.of(
      "http-retries", "3"
    )));
    PlanetilerConfig config = PlanetilerConfig.from(arguments);
    var stats = arguments.getStats();
    var timer = stats.startStage("top-osm-tiles");
    LocalDate date = LocalDate.now(ZoneOffset.UTC);
    int days = arguments.getInteger("days", "number of days into the past to look", 90);
    int maxZoom = arguments.getInteger("maxzoom", "max zoom", 15);
    int topN = arguments.getInteger("top", "top n", 1_000_000);
    Path output = arguments.file("output", "output", Path.of("top_osm_tiles.tsv.gz"));
    int threads = arguments.getInteger("download-threads", "number of threads to use for downloading/parsing",
      Math.min(10, arguments.threads()));

    var toDownload = IntStream.range(0, days)
      .mapToObj(i -> date.minus(Period.ofDays(i)))
      .map(d -> "https://planet.openstreetmap.org/tile_logs/tiles-%4d-%02d-%02d.txt.xz".formatted(
        d.getYear(),
        d.getMonthValue(),
        d.getDayOfMonth()
      )).toList();
    var downloader = Downloader.create(config, stats);
    var splitter = Pattern.compile("[/ ]");
    AtomicLong downloaded = new AtomicLong();

    var pipeline = WorkerPipeline.start("top-osm-tiles", stats)
      .readFromTiny("urls", toDownload).<Map.Entry<Integer, Long>>addWorker("download", threads,
        (prev, next) -> {
          for (var url : prev) {
            for (int i = 0; i <= config.httpRetries(); i++) {
              List<Map.Entry<Integer, Long>> result = new ArrayList<>();
              try (
                var inputStream = new XZInputStream(new BufferedInputStream(downloader.openStream(url)));
                var reader = new InputStreamReader(inputStream);
              ) {
                LineReader lines = new LineReader(reader);
                String line;
                while ((line = lines.readLine()) != null) {
                  String[] parts = splitter.split(line);
                  if (parts.length == 4) {
                    // adjust osm tiles (256x256px) to vector (512x512px) by moving up one zoom level
                    int z = Integer.parseInt(parts[0]) - 1;
                    if (z >= 0 && z <= maxZoom) {
                      int x = Integer.parseInt(parts[1]) >> 1;
                      int y = Integer.parseInt(parts[2]) >> 1;
                      long loads = Long.parseLong(parts[3]);
                      result.add(Map.entry(TileCoord.ofXYZ(x, y, z).encoded(), loads));
                    }
                  }
                }
                result.forEach(next);
                break;
              } catch (FileNotFoundException e) {
                LOGGER.info("No data for {}", url);
                break;
              } catch (IOException e) {
                if (i == config.httpRetries()) {
                  LOGGER.warn("Failed getting {} {}", url, e);
                }
              }
            }
            downloaded.incrementAndGet();
          }
        })
      .addBuffer("lines", 100_000, 1_000)
      .sinkTo("collect", 1, lines -> {
        Map<Integer, Long> counts = new HashMap<>();
        for (var line : lines) {
          counts.merge(line.getKey(), line.getValue(), Long::sum);
        }
        LOGGER.info("Extracting top {} tiles from {} tiles", topN, counts.size());
        var topCounts = counts.entrySet().stream()
          .sorted(Comparator.comparingLong(e -> -e.getValue()))
          .limit(topN)
          .sorted(Comparator.comparingInt(Map.Entry::getKey))
          .toList();
        try (
          var ouput = new GZIPOutputStream(
            new BufferedOutputStream(Files.newOutputStream(output, CREATE, TRUNCATE_EXISTING, WRITE)));
          var writer = WRITER.writeValues(ouput)
        ) {
          for (var entry : topCounts) {
            TileCoord coord = TileCoord.decode(entry.getKey());
            writer.write(new Row(coord.z(), coord.x(), coord.y(), entry.getValue()));
          }
        }
      });

    ProgressLoggers progress = ProgressLoggers.create()
      .addPercentCounter("files", toDownload.size(), downloaded)
      .newLine()
      .addPipelineStats(pipeline)
      .newLine()
      .addProcessStats();

    pipeline.awaitAndLog(progress, config.logInterval());
    timer.stop();
    stats.printSummary();
  }

  public static TileWeights loadFromFile(Path path) {
    TileWeights result = new TileWeights();
    try (
      var input = new GZIPInputStream(new BufferedInputStream(Files.newInputStream(path)));
      var reader = READER.<Row>readValues(input)
    ) {
      while (reader.hasNext()) {
        var row = reader.next();
        result.put(TileCoord.ofXYZ(row.x(), row.y(), row.z()), row.loads());
      }
    } catch (IOException e) {
      LOGGER.warn("Unable to load tile weights from {}, will fall back to unweighted average: {}", path, e);
      return new TileWeights();
    }
    return result;
  }

  public static void download(PlanetilerConfig config, Stats stats) {
    if (!Files.exists(config.tileWeights())) {
      Downloader.create(config, stats)
        .downloadIfNecessary(new Downloader.ResourceToDownload("osm-tile-weights", DOWLOAD_URL, config.tileWeights()));
    }
  }

  @JsonPropertyOrder({"z", "x", "y", "loads"})
  record Row(int z, int x, int y, long loads) {}
}
