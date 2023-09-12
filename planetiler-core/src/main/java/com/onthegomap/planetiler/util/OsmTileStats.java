package com.onthegomap.planetiler.util;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.regex.Pattern;
import java.util.stream.IntStream;
import java.util.zip.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.tukaani.xz.XZInputStream;

public class OsmTileStats {
  private static final Logger LOGGER = LoggerFactory.getLogger(OsmTileStats.class);

  public static void main(String[] args) {
    Arguments arguments = Arguments.fromArgsOrConfigFile(args);
    PlanetilerConfig config = PlanetilerConfig.from(arguments);
    var stats = arguments.getStats();
    var timer = stats.startStage("osm-tile-stats");
    LocalDate date = LocalDate.now(ZoneOffset.UTC);
    int days = arguments.getInteger("days", "number of days into the past to look", 90);
    int maxZoom = arguments.getInteger("maxzoom", "max zoom", 15);
    int topN = arguments.getInteger("top", "top n", 10_000_000);
    Path output = arguments.file("output", "output", Path.of("top_tiles.tsv.gz"));
    int threads = arguments.getInteger("download-threads", "number of threads to use for downloading",
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

    var pipeline = WorkerPipeline.start("osm-tile-stats", stats)
      .readFromTiny("urls", toDownload).<Map.Entry<Integer, Long>>addWorker("parse", arguments.threads(),
        (prev, next) -> {
          for (var url : prev) {
            try (
              var inputStream = new XZInputStream(new BufferedInputStream(downloader.openStream(url)));
              var reader = new BufferedReader(new InputStreamReader(inputStream));
            ) {
              String line;
              while ((line = reader.readLine()) != null) {
                String[] parts = splitter.split(line);
                if (parts.length == 4) {
                  int z = Integer.parseInt(parts[0]);
                  if (z <= maxZoom) {
                    int x = Integer.parseInt(parts[1]);
                    int y = Integer.parseInt(parts[2]);
                    long loads = Long.parseLong(parts[3]);
                    next.accept(Map.entry(TileCoord.ofXYZ(x, y, z).hilbertEncoded(), loads));
                  }
                }
              }
              downloaded.incrementAndGet();
            } catch (IOException e) {
              LOGGER.warn("Error getting file {} {}", url, e);
            }
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
          .sorted(Comparator.<Map.Entry<Integer, Long>>comparingLong(Map.Entry::getValue).reversed())
          .limit(topN)
          .sorted(Comparator.comparingInt(Map.Entry::getKey))
          .toList();
        try (
          var ouput = new GZIPOutputStream(
            new BufferedOutputStream(Files.newOutputStream(output, CREATE, TRUNCATE_EXISTING, WRITE)));
          var writer = new BufferedWriter(new OutputStreamWriter(ouput))
        ) {
          writer.write("z\tx\ty\tcount\n");
          for (var entry : topCounts) {
            TileCoord coord = TileCoord.hilbertDecode(entry.getKey());
            writer.write("%d\t%d\t%d\t%d%n".formatted(coord.z(), coord.x(), coord.y(), entry.getValue()));
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
}
