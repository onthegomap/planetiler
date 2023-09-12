package com.onthegomap.planetiler.util;

import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;

import com.google.common.collect.Multiset;
import com.google.common.collect.Multisets;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;
import java.util.HashMap;
import java.util.Map;
import java.util.PriorityQueue;
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
    LocalDate date = LocalDate.now(ZoneOffset.UTC);
    int days = arguments.getInteger("days", "number of days into the past to look", 90);
    int maxZoom = arguments.getInteger("maxzoom", "max zoom", 15);
    int topN = arguments.getInteger("top", "top n", 1_000_000);
    Path output = arguments.file("output", "output", Path.of("top_tiles.tsv.gz"));

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
      .readFromTiny("urls", toDownload)
      .<byte[]>addWorker("download", 10, (prev, next) -> {
        for (var file : prev) {
          try (var stream = downloader.openStream(file)) {
            next.accept(stream.readAllBytes());
          } catch (IOException e) {
            LOGGER.warn("Error downloading {} {}", file, e);
          }
        }
      })
      .addBuffer("files", 30).<Map.Entry<Integer, Long>>addWorker("parse", arguments.threads(),
        (prev, next) -> {
          for (var bytes : prev) {
            try (
              var inputStream = new XZInputStream(new ByteArrayInputStream(bytes));
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
            }
          }
        })
      .addBuffer("lines", 100_000, 1_000)
      .sinkTo("collect", 1, lines -> {
        Map<Integer, Long> counts = new HashMap<>();
        Multiset m;
        Multisets.
        for (var line : lines) {
          counts.merge(line.getKey(), line.getValue(), Long::sum);
        }
        PriorityQueue<Long> top = new PriorityQueue<>(topN);
        LOGGER.info("Extracting top {} tiles from {} tiles", topN, counts.size());
        for (var cursor : counts.entrySet()) {
          top.offer(cursor.getValue());
          if (top.size() > topN) {
            top.poll();
          }
        }
        Long cutoff = top.poll();
        try (
          var ouput = new GZIPOutputStream(
            new BufferedOutputStream(Files.newOutputStream(output, CREATE, TRUNCATE_EXISTING, WRITE)));
          var writer = new BufferedWriter(new OutputStreamWriter(ouput))
        ) {
          writer.write("z\tx\ty\tcount\n");
          for (var cursor : counts.entrySet()) {
            if (cursor.getValue() >= cutoff) {
              TileCoord coord = TileCoord.hilbertDecode(cursor.getKey());
              writer.write("%d\t%d\t%d\t%d%n".formatted(coord.z(), coord.x(), coord.y(), cursor.getValue()));
            }
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

  }
}
