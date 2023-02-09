package com.onthegomap.planetiler.util;

import com.luciad.imageio.webp.WebPWriteParam;
import com.onthegomap.planetiler.archive.HttpArchive;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Semaphore;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import javax.imageio.IIOImage;
import javax.imageio.ImageIO;
import javax.imageio.ImageWriteParam;
import javax.imageio.ImageWriter;
import javax.imageio.stream.MemoryCacheImageOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Copier {
  private static final Logger LOGGER = LoggerFactory.getLogger(Copier.class);

  private record Item(
    TileCoord coord,
    CompletableFuture<byte[]> bytes
  ) {

    Item {
      Objects.requireNonNull(coord);
    }

    public Item(TileCoord coord) {
      this(coord, new CompletableFuture<>());
    }
  }

  public static void main(String[] args) throws IOException {
    var arguments = Arguments.fromEnvOrArgs(args).orElse(Arguments.of(Map.of(
      "minzoom", "0",
      "maxzoom", "12",
      "download-max-bandwidth", "500MB/s",
      "download-threads", "50"
    )));
    var stats = arguments.getStats();
    var config = PlanetilerConfig.from(arguments);
    var from = arguments.getString("source", "source",
      "https://elevation-tiles-prod.s3.amazonaws.com/v2/terrarium/{z}/{x}/{y}.png");
    var to = arguments.file("dest", "output.mbtiles", Path.of("output.mbtiles"));
    var format = arguments.getString("format", "format (webp/png)", "png");
    FileUtils.delete(to);
    var bounds = config.bounds();
    try (
      var in = new HttpArchive(from, config);
      var out = Mbtiles.newWriteToFileDatabase(to, config.compactDb())
    ) {
      out.createTablesWithIndexes();
      out.metadata()
        .setName("elevation")
        .setFormat(format)
        .setType("baselayer")
        .setBoundsAndCenter(config.bounds().latLon())
        .setMinzoom(config.minzoom())
        .setMaxzoom(config.maxzoom())
        .setJson(new Mbtiles.MetadataJson());

      var counter = new AtomicLong(0);
      var byteCounter = new AtomicLong(0);
      var lastTile = new AtomicReference<>("");
      var pipeline = WorkerPipeline.start("copy", stats)
        .<Item>fromGenerator("enumerate", next -> {
          var iter = out.tileOrder().enumerate(config.minzoom(), config.maxzoom(), bounds);
          Semaphore s = new Semaphore(config.downloadThreads());
          while (iter.hasNext()) {
            s.acquire();
            var coord = iter.next();
            var item = new Item(coord, in.getTileFuture(coord.x(), coord.y(), coord.z())
              .thenComposeAsync(bytes -> {
                byteCounter.addAndGet(bytes.length);
                try {
                  return CompletableFuture.completedFuture("webp".equals(format) ? pngToWebp(bytes) : bytes);
                } catch (Exception t) {
                  return CompletableFuture.failedFuture(t);
                }
              }));
            item.bytes.whenComplete((a, b) -> s.release());
            next.accept(item);
          }
        }, 1)
        .addBuffer("encoded", 10_000, 1)
        .sinkTo("write", 1, prev -> {
          try (var writer = out.newTileWriter()) {
            for (var item : prev) {
              counter.incrementAndGet();
              lastTile.set(item.coord.getDebugUrl());
              try {
                var bytes = item.bytes.get();
                if (bytes != null && bytes.length > 0) {
                  writer.write(new TileEncodingResult(item.coord, item.bytes.get(), OptionalLong.empty()));
                }
              } catch (ExecutionException e) {
                LOGGER.warn("Error getting {} : {}", item.coord, e.getCause().toString());
              }
            }
          }
        });

      int total = 0;
      for (int z = config.minzoom(); z <= config.maxzoom(); z++) {
        total += 1 << (2 * z);
      }

      var loggers = ProgressLoggers.create()
        .addRatePercentCounter("tiles", total, counter, false)
        .addRateCounter("bytes", byteCounter)
        .addFileSize(to)
        .newLine()
        .addProcessStats()
        .newLine()
        .addPipelineStats(pipeline)
        .newLine()
        .add(lastTile::get);

      loggers.awaitAndLog(pipeline.done(), config.logInterval());
    }
  }

  private static byte[] pngToWebp(byte[] success) throws IOException {
    var image = ImageIO.read(new ByteArrayInputStream(success));
    ImageWriter writer = ImageIO.getImageWritersByMIMEType("image/webp").next();
    WebPWriteParam writeParam = new WebPWriteParam(writer.getLocale());
    writeParam.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
    writeParam.setCompressionType(
      writeParam.getCompressionTypes()[WebPWriteParam.LOSSLESS_COMPRESSION]);
    //                  writeParam.setCompressiolsnQuality(6);
    var baos = new ByteArrayOutputStream();
    try (var c = new MemoryCacheImageOutputStream(baos)) {
      writer.setOutput(c);
      writer.write(null, new IIOImage(image, null, null), writeParam);
    }
    return baos.toByteArray();
  }
}
