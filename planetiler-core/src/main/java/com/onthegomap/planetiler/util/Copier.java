package com.onthegomap.planetiler.util;

import ar.com.hjg.pngj.IImageLine;
import ar.com.hjg.pngj.ImageLineInt;
import ar.com.hjg.pngj.PngReader;
import ar.com.hjg.pngj.PngWriter;
import ar.com.hjg.pngj.chunks.ChunkCopyBehaviour;
import com.luciad.imageio.webp.WebP;
import com.luciad.imageio.webp.WebPEncoderOptions;
import com.luciad.imageio.webp.WebPWriteParam;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Timer;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.awt.image.BufferedImage;
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

  // level=0 1m17s 4.6G
  // level=1
  // level=2 1m11s 107m
  // level=3
  // level=4 1m24s 83m
  // level=5
  // level=6 1m35s 77m

  public static void main(String[] args) throws IOException {
    var arguments = Arguments.fromEnvOrArgs(args).orElse(Arguments.of(Map.of(
      "minzoom", "0",
      "maxzoom", "10",
      "download-max-bandwidth", "300MB/s",
      "download-threads", "50"
    )));
    var timer = Timer.start();
    var stats = arguments.getStats();
    var config = PlanetilerConfig.from(arguments);
    var from = arguments.getString("source", "source",
      "https://elevation-tiles-prod.s3.amazonaws.com/terrarium/{z}/{x}/{y}.png");
    var to = arguments.file("dest", "output.mbtiles", Path.of("output.mbtiles"));
    var webpQuality = arguments.getDouble("webp-quality", "image encode quality", 0);
    var webpMethod = arguments.getInteger("webp-method", "image encode method", 2);
    var format = arguments.getString("format", "format (webp/png)", "png");
    var pngLevel = arguments.getInteger("level", "png gzip level", -1);
    var bathymetry = arguments.getBoolean("bathymetry", "include bathymetry", false);
    var lossless = arguments.getBoolean("lossless", "lossless compression", true);
    FileUtils.delete(to);
    var bounds = config.bounds();
    try (
      var in = Mbtiles
        .newReadOnlyDatabase(Path.of("terrarium-z11-512.mbtiles"));
      var out = Mbtiles.newWriteToFileDatabase(to, config.compactDb())
    ) {
      out.createTablesWithIndexes();
      out.metadata()
        .setName("ne2sr")
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
          try (var iter = in.getAllTileCoords()) { //out.tileOrder().enumerate(config.minzoom(), config.maxzoom(), bounds);
            Semaphore s = new Semaphore(config.downloadThreads());
            while (iter.hasNext()) {
              s.acquire();
              var coord = iter.next();
              var item = new Item(coord, CompletableFuture.completedFuture(in.getTile(coord.x(), coord.y(), coord.z()))
                .thenComposeAsync(result -> {
                  if (result == null)
                    return CompletableFuture.completedFuture(null);
                  try {
                    byteCounter.addAndGet(result.length);
                    return CompletableFuture
                      .completedFuture(
                        "webp".equals(format) ? pngToWebp2(result, webpQuality, lossless, webpMethod) :
                          stripBlue(result, pngLevel, bathymetry));
                  } catch (Exception e) {
                    return CompletableFuture.failedFuture(e);
                  }
                }));
              item.bytes.whenComplete((ab, bb) -> s.release());
              next.accept(item);
            }
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
                  writer.write(
                    new TileEncodingResult(item.coord, item.bytes.get(), OptionalLong.of(Hashing.fnv1a64(bytes))));
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
      LOGGER.info("{}", timer.stop());
    }
  }

  private static byte[] encodeAsPng(BufferedImage result) throws IOException {
    var baos = new ByteArrayOutputStream();
    ImageIO.write(result, "png", baos);
    return baos.toByteArray();
  }

  private static byte[] pngToWebp2(byte[] bytes, double quality, boolean lossless, int method) {
    PngReader pngr = new PngReader(new ByteArrayInputStream(bytes));
    byte[] imageData = new byte[3 * pngr.imgInfo.cols * pngr.imgInfo.rows];
    int i = 0;
    while (pngr.hasMoreRows()) {
      IImageLine l1 = pngr.readRow();
      int[] scanline = ((ImageLineInt) l1).getScanline();
      for (int c = 0; c < scanline.length; c += 3) {
        imageData[i++] = (byte) scanline[c];
        imageData[i++] = (byte) scanline[c + 1];
        imageData[i++] = 0;
      }
    }
    pngr.end();
    WebPEncoderOptions p = new WebPEncoderOptions();
    p.setLossless(lossless);
    p.setCompressionQuality((float) quality);
    p.setMethod(method);
    return WebP.encodeRGB(p, imageData, pngr.imgInfo.cols, pngr.imgInfo.rows, pngr.imgInfo.rows * 3);
  }

  private static byte[] pngToWebp(byte[] success, double quality, boolean lossless) throws IOException {
    var image = ImageIO.read(new ByteArrayInputStream(success));
    return encodeAsWebp(image, quality, lossless);
  }

  private static byte[] stripBlue(byte[] bytes, int level, boolean bathymetry) {
    boolean allLow = true;
    PngReader pngr = new PngReader(new ByteArrayInputStream(bytes));
    var baos = new ByteArrayOutputStream();
    PngWriter pngw = new PngWriter(baos, pngr.imgInfo);
    pngw.setCompLevel(level);
    pngw.copyChunksFrom(pngr.getChunksList(), ChunkCopyBehaviour.COPY_ALL_SAFE);
    while (pngr.hasMoreRows()) {
      IImageLine l1 = pngr.readRow();
      int[] scanline = ((ImageLineInt) l1).getScanline();
      for (int j = 0; j < pngr.imgInfo.cols; j++) {
        int red = scanline[j * 3];
        int green = scanline[j * 3 + 1];
        int elevation = (red * 256 + green) - 32768;
        if (elevation < -512) {
          scanline[j * 3] = 0;
          scanline[j * 3 + 1] = 0;
        } else {
          allLow = false;
        }
        scanline[j * 3 + 2] = 0;
      }
      pngw.writeRow(l1);
    }
    pngr.end();
    if (allLow && !bathymetry) {
      return null;
    }
    pngw.end();
    return baos.toByteArray();
  }

  private static byte[] encodeAsWebp(BufferedImage image, double quality, boolean lossless) throws IOException {
    ImageWriter writer = ImageIO.getImageWritersByMIMEType("image/webp").next();
    WebPWriteParam writeParam = new WebPWriteParam(writer.getLocale());
    writeParam.setCompressionMode(ImageWriteParam.MODE_EXPLICIT);
    writeParam.setCompressionType(writeParam.getCompressionTypes()[lossless ? WebPWriteParam.LOSSLESS_COMPRESSION :
      WebPWriteParam.LOSSY_COMPRESSION]);
    writeParam.setCompressionQuality((float) quality);
    var baos = new ByteArrayOutputStream();
    try (var c = new MemoryCacheImageOutputStream(baos)) {
      writer.setOutput(c);
      writer.write(null, new IIOImage(image, null, null), writeParam);
    }
    return baos.toByteArray();
  }
}
// 0.1 211M 2m19
// 0.25 205M 3m2s
// 0.5 192M 3m37s
// 1 191M 13m29s
