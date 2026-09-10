package com.onthegomap.planetiler.util;

import static com.onthegomap.planetiler.worker.Worker.joinFutures;

import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.pmtiles.Pmtiles;
import com.onthegomap.planetiler.pmtiles.RangeReader;
import com.onthegomap.planetiler.pmtiles.ReadablePmtiles;
import com.onthegomap.planetiler.pmtiles.WriteablePmtiles;
import com.onthegomap.planetiler.stats.ProgressLoggers;
import com.onthegomap.planetiler.stats.Timer;
import com.onthegomap.planetiler.worker.WorkQueue;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import dev.matrixlab.webp4j.WebPCodec;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Mapterhorn {
  private static final Logger LOGGER = LoggerFactory.getLogger(Mapterhorn.class);

  public static void main(String[] args) throws Exception {
    var arguments = Arguments.fromArgs(args).withDefault("maxzoom", "3");
    var config = PlanetilerConfig.from(arguments);
    var input =
      arguments.getString("input", "url of the input archive", "https://download.mapterhorn.com/planet.pmtiles");
    var output = arguments.file("output", "what file to write to", Path.of("output.pmtiles"));
    int maxZoom = config.maxzoom();
    int maxGroupSize = arguments.getInteger("maxgroupsize", "max zoom size in mb", 64) * 1_000_000;
    int maxGap = arguments.getInteger("maxgap", "max gap in kb", 1000) * 1_000;
    int downloadConcurrency = arguments.getInteger("downloaders", "download threads", 10);

    FileUtils.delete(output);

    Timer timer = Timer.start();
    var reader = new RangeReader.FromUrl(input, config);
    var pmtiles = new ReadablePmtiles(reader);
    var header = pmtiles.getHeader();
    List<Pmtiles.Entry> rootDir =
      pmtiles.readDir(pmtiles.getHeader().rootDirOffset(), (int) pmtiles.getHeader().rootDirLength());
    LOGGER.info("Fetching all tile coordinates <= z{}...", maxZoom);
    List<Pmtiles.Entry> tileCoords = pmtiles.getTileLocations(rootDir, maxZoom).toList();
    LOGGER.info("Fetched {} tile coordinates", tileCoords.size());

    record TileData(long offset, int length) {}
    List<TileData> tiles =
      tileCoords.stream().map(it -> new TileData(it.offset(), it.length()))
        .sorted(Comparator.comparing(TileData::offset))
        .distinct()
        .toList();
    LOGGER.info("Sorted {} unique raw tiles", tiles.size());

    List<List<TileData>> grouped = new ArrayList<>();
    List<TileData> currentGroup = null;
    var first = tiles.stream().min(Comparator.comparing(TileData::offset)).get();
    var lastEntry = tiles.stream().max(Comparator.comparing(TileData::offset)).get();
    long totalBytes = lastEntry.offset + lastEntry.length - first.offset;
    long groupStart = tiles.getFirst().offset;
    long last = groupStart;
    for (var tile : tiles) {
      if (currentGroup == null) {
        grouped.add(currentGroup = new ArrayList<>());
        groupStart = last = tile.offset;
      }
      currentGroup.add(tile);
      if (tile.offset > groupStart + maxGroupSize || tile.offset > last + maxGap) {
        currentGroup = null;
      }
      last = tile.offset;
    }

    LOGGER.info("Grouped into {} <{}mb tile groups", grouped.size(), maxGroupSize / 1_000_000);

    // process those contiguous ranges
    // convert output tiles and write to output file
    // then finalize the archive

    AtomicLong written = new AtomicLong();
    AtomicLong bytesRead = new AtomicLong();
    record PendingTile(long offset, byte[] bytes, CompletableFuture<byte[]> result) {}
    WorkQueue<PendingTile> writerQueue = new WorkQueue<>("write_queue", 10_000, 1, arguments.getStats());

    AtomicLong remaining = new AtomicLong(downloadConcurrency);

    var pipeline = WorkerPipeline.start("process", arguments.getStats());
    var readerBranch = pipeline
      .readFromTiny("groups", grouped)
      .<PendingTile>addWorker("download", downloadConcurrency, (prev, next) -> {
        var writerEnqueuer = writerQueue.threadLocalWriter();
        for (var group : prev) {
          long start = group.getFirst().offset;
          int length = (int) (group.getLast().offset + group.getLast().length() - start);
          byte[] segmentBytes = new byte[length];
          for (int i = 1; i <= config.httpRetries(); i++) {
            try (var stream = reader.startReading(header.tileDataOffset() + start, length)) {
              IOUtils.readFully(stream, segmentBytes);
              bytesRead.addAndGet(length);
              break;
            } catch (IOException e) {
              LOGGER.error("Error reading, retry {} {}", i, e);
              Thread.sleep(config.httpRetryWait());
            }
          }
          for (var item : group) {
            int from = Math.toIntExact(item.offset - start);
            int to = from + item.length;
            byte[] bytes = Arrays.copyOfRange(segmentBytes, from, to);
            if (bytes.length != item.length) {
              throw new IllegalStateException("Read " + bytes.length + " wanted " + item.length);
            }
            var result = new PendingTile(item.offset, bytes, new CompletableFuture<>());
            next.accept(result);
            writerEnqueuer.accept(result);
          }
        }
        if (remaining.decrementAndGet() <= 0) {
          writerQueue.close();
        }
      }).addBuffer("to_convert", 1000, 1)
      .sinkTo("convert", arguments.threads(), (prev) -> {
        for (var item : prev) {
          item.result().complete(convertWebpToPng(item.bytes, 1));
        }
      });
    var writerBranch = pipeline.readFromQueue(writerQueue)
      .sinkTo("write", 1, prev -> {
        try (var outfile = WriteablePmtiles.newWriteToFile(output)) {
          outfile.initialize();
          var writer = outfile.newManualWriter();
          record OffsetAndLength(long offset, int length) {}
          Map<Long, OffsetAndLength> oldOffsetToNew = new HashMap<>();
          for (var tile : prev) {
            var outBytes = tile.result.get();
            long newOffset = writer.writeData(outBytes);
            oldOffsetToNew.put(tile.offset, new OffsetAndLength(newOffset, outBytes.length));
            written.incrementAndGet();
          }
          for (var tile : tileCoords) {
            var newLoc = oldOffsetToNew.get(tile.offset());
            writer.writeEntry(newLoc.offset, tile.tileId(), newLoc.length, tile.runLength());
          }
          var oldMetadata = pmtiles.metadata();
          outfile.finish(new TileArchiveMetadata(
            oldMetadata.name(),
            oldMetadata.description(),
            oldMetadata.attribution(),
            oldMetadata.version(),
            oldMetadata.type(),
            oldMetadata.format(),
            oldMetadata.bounds(),
            oldMetadata.center(),
            oldMetadata.minzoom(),
            maxZoom,
            oldMetadata.json(),
            oldMetadata.others(),
            oldMetadata.tileCompression()
          ));
        }
      });

    ProgressLoggers loggers = ProgressLoggers.create()
      .addRatePercentCounter("tiles", tiles.size(), written, true)
      .addStorageRatePercentCounter("bytes", totalBytes, bytesRead::longValue, true)
      .add(" pmtiles")
      .addFileSize(output)
      .newLine()
      .addProcessStats()
      .newLine()
      .addPipelineStats(readerBranch)
      .addPipelineStats(writerBranch);

    var doneFuture = joinFutures(readerBranch.done(), writerBranch.done());
    loggers.awaitAndLog(doneFuture, config.logInterval());

    LOGGER.info("Finished in {} {}", timer.stop(), Files.size(output));
  }

  public static byte[] convertWebpToPng(byte[] webpBytes, int resolution) throws IOException {
    if (!WebPCodec.isAvailable()) {
      throw new RuntimeException("Not available");
    }
    var image = WebPCodec.decodeImage(webpBytes);
    var raster = image.getRaster();
    int halfRes = resolution / 2;
    for (int y = 0; y < raster.getHeight(); y++) {
      for (int x = 0; x < raster.getWidth(); x++) {
        if (resolution > 1) {
          int green = raster.getSample(x, y, 1);
          raster.setSample(x, y, 1, (green + halfRes) / resolution * resolution);
        }
        raster.setSample(x, y, 2, 0);
      }
    }

    return WebPCodec.encodeImage(image, 0f, true, false);

    //    WebPImage image = WebPImage.read(new ByteArrayInputStream(webpBytes));
    //
    //    int width = image.getWidth();
    //    int height = image.getHeight();
    //    WebPFrame raster = image.getFirstFrame();
    //    var pixels = raster.getArgbArray();
    //
    //    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    //    ImageInfo imgInfo = new ImageInfo(width, height, 8, false); // RGB, no alpha
    //    PngWriter pngWriter = new PngWriter(baos, imgInfo);
    //    pngWriter.setCompLevel(6);
    //
    //    int offset = 0;
    //
    //    for (int y = 0; y < height; y++) {
    //      ImageLineInt line = new ImageLineInt(imgInfo);
    //      int[] scanline = line.getScanline();
    //      int base = 0;
    //
    //      for (int x = 0; x < width; x++) {
    //        int argb = pixels[offset];
    //        scanline[base] = (argb >>> 16) & 0xFF;
    //        scanline[base + 1] = (argb >>> 8) & 0xFF;
    //        //        scanline[base + 2] = samples[2]; // B
    //        offset += 1;
    //        base += 3;
    //      }
    //
    //      pngWriter.writeRow(line, y);
    //    }
    //
    //    pngWriter.end();
    //    return baos.toByteArray();
  }
}
