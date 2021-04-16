package com.onthegomap.flatmap;

import static com.onthegomap.flatmap.GeoUtils.x;
import static com.onthegomap.flatmap.GeoUtils.y;
import static com.onthegomap.flatmap.GeoUtils.z;

import com.onthegomap.flatmap.collections.MergeSortFeatureMap;
import com.onthegomap.flatmap.collections.MergeSortFeatureMap.TileFeatures;
import com.onthegomap.flatmap.monitoring.ProgressLoggers;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.worker.Topology;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.zip.GZIPOutputStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MbtilesWriter {

  private final AtomicLong featuresProcessed = new AtomicLong(0);
  private final AtomicLong memoizedTiles = new AtomicLong(0);
  private final AtomicLong tiles = new AtomicLong(0);
  private final Stats stats;

  private MbtilesWriter(Stats stats) {
    this.stats = stats;
  }

  private static final Logger LOGGER = LoggerFactory.getLogger(MbtilesWriter.class);

  private static record RenderedTile(int tile, byte[] contents) {

  }

  public static void writeOutput(long featureCount, MergeSortFeatureMap features, File output, FlatMapConfig config) {
    Stats stats = config.stats();
    output.delete();
    MbtilesWriter writer = new MbtilesWriter(config.stats());

    var topology = Topology.start("mbtiles", stats)
      .readFromIterator("reader", features.getAll())
      .addBuffer("reader_queue", 50_000, 1_000)
      .addWorker("encoder", config.threads(), writer::tileEncoder)
      .addBuffer("writer_queue", 50_000, 1_000)
      .sinkTo("writer", 1, writer::tileWriter);

    var loggers = new ProgressLoggers("mbtiles")
      .addRatePercentCounter("features", featureCount, writer.featuresProcessed)
      .addRateCounter("tiles", writer.tiles)
      .addFileSize(output)
      .add(" features ").addFileSize(features::getStorageSize)
      .addProcessStats()
      .addTopologyStats(topology);

    topology.awaitAndLog(loggers, config.logInterval());
  }

  public void tileEncoder(Supplier<TileFeatures> prev, Consumer<RenderedTile> next) throws Exception {
    MergeSortFeatureMap.TileFeatures tileFeatures, last = null;
    byte[] lastBytes = null, lastEncoded = null;
    while ((tileFeatures = prev.get()) != null) {
      featuresProcessed.addAndGet(tileFeatures.getNumFeatures());
      byte[] bytes, encoded;
      int zoom = z(tileFeatures.getTileId());
      if (tileFeatures.hasSameContents(last)) {
        bytes = lastBytes;
        encoded = lastEncoded;
        memoizedTiles.incrementAndGet();
      } else {
        VectorTile en = tileFeatures.getTile();
        encoded = en.encode();
        bytes = gzipCompress(encoded);
        last = tileFeatures;
        lastEncoded = encoded;
        lastBytes = bytes;
        if (encoded.length > 1_000_000) {
          LOGGER.warn("Tile " + zoom + "/" + x(tileFeatures.getTileId()) + "/" + y(tileFeatures.getTileId()) + " "
            + encoded.length / 1024 + "kb uncompressed");
        }
      }
      stats.encodedTile(zoom, encoded.length);
      next.accept(new RenderedTile(tileFeatures.getTileId(), bytes));
    }
  }

  private void tileWriter(Supplier<RenderedTile> prev) throws Exception {

  }

  private static byte[] gzipCompress(byte[] uncompressedData) throws IOException {
    var bos = new ByteArrayOutputStream(uncompressedData.length);
    try (var gzipOS = new GZIPOutputStream(bos)) {
      gzipOS.write(uncompressedData);
    }
    return bos.toByteArray();
  }

}
