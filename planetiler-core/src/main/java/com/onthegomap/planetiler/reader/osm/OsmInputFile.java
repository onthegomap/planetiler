package com.onthegomap.planetiler.reader.osm;

import com.google.protobuf.ByteString;
import com.graphhopper.reader.ReaderElement;
import com.graphhopper.reader.osm.pbf.PbfDecoder;
import com.graphhopper.reader.osm.pbf.PbfStreamSplitter;
import com.graphhopper.reader.osm.pbf.Sink;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.function.Consumer;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import org.locationtech.jts.geom.Envelope;
import org.openstreetmap.osmosis.osmbinary.Fileformat.Blob;
import org.openstreetmap.osmosis.osmbinary.Fileformat.BlobHeader;
import org.openstreetmap.osmosis.osmbinary.Osmformat.HeaderBBox;
import org.openstreetmap.osmosis.osmbinary.Osmformat.HeaderBlock;

/**
 * An input file in {@code .osm.pbf} format.
 *
 * @see <a href="https://wiki.openstreetmap.org/wiki/PBF_Format">OSM PBF Format</a>
 */
public class OsmInputFile implements Bounds.Provider, OsmSource {

  private final Path path;

  public OsmInputFile(Path path) {
    this.path = path;
  }

  /**
   * Returns the bounding box of this file from the header block.
   *
   * @throws IllegalArgumentException if an error is encountered reading the file
   */
  @Override
  public Envelope getLatLonBounds() {
    try (var input = Files.newInputStream(path)) {
      // Read the "bbox" field of the header block of the input file.
      // https://wiki.openstreetmap.org/wiki/PBF_Format
      var dataInput = new DataInputStream(input);
      int headerSize = dataInput.readInt();
      if (headerSize > 64 * 1024) {
        throw new IllegalArgumentException("Header longer than 64 KiB: " + path);
      }
      byte[] buf = dataInput.readNBytes(headerSize);
      BlobHeader header = BlobHeader.parseFrom(buf);
      if (!header.getType().equals("OSMHeader")) {
        throw new IllegalArgumentException("Expecting OSMHeader got " + header.getType() + " in " + path);
      }
      buf = dataInput.readNBytes(header.getDatasize());
      Blob blob = Blob.parseFrom(buf);
      ByteString data;
      if (blob.hasRaw()) {
        data = blob.getRaw();
      } else if (blob.hasZlibData()) {
        byte[] buf2 = new byte[blob.getRawSize()];
        Inflater decompresser = new Inflater();
        decompresser.setInput(blob.getZlibData().toByteArray());
        decompresser.inflate(buf2);
        decompresser.end();
        data = ByteString.copyFrom(buf2);
      } else {
        throw new IllegalArgumentException("Header does not have raw or zlib data");
      }
      HeaderBlock headerblock = HeaderBlock.parseFrom(data);
      HeaderBBox bbox = headerblock.getBbox();
      // always specified in nanodegrees
      return new Envelope(
        bbox.getLeft() / 1e9,
        bbox.getRight() / 1e9,
        bbox.getBottom() / 1e9,
        bbox.getTop() / 1e9
      );
    } catch (IOException | DataFormatException e) {
      throw new IllegalArgumentException(e);
    }
  }

  /**
   * Reads all elements from the input file using {@code threads} threads to decode blocks in parallel and writes them
   * to {@code next}.
   *
   * @throws IOException if an error is encountered reading the file
   */
  public void readTo(Consumer<ReaderElement> next, String poolName, int threads) throws IOException {
    ThreadFactory threadFactory = Executors.defaultThreadFactory();
    ExecutorService executorService = Executors.newFixedThreadPool(threads, (runnable) -> {
      Thread thread = threadFactory.newThread(runnable);
      thread.setName(poolName + "-" + thread.getName());
      return thread;
    });
    try (var stream = new BufferedInputStream(Files.newInputStream(path), 50_000)) {
      PbfStreamSplitter streamSplitter = new PbfStreamSplitter(new DataInputStream(stream));
      var sink = new ReaderElementSink(next);
      PbfDecoder pbfDecoder = new PbfDecoder(streamSplitter, executorService, threads + 1, sink);
      pbfDecoder.run();
    } finally {
      executorService.shutdownNow();
    }
  }

  /** Starts a {@link WorkerPipeline} with all elements read from this input file. */
  @Override
  public WorkerPipeline.SourceStep<ReaderElement> read(String poolName, int threads) {
    return next -> readTo(next, poolName, threads);
  }

  private static record ReaderElementSink(Consumer<ReaderElement> queue) implements Sink {

    @Override
    public void process(ReaderElement readerElement) {
      queue.accept(readerElement);
    }

    @Override
    public void complete() {
    }
  }
}
