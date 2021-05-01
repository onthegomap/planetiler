package com.onthegomap.flatmap.read;

import com.google.protobuf.ByteString;
import com.graphhopper.reader.ReaderElement;
import com.graphhopper.reader.osm.pbf.PbfDecoder;
import com.graphhopper.reader.osm.pbf.PbfStreamSplitter;
import com.graphhopper.reader.osm.pbf.Sink;
import com.onthegomap.flatmap.BoundsProvider;
import com.onthegomap.flatmap.worker.Topology;
import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import org.locationtech.jts.geom.Envelope;
import org.openstreetmap.osmosis.osmbinary.Fileformat.Blob;
import org.openstreetmap.osmosis.osmbinary.Fileformat.BlobHeader;
import org.openstreetmap.osmosis.osmbinary.Osmformat.HeaderBBox;
import org.openstreetmap.osmosis.osmbinary.Osmformat.HeaderBlock;
import org.openstreetmap.osmosis.osmbinary.file.FileFormatException;

public class OsmInputFile implements BoundsProvider {

  private final Path path;

  public OsmInputFile(Path path) {
    this.path = path;
  }

  @Override
  public Envelope getBounds() {
    try (var input = Files.newInputStream(path)) {
      var dataInput = new DataInputStream(input);
      int headerSize = dataInput.readInt();
      if (headerSize > 65536) {
        throw new FileFormatException("Unexpectedly long header 65536 bytes. Possibly corrupt file " + path);
      }
      byte[] buf = dataInput.readNBytes(headerSize);
      BlobHeader header = BlobHeader.parseFrom(buf);
      if (!header.getType().equals("OSMHeader")) {
        throw new IllegalArgumentException("Expecting OSMHeader got " + header.getType());
      }
      buf = dataInput.readNBytes(header.getDatasize());
      Blob blob = Blob.parseFrom(buf);
      ByteString data = null;
      if (blob.hasRaw()) {
        data = blob.getRaw();
      } else if (blob.hasZlibData()) {
        byte[] buf2 = new byte[blob.getRawSize()];
        Inflater decompresser = new Inflater();
        decompresser.setInput(blob.getZlibData().toByteArray());
        decompresser.inflate(buf2);
        decompresser.end();
        data = ByteString.copyFrom(buf2);
      }
      HeaderBlock headerblock = HeaderBlock.parseFrom(data);
      HeaderBBox bbox = headerblock.getBbox();
      return new Envelope(
        bbox.getLeft() / 1e9,
        bbox.getRight() / 1e9,
        bbox.getBottom() / 1e9,
        bbox.getTop() / 1e9
      );
    } catch (IOException | DataFormatException e) {
      throw new RuntimeException(e);
    }
  }

  public void readTo(Consumer<ReaderElement> next, int threads) throws IOException {
    ExecutorService executorService = Executors.newFixedThreadPool(threads);
    try (var stream = new BufferedInputStream(Files.newInputStream(path), 50_000)) {
      PbfStreamSplitter streamSplitter = new PbfStreamSplitter(new DataInputStream(stream));
      var sink = new ReaderElementSink(next);
      PbfDecoder pbfDecoder = new PbfDecoder(streamSplitter, executorService, threads + 1, sink);
      pbfDecoder.run();
    } finally {
      executorService.shutdownNow();
    }
  }

  public Topology.SourceStep<ReaderElement> read(int threads) {
    return next -> readTo(next, threads);
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
