package com.onthegomap.flatmap;

import com.google.protobuf.ByteString;
import com.graphhopper.reader.ReaderElement;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.worker.Topology;
import com.onthegomap.flatmap.worker.WorkQueue;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import org.openstreetmap.osmosis.osmbinary.Fileformat.Blob;
import org.openstreetmap.osmosis.osmbinary.Fileformat.BlobHeader;
import org.openstreetmap.osmosis.osmbinary.Osmformat.HeaderBBox;
import org.openstreetmap.osmosis.osmbinary.Osmformat.HeaderBlock;
import org.openstreetmap.osmosis.osmbinary.file.FileFormatException;

public class OsmInputFile {

  private final File file;

  public OsmInputFile(File file) {
    this.file = file;
  }

  public double[] getBounds() {
    try (var input = new FileInputStream(file)) {
      var datinput = new DataInputStream(input);
      int headersize = datinput.readInt();
      if (headersize > 65536) {
        throw new FileFormatException(
          "Unexpectedly long header 65536 bytes. Possibly corrupt file " + file);
      }
      byte[] buf = datinput.readNBytes(headersize);
      BlobHeader header = BlobHeader.parseFrom(buf);
      if (!header.getType().equals("OSMHeader")) {
        throw new IllegalArgumentException("Expecting OSMHeader got " + header.getType());
      }
      buf = datinput.readNBytes(header.getDatasize());
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
      return new double[]{
        bbox.getLeft() / 1e9,
        bbox.getBottom() / 1e9,
        bbox.getRight() / 1e9,
        bbox.getTop() / 1e9
      };
    } catch (IOException | DataFormatException e) {
      throw new RuntimeException(e);
    }
  }

  public WorkQueue<ReaderElement> newReaderQueue(String name, int threads, int size, int batchSize, Stats stats) {
    return null;
  }

  public Topology.Builder<?, ReaderElement> newTopology(
    String prefix,
    int readerThreads,
    int size,
    int batchSize,
    Stats stats
  ) {
    return Topology.start(prefix, stats)
      .readFromQueue(newReaderQueue(prefix + "_reader_queue", readerThreads, size, batchSize, stats));
  }
}
