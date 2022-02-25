package com.onthegomap.planetiler.reader.osm;

import com.google.protobuf.ByteString;
import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.worker.WorkerPipeline;
import java.io.DataInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.zip.DataFormatException;
import java.util.zip.Inflater;
import org.locationtech.jts.geom.Envelope;
import org.openstreetmap.osmosis.osmbinary.Fileformat;
import org.openstreetmap.osmosis.osmbinary.Fileformat.Blob;
import org.openstreetmap.osmosis.osmbinary.Fileformat.BlobHeader;
import org.openstreetmap.osmosis.osmbinary.Osmformat.HeaderBBox;
import org.openstreetmap.osmosis.osmbinary.Osmformat.HeaderBlock;

/**
 * An input file in {@code .osm.pbf} format.
 *
 * @see <a href="https://wiki.openstreetmap.org/wiki/PBF_Format">OSM PBF Format</a>
 */
public class OsmInputFile implements Bounds.Provider {

  private final Path path;
  private final boolean lazy;

  public OsmInputFile(Path path, boolean lazyReads) {
    this.path = path;
    lazy = lazyReads;
  }

  public OsmInputFile(Path path) {
    this(path, false);
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

  private static int readInt(FileChannel channel) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(4);
    int read = channel.read(buf);
    if (read != 4) {
      throw new IOException("Tried to read 4 bytes but only got " + read);
    }
    return buf.flip().getInt();
  }

  private static byte[] readBytes(FileChannel channel, int length) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(length);
    int read = channel.read(buf);
    if (read != length) {
      throw new IOException("Tried to read " + length + " bytes but only got " + read);
    }
    return buf.flip().array();
  }

  private static byte[] readBytes(FileChannel channel, long offset, int length) throws IOException {
    ByteBuffer buf = ByteBuffer.allocate(length);
    int read = channel.read(buf, offset);
    if (read != length) {
      throw new IOException("Tried to read " + length + " bytes at " + offset + " but only got " + read);
    }
    return buf.flip().array();
  }

  public static OsmSource readFrom(Path path) {
    return new OsmInputFile(path).newReader();
  }

  public OsmSource newReader() {
    return lazy ? new LazyReader() : new EagerReader();
  }

  private FileChannel openChannel() {
    try {
      return FileChannel.open(path, StandardOpenOption.READ);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private class EagerReader implements OsmSource {

    record EagerBlock(@Override int id, byte[] bytes) implements Block {

      public Iterable<OsmElement> parse() {
        return PbfDecoder.decode(bytes);
      }
    }

    @Override
    public WorkerPipeline.SourceStep<Block> readBlocks() {
      return next -> {
        int blockId = 0;
        try (FileChannel channel = openChannel()) {
          while (channel.position() < channel.size()) {
            int headerSize = readInt(channel);
            byte[] headerBytes = readBytes(channel, headerSize);
            Fileformat.BlobHeader header = Fileformat.BlobHeader.parseFrom(headerBytes);
            byte[] blockBytes = readBytes(channel, header.getDatasize());
            if ("OSMData".equals(header.getType())) {
              next.accept(new EagerBlock(blockId++, blockBytes));
            }
          }
        }
      };
    }
  }

  private class LazyReader implements OsmSource {

    FileChannel lazyReadChannel = openChannel();

    record LazyBlock(@Override int id, long offset, int length, FileChannel channel) implements Block {

      public Iterable<OsmElement> parse() {
        try {
          return PbfDecoder.decode(readBytes(channel, offset, length));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }

    @Override
    public WorkerPipeline.SourceStep<Block> readBlocks() {
      return next -> {
        int blockId = 0;
        try (FileChannel channel = openChannel()) {
          while (channel.position() < channel.size()) {
            int headerSize = readInt(channel);
            byte[] headerBytes = readBytes(channel, headerSize);
            Fileformat.BlobHeader header = Fileformat.BlobHeader.parseFrom(headerBytes);
            int blockSize = header.getDatasize();
            if ("OSMData".equals(header.getType())) {
              next.accept(new LazyBlock(blockId++, channel.position(), blockSize, lazyReadChannel));
            }
            channel.position(channel.position() + blockSize);
          }
        }
      };
    }

    @Override
    public void close() {
      try {
        lazyReadChannel.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
  }
}
