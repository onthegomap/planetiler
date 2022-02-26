package com.onthegomap.planetiler.reader.osm;

import com.onthegomap.planetiler.config.Bounds;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Supplier;
import org.locationtech.jts.geom.Envelope;
import org.openstreetmap.osmosis.osmbinary.Fileformat.BlobHeader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An input file in {@code .osm.pbf} format.
 *
 * @see <a href="https://wiki.openstreetmap.org/wiki/PBF_Format">OSM PBF Format</a>
 */
public class OsmInputFile implements Bounds.Provider, Supplier<OsmBlockSource> {

  private static final Logger LOGGER = LoggerFactory.getLogger(OsmInputFile.class);

  private final Path path;
  private final boolean lazy;

  public OsmInputFile(Path path, boolean lazyReads) {
    this.path = path;
    lazy = lazyReads;
  }

  public OsmInputFile(Path path) {
    this(path, false);
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

  public static OsmBlockSource readFrom(Path path) {
    return new OsmInputFile(path).get();
  }

  private static void validateHeader(byte[] data) {
    OsmHeader header = PbfDecoder.decodeHeader(data);
    List<String> unsupportedFeatures = header.requiredFeatures().stream()
      .filter(feature -> !(feature.equals("OsmSchema-V0.6") || feature.equals("DenseNodes")))
      .toList();
    if (!unsupportedFeatures.isEmpty()) {
      throw new RuntimeException("PBF file contains unsupported features " + unsupportedFeatures);
    }
  }

  private static BlobHeader readBlobHeader(FileChannel channel) throws IOException {
    int headerSize = readInt(channel);
    if (headerSize > 64 * 1024) {
      throw new IllegalArgumentException("Header longer than 64 KiB");
    }
    byte[] headerBytes = readBytes(channel, headerSize);
    return BlobHeader.parseFrom(headerBytes);
  }

  /**
   * Returns the bounding box of this file from the header block.
   *
   * @throws IllegalArgumentException if an error is encountered reading the file
   */
  @Override
  public Envelope getLatLonBounds() {
    return getHeader().bounds();
  }

  /**
   * Returns details from the header block for this osm.pbf file.
   *
   * @throws IllegalArgumentException if an error is encountered reading the file
   */
  public OsmHeader getHeader() {
    try (var channel = openChannel()) {
      BlobHeader header = readBlobHeader(channel);
      byte[] blobBytes = readBytes(channel, header.getDatasize());
      return PbfDecoder.decodeHeader(blobBytes);
    } catch (IOException e) {
      throw new IllegalArgumentException(e);
    }
  }

  @Override
  public OsmBlockSource get() {
    return lazy ? new LazyReader() : new EagerReader();
  }

  private FileChannel openChannel() {
    try {
      return FileChannel.open(path, StandardOpenOption.READ);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private class EagerReader implements OsmBlockSource {

    @Override
    public void forEachBlock(Consumer<Block> consumer) {
      int blockId = 0;
      try (FileChannel channel = openChannel()) {
        final long size = channel.size();
        while (channel.position() < size) {
          BlobHeader header = readBlobHeader(channel);
          byte[] blockBytes = readBytes(channel, header.getDatasize());
          String headerType = header.getType();
          if ("OSMData".equals(headerType)) {
            consumer.accept(new EagerBlock(blockId++, blockBytes));
          } else if ("OSMHeader".equals(headerType)) {
            validateHeader(blockBytes);
          } else {
            LOGGER.warn("Unrecognized OSM PBF blob header type: " + headerType);
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    record EagerBlock(@Override int id, byte[] bytes) implements Block {

      public Iterable<OsmElement> parse() {
        return PbfDecoder.decode(bytes);
      }
    }
  }

  private class LazyReader implements OsmBlockSource {

    FileChannel lazyReadChannel = openChannel();

    @Override
    public void forEachBlock(Consumer<Block> consumer) {
      int blockId = 0;
      try (FileChannel channel = openChannel()) {
        final long size = channel.size();
        while (channel.position() < size) {
          BlobHeader header = readBlobHeader(channel);
          int blockSize = header.getDatasize();
          String headerType = header.getType();
          long blockStartPosition = channel.position();
          if ("OSMData".equals(headerType)) {
            consumer.accept(new LazyBlock(blockId++, blockStartPosition, blockSize, lazyReadChannel));
          } else if ("OSMHeader".equals(headerType)) {
            validateHeader(readBytes(channel, blockStartPosition, blockSize));
          } else {
            LOGGER.warn("Unrecognized OSM PBF blob header type: " + headerType);
          }
          channel.position(blockStartPosition + blockSize);
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void close() {
      try {
        lazyReadChannel.close();
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    record LazyBlock(@Override int id, long offset, int length, FileChannel channel) implements Block {

      public Iterable<OsmElement> parse() {
        try {
          return PbfDecoder.decode(readBytes(channel, offset, length));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }
  }
}
