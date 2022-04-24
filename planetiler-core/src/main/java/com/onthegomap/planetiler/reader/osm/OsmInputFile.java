package com.onthegomap.planetiler.reader.osm;

import com.onthegomap.planetiler.config.Bounds;
import com.onthegomap.planetiler.reader.FileFormatException;
import com.onthegomap.planetiler.util.DiskBacked;
import com.onthegomap.planetiler.util.FileUtils;
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
public class OsmInputFile implements Bounds.Provider, Supplier<OsmBlockSource>, DiskBacked {

  private static final Logger LOGGER = LoggerFactory.getLogger(OsmInputFile.class);

  private final Path path;
  private final boolean lazy;

  /**
   * Creates a new OSM input file reader.
   *
   * @param path      Path to the file
   * @param lazyReads If {@code true}, defers reading the actual content of each block from disk until the block is
   *                  decoded in a worker thread.
   */
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
      throw new FileFormatException("PBF file contains unsupported features " + unsupportedFeatures);
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

  @Override
  public long diskUsageBytes() {
    return FileUtils.size(path);
  }

  private FileChannel openChannel() {
    try {
      return FileChannel.open(path, StandardOpenOption.READ);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * An OSM block reader that iterates through the input file in a single thread, reading the raw bytes of each block
   * and passing them off to worker threads.
   */
  private class EagerReader implements OsmBlockSource {

    @Override
    public void forEachBlock(Consumer<Block> consumer) {
      try (FileChannel channel = openChannel()) {
        final long size = channel.size();
        while (channel.position() < size) {
          BlobHeader header = readBlobHeader(channel);
          byte[] blockBytes = readBytes(channel, header.getDatasize());
          String headerType = header.getType();
          if ("OSMData".equals(headerType)) {
            consumer.accept(new EagerBlock(blockBytes));
          } else if ("OSMHeader".equals(headerType)) {
            validateHeader(blockBytes);
          } else {
            LOGGER.warn("Unrecognized OSM PBF blob header type: {}", headerType);
          }
        }
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    private static final class EagerBlock implements Block {
      // not a record since would need to override equals/hashcode for byte array anyway
      private final byte[] bytes;

      private EagerBlock(byte[] bytes) {
        this.bytes = bytes;
      }

      public Iterable<OsmElement> decodeElements() {
        return PbfDecoder.decode(bytes);
      }
    }
  }

  /**
   * An OSM block reader that iterates through the input file in a single thread, skipping over each block and just
   * passing the position/offset to workers so they can read the contents from disk in parallel.
   * <p>
   * This may result in a speedup on some systems.
   */
  private class LazyReader implements OsmBlockSource {

    final FileChannel lazyReadChannel = openChannel();

    @Override
    public void forEachBlock(Consumer<Block> consumer) {
      try (FileChannel channel = openChannel()) {
        final long size = channel.size();
        while (channel.position() < size) {
          BlobHeader header = readBlobHeader(channel);
          int blockSize = header.getDatasize();
          String headerType = header.getType();
          long blockStartPosition = channel.position();
          if ("OSMData".equals(headerType)) {
            consumer.accept(new LazyBlock(blockStartPosition, blockSize, lazyReadChannel));
          } else if ("OSMHeader".equals(headerType)) {
            validateHeader(readBytes(channel, blockStartPosition, blockSize));
          } else {
            LOGGER.warn("Unrecognized OSM PBF blob header type: {}", headerType);
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

    private record LazyBlock(long offset, int length, FileChannel channel) implements Block {

      public Iterable<OsmElement> decodeElements() {
        try {
          return PbfDecoder.decode(readBytes(channel, offset, length));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
      }
    }
  }
}
