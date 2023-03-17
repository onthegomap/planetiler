package com.onthegomap.planetiler.pmtiles;

import com.carrotsearch.hppc.ByteArrayList;
import com.carrotsearch.hppc.LongLongHashMap;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.archive.WriteableTileArchive;
import com.onthegomap.planetiler.collection.Hppc;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.util.Gzip;
import com.onthegomap.planetiler.util.SeekableInMemoryByteChannel;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PMTiles is a single-file tile archive format designed for efficient access on cloud storage.
 *
 * @see <a href="https://github.com/protomaps/PMTiles/blob/main/spec/v3/spec.md">PMTiles Specification</a>
 */
public final class WriteablePmtiles implements WriteableTileArchive {

  static final int INIT_SECTION = 16384;
  private static final Logger LOGGER = LoggerFactory.getLogger(WriteablePmtiles.class);
  final LongLongHashMap hashToOffset = Hppc.newLongLongHashMap();
  final ArrayList<Pmtiles.Entry> entries = new ArrayList<>();
  private final SeekableByteChannel out;
  private long currentOffset = 0;
  private long numUnhashedTiles = 0;
  private long numAddressedTiles = 0;
  private TileArchiveMetadata tileArchiveMetadata;
  private boolean isClustered = true;

  private WriteablePmtiles(SeekableByteChannel channel) throws IOException {
    this.out = channel;
    out.write(ByteBuffer.allocate(INIT_SECTION));
  }

  private static Directories makeDirectoriesWithLeaves(List<Pmtiles.Entry> subEntries, int leafSize, int attemptNum)
    throws IOException {
    LOGGER.info("Building directories with {} entries per leaf, attempt {}...", leafSize, attemptNum);
    ArrayList<Pmtiles.Entry> rootEntries = new ArrayList<>();
    ByteArrayList leavesOutputStream = new ByteArrayList();
    int leavesLength = 0;
    int numLeaves = 0;

    for (int i = 0; i < subEntries.size(); i += leafSize) {
      numLeaves++;
      int end = i + leafSize;
      if (i + leafSize > subEntries.size()) {
        end = subEntries.size();
      }
      byte[] leafBytes = Pmtiles.directoryToBytes(subEntries, i, end);
      leafBytes = Gzip.gzip(leafBytes);
      rootEntries.add(new Pmtiles.Entry(subEntries.get(i).tileId(), leavesLength, leafBytes.length, 0));
      leavesOutputStream.add(leafBytes);
      leavesLength += leafBytes.length;
    }

    byte[] rootBytes = Pmtiles.directoryToBytes(rootEntries);
    rootBytes = Gzip.gzip(rootBytes);

    LOGGER.info("Built directories with {} leaves, {}B root directory", rootEntries.size(), rootBytes.length);

    return new Directories(rootBytes, leavesOutputStream.toArray(), numLeaves, leafSize, attemptNum);
  }

  /**
   * Serialize all entries into bytes, choosing the # of leaf directories to ensure the header+root fits in 16 KB.
   *
   * @param entries a sorted ObjectArrayList of all entries in the tileset.
   * @return byte arrays of the root and all leaf directories, and the # of leaves.
   * @throws IOException if compression fails
   */
  static Directories makeDirectories(List<Pmtiles.Entry> entries) throws IOException {
    int maxEntriesRootOnly = 16384;
    int attemptNum = 1;
    if (entries.size() < maxEntriesRootOnly) {
      byte[] testBytes = Pmtiles.directoryToBytes(entries, 0, entries.size());
      testBytes = Gzip.gzip(testBytes);

      if (testBytes.length < INIT_SECTION - Pmtiles.HEADER_LEN) {
        return new Directories(testBytes, new byte[0], 0, 0, attemptNum);
      }
    }

    double estimatedLeafSize = entries.size() / 3_500d;
    int leafSize = (int) Math.max(estimatedLeafSize, 4096);

    while (true) {
      Directories temp = makeDirectoriesWithLeaves(entries, leafSize, attemptNum++);
      if (temp.root.length < INIT_SECTION - Pmtiles.HEADER_LEN) {
        return temp;
      }
      leafSize *= 1.2;
    }
  }

  public static WriteablePmtiles newWriteToFile(Path path) throws IOException {
    return new WriteablePmtiles(
      FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE));
  }

  public static WriteablePmtiles newWriteToMemory(SeekableInMemoryByteChannel bytes) throws IOException {
    return new WriteablePmtiles(bytes);
  }

  @Override
  public TileOrder tileOrder() {
    return TileOrder.HILBERT;
  }

  @Override
  public void initialize(TileArchiveMetadata tileArchiveMetadata) {
    this.tileArchiveMetadata = tileArchiveMetadata;
  }

  @Override
  public void finish() {
    if (!isClustered) {
      LOGGER.info("Tile data was not written in order, sorting entries...");
      Collections.sort(entries);
      LOGGER.info("Done sorting.");
    }
    try {
      Directories directories = makeDirectories(entries);
      var otherMetadata = new LinkedHashMap<>(tileArchiveMetadata.toMap());

      // exclude keys included in top-level header
      otherMetadata.remove(TileArchiveMetadata.CENTER);
      otherMetadata.remove(TileArchiveMetadata.ZOOM);
      otherMetadata.remove(TileArchiveMetadata.BOUNDS);
      otherMetadata.remove(TileArchiveMetadata.FORMAT);
      otherMetadata.remove(TileArchiveMetadata.MINZOOM);
      otherMetadata.remove(TileArchiveMetadata.MAXZOOM);
      otherMetadata.remove(TileArchiveMetadata.VECTOR_LAYERS);

      byte[] jsonBytes =
        new Pmtiles.JsonMetadata(tileArchiveMetadata.vectorLayers(), otherMetadata).toBytes();
      jsonBytes = Gzip.gzip(jsonBytes);

      String formatString = tileArchiveMetadata.format();
      var outputFormat =
        TileArchiveMetadata.MVT_FORMAT.equals(formatString) ? Pmtiles.TileType.MVT : Pmtiles.TileType.UNKNOWN;

      var bounds = tileArchiveMetadata.bounds() == null ? GeoUtils.WORLD_LAT_LON_BOUNDS : tileArchiveMetadata.bounds();
      var center = tileArchiveMetadata.center() == null ? bounds.centre() : tileArchiveMetadata.center();
      int zoom =
        (int) (tileArchiveMetadata.zoom() == null ? Math.ceil(GeoUtils.getZoomFromLonLatBounds(bounds)) :
          tileArchiveMetadata.zoom());
      int minzoom = tileArchiveMetadata.minzoom() == null ? 0 : tileArchiveMetadata.minzoom();
      int maxzoom =
        tileArchiveMetadata.maxzoom() == null ? PlanetilerConfig.MAX_MAXZOOM : tileArchiveMetadata.maxzoom();

      Pmtiles.Header header = new Pmtiles.Header(
        (byte) 3,
        Pmtiles.HEADER_LEN,
        directories.root.length,
        INIT_SECTION + currentOffset,
        jsonBytes.length,
        INIT_SECTION + currentOffset + jsonBytes.length,
        directories.leaves.length,
        INIT_SECTION,
        currentOffset,
        numAddressedTiles,
        entries.size(),
        hashToOffset.size() + numUnhashedTiles,
        isClustered,
        Pmtiles.Compression.GZIP,
        Pmtiles.Compression.GZIP,
        outputFormat,
        (byte) minzoom,
        (byte) maxzoom,
        (int) (bounds.getMinX() * 10_000_000),
        (int) (bounds.getMinY() * 10_000_000),
        (int) (bounds.getMaxX() * 10_000_000),
        (int) (bounds.getMaxY() * 10_000_000),
        (byte) zoom,
        (int) center.x * 10_000_000,
        (int) center.y * 10_000_000
      );

      LOGGER.info("Writing metadata and leaf directories...");

      out.write(ByteBuffer.wrap(jsonBytes));
      out.write(ByteBuffer.wrap(directories.leaves));

      LOGGER.info("Writing header...");
      out.position(0);
      out.write(ByteBuffer.wrap(header.toBytes()));
      out.write(ByteBuffer.wrap(directories.root));

      Format format = Format.defaultInstance();

      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("# addressed tiles: {}", numAddressedTiles);
        LOGGER.info("# of tile entries: {}", entries.size());
        LOGGER.info("# of tile contents: {}", (hashToOffset.size() + numUnhashedTiles));
        LOGGER.info("Root directory: {}B", format.storage(directories.root.length, false));

        LOGGER.info("# leaves: {}", directories.numLeaves);
        if (directories.numLeaves > 0) {
          LOGGER.info("Leaf directories: {}B", format.storage(directories.leaves.length, false));
          LOGGER
            .info("Avg leaf size: {}B", format.storage(directories.leaves.length / directories.numLeaves, false));
        }

        LOGGER
          .info("Total dir bytes: {}B", format.storage(directories.root.length + directories.leaves.length, false));
        double tot = (double) directories.root.length + directories.leaves.length;
        LOGGER.info("Average bytes per addressed tile: {}", tot / numAddressedTiles);
      }
    } catch (IOException e) {
      LOGGER.error(e.getMessage());
    }
  }

  @Override
  public void close() throws IOException {
    out.close();
  }

  public WriteableTileArchive.TileWriter newTileWriter() {
    return new DeduplicatingTileWriter();
  }

  public record Directories(byte[] root, byte[] leaves, int numLeaves, int leafSize, int numAttempts) {

    @Override
    public boolean equals(Object o) {
      return o instanceof Directories that &&
        numLeaves == that.numLeaves &&
        Arrays.equals(root, that.root) &&
        Arrays.equals(leaves, that.leaves) &&
        leafSize == that.leafSize &&
        numAttempts == that.numAttempts;
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(numLeaves, leafSize, numAttempts);
      result = 31 * result + Arrays.hashCode(root);
      result = 31 * result + Arrays.hashCode(leaves);
      return result;
    }

    @Override
    public String toString() {
      return "Directories{" +
        "root=" + Arrays.toString(root) +
        ", leaves=" + Arrays.toString(leaves) +
        ", numLeaves=" + numLeaves +
        ", leafSize=" + leafSize +
        ", numAttempts=" + numAttempts +
        '}';
    }
  }

  private class DeduplicatingTileWriter implements TileWriter {
    Pmtiles.Entry lastEntry = null;

    @Override
    public void write(TileEncodingResult encodingResult) {
      numAddressedTiles++;
      boolean writeTileData;
      long offset;
      OptionalLong tileDataHashOpt = encodingResult.tileDataHash();
      var data = encodingResult.tileData();
      TileCoord coord = encodingResult.coord();

      long tileId = coord.hilbertEncoded();

      if (!entries.isEmpty()) {
        if (tileId < lastEntry.tileId()) {
          isClustered = false;
        } else if (tileId == lastEntry.tileId()) {
          LOGGER.error("Duplicate tile detected in writer");
        }
      }

      if (tileDataHashOpt.isPresent()) {
        long tileDataHash = tileDataHashOpt.getAsLong();
        if (hashToOffset.containsKey(tileDataHash)) {
          offset = hashToOffset.get(tileDataHash);
          writeTileData = false;
          if (lastEntry != null && lastEntry.tileId() + lastEntry.runLength() == tileId &&
            lastEntry.offset() == offset) {
            lastEntry.runLength++;
            return;
          }
        } else {
          hashToOffset.put(tileDataHash, currentOffset);
          offset = currentOffset;
          writeTileData = true;
        }
      } else {
        numUnhashedTiles++;
        offset = currentOffset;
        writeTileData = true;
      }

      var newEntry = new Pmtiles.Entry(tileId, offset, data.length, 1);
      entries.add(newEntry);
      lastEntry = newEntry;

      if (writeTileData) {
        try {
          out.write(ByteBuffer.wrap(data));
        } catch (IOException e) {
          throw new UncheckedIOException(e);
        }
        currentOffset += data.length;
      }
    }

    @Override
    public void close() {
      // no cleanup needed.
    }
  }
}
