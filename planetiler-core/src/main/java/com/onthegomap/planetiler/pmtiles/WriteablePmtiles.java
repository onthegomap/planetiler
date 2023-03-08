package com.onthegomap.planetiler.pmtiles;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_ABSENT;

import com.carrotsearch.hppc.ByteArrayList;
import com.carrotsearch.hppc.LongLongHashMap;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
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
import com.onthegomap.planetiler.util.LayerStats;
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
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PMTiles is a single-file tile archive format designed for efficient access on cloud storage.
 *
 * @see <a href="https://github.com/protomaps/PMTiles/blob/main/spec/v3/spec.md">PMTiles Specification</a>
 */
public final class WriteablePmtiles implements WriteableTileArchive {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteablePmtiles.class);
  private static final ObjectMapper objectMapper = new ObjectMapper()
    .registerModules(new Jdk8Module())
    .setSerializationInclusion(NON_ABSENT);

  private final SeekableByteChannel out;
  private long currentOffset = 0;
  private long numUnhashedTiles = 0;
  private long numAddressedTiles = 0;
  private LayerStats layerStats;
  private TileArchiveMetadata tileArchiveMetadata;
  final LongLongHashMap hashToOffset = Hppc.newLongLongHashMap();
  private boolean isClustered = true;
  final ArrayList<Pmtiles.Entry> entries = new ArrayList<>();


  static final int INIT_SECTION = 16384;

  /**
   * Arbitrary application-specific JSON metadata in the archive.
   * <p>
   * stores name, attribution, created_at, planetiler build SHA, vector_layers, etc.
   */
  public record JsonMetadata(
    @JsonProperty("vector_layers") List<LayerStats.VectorLayer> vectorLayers,
    @JsonAnyGetter Map<String, String> otherMetadata
  ) {

    public byte[] toBytes() {
      try {
        return objectMapper.writeValueAsBytes(this);
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Unable to encode as string: " + this, e);
      }
    }
  }

  public record Directories(byte[] root, byte[] leaves, int numLeaves) {

    @Override
    public boolean equals(Object o) {
      return o instanceof Directories that &&
        numLeaves == that.numLeaves &&
        Arrays.equals(root, that.root) &&
        Arrays.equals(leaves, that.leaves);
    }

    @Override
    public int hashCode() {
      int result = Objects.hash(numLeaves);
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
        '}';
    }
  }

  private static Directories buildRootLeaves(List<Pmtiles.Entry> subEntries, int leafSize) throws IOException {
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

    byte[] rootBytes = Pmtiles.directoryToBytes(rootEntries, 0, rootEntries.size());
    rootBytes = Gzip.gzip(rootBytes);

    return new Directories(rootBytes, leavesOutputStream.toArray(), numLeaves);
  }

  /**
   * Serialize all entries into bytes, choosing the # of leaf directories to ensure the header+root fits in 16 KB.
   *
   * @param entries a sorted ObjectArrayList of all entries in the tileset.
   * @return byte arrays of the root and all leaf directories, and the # of leaves.
   * @throws IOException
   */
  private static Directories makeRootLeaves(List<Pmtiles.Entry> entries) throws IOException {
    int maxEntriesRootOnly = 16384;
    if (entries.size() < maxEntriesRootOnly) {
      byte[] testBytes = Pmtiles.directoryToBytes(entries, 0, entries.size());
      testBytes = Gzip.gzip(testBytes);

      if (testBytes.length < INIT_SECTION - Pmtiles.HEADER_LEN) {
        return new Directories(testBytes, new byte[0], 0);
      }
    }

    double estimatedLeafSize = entries.size() / 3_500d;
    int leafSize = (int) Math.max(estimatedLeafSize, 4096);

    while (true) {
      Directories temp = buildRootLeaves(entries, leafSize);
      if (temp.root.length < INIT_SECTION - Pmtiles.HEADER_LEN) {
        return temp;
      }
      leafSize *= 1.2;
    }
  }

  private WriteablePmtiles(SeekableByteChannel channel) throws IOException {
    this.out = channel;
    out.write(ByteBuffer.allocate(INIT_SECTION));
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
  public void initialize(PlanetilerConfig config, TileArchiveMetadata tileArchiveMetadata, LayerStats layerStats) {
    this.layerStats = layerStats;
    this.tileArchiveMetadata = tileArchiveMetadata;
  }

  @Override
  public void finish(PlanetilerConfig config) {
    if (!isClustered) {
      Collections.sort(entries);
    }
    try {
      Directories archiveDirs = makeRootLeaves(entries);
      byte[] jsonBytes = new JsonMetadata(layerStats.getTileStats(), tileArchiveMetadata.getAll()).toBytes();
      jsonBytes = Gzip.gzip(jsonBytes);

      Envelope envelope = config.bounds().latLon();

      Pmtiles.Header header = new Pmtiles.Header(
        (byte) 3,
        Pmtiles.HEADER_LEN,
        archiveDirs.root.length,
        INIT_SECTION + currentOffset,
        jsonBytes.length,
        INIT_SECTION + currentOffset + jsonBytes.length,
        archiveDirs.leaves.length,
        INIT_SECTION,
        currentOffset,
        numAddressedTiles,
        entries.size(),
        hashToOffset.size() + numUnhashedTiles,
        isClustered,
        Pmtiles.Compression.GZIP,
        Pmtiles.Compression.GZIP,
        Pmtiles.TileType.MVT,
        (byte) config.minzoom(),
        (byte) config.maxzoom(),
        (int) (envelope.getMinX() * 10_000_000),
        (int) (envelope.getMinY() * 10_000_000),
        (int) (envelope.getMaxX() * 10_000_000),
        (int) (envelope.getMaxY() * 10_000_000),
        (byte) Math.ceil(GeoUtils.getZoomFromLonLatBounds(envelope)),
        (int) ((envelope.getMinX() + envelope.getMaxX()) / 2 * 10_000_000),
        (int) ((envelope.getMinY() + envelope.getMaxY()) / 2 * 10_000_000)
      );

      out.write(ByteBuffer.wrap(jsonBytes));
      out.write(ByteBuffer.wrap(archiveDirs.leaves));

      out.position(0);
      out.write(ByteBuffer.wrap(header.toBytes()));
      out.write(ByteBuffer.wrap(archiveDirs.root));

      Format format = Format.defaultInstance();

      if (LOGGER.isInfoEnabled()) {
        LOGGER.info("# addressed tiles: {}", numAddressedTiles);
        LOGGER.info("# of tile entries: {}", entries.size());
        LOGGER.info("# of tile contents: {}", (hashToOffset.size() + numUnhashedTiles));
        LOGGER.info("Root directory: {}B", format.storage(archiveDirs.root.length, false));

        LOGGER.info("# leaves: {}", archiveDirs.numLeaves);
        if (archiveDirs.numLeaves > 0) {
          LOGGER.info("Leaf directories: {}B", format.storage(archiveDirs.leaves.length, false));
          LOGGER
            .info("Avg leaf size: {}B", format.storage(archiveDirs.leaves.length / archiveDirs.numLeaves, false));
        }

        LOGGER
          .info("Total dir bytes: {}B", format.storage(archiveDirs.root.length + archiveDirs.leaves.length, false));
        double tot = (double) archiveDirs.root.length + archiveDirs.leaves.length;
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

  private class DeduplicatingTileWriter implements TileWriter {

    @Override
    public void write(TileEncodingResult encodingResult) {
      numAddressedTiles++;
      boolean writeData;
      long offset;
      OptionalLong tileDataHashOpt = encodingResult.tileDataHash();
      var data = encodingResult.tileData();
      TileCoord coord = encodingResult.coord();

      long tileId = coord.hilbertEncoded();
      Pmtiles.Entry lastEntry = null;

      if (!entries.isEmpty()) {
        lastEntry = entries.get(entries.size() - 1);
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
          writeData = false;
          if (lastEntry != null && lastEntry.tileId() + lastEntry.runLength() == tileId &&
            lastEntry.offset() == offset) {
            entries.get(entries.size() - 1).runLength = lastEntry.runLength() + 1;
          } else {
            entries.add(new Pmtiles.Entry(tileId, offset, data.length, 1));
          }
        } else {
          hashToOffset.put(tileDataHash, currentOffset);
          entries.add(new Pmtiles.Entry(tileId, currentOffset, data.length, 1));
          writeData = true;
        }
      } else {
        numUnhashedTiles++;
        entries.add(new Pmtiles.Entry(tileId, currentOffset, data.length, 1));
        writeData = true;
      }

      if (writeData) {
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

  public WriteableTileArchive.TileWriter newTileWriter() {
    return new DeduplicatingTileWriter();
  }
}
