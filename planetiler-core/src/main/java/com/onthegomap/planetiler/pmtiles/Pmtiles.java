package com.onthegomap.planetiler.pmtiles;

import static com.fasterxml.jackson.annotation.JsonInclude.Include.NON_ABSENT;

import com.carrotsearch.hppc.ByteArrayList;
import com.fasterxml.jackson.annotation.JsonAnyGetter;
import com.fasterxml.jackson.annotation.JsonAnySetter;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import com.onthegomap.planetiler.reader.FileFormatException;
import com.onthegomap.planetiler.util.LayerStats;
import com.onthegomap.planetiler.util.MemoryEstimator;
import com.onthegomap.planetiler.util.VarInt;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;

public class Pmtiles {
  public enum Compression {
    UNKNOWN((byte) 0),
    NONE((byte) 1),
    GZIP((byte) 2);

    private final byte value;

    Compression(byte value) {
      this.value = value;
    }

    public byte getValue() {
      return this.value;
    }

    public static Compression fromByte(byte b) {
      for (var entry : values()) {
        if (entry.value == b) {
          return entry;
        }
      }
      return UNKNOWN;
    }
  }

  public enum TileType {
    UNKNOWN((byte) 0),
    MVT((byte) 1);

    private final byte value;

    TileType(byte value) {
      this.value = value;
    }

    public byte getValue() {
      return this.value;
    }

    public static TileType fromByte(byte b) {
      for (var entry : values()) {
        if (entry.value == b) {
          return entry;
        }
      }
      return UNKNOWN;
    }
  }

  static final int HEADER_LEN = 127;

  public record Header(
    byte specVersion,
    long rootDirOffset,
    long rootDirLength,
    long jsonMetadataOffset,
    long jsonMetadataLength,
    long leafDirectoriesOffset,
    long leafDirectoriesLength,
    long tileDataOffset,
    long tileDataLength,
    long numAddressedTiles,
    long numTileEntries,
    long numTileContents,
    boolean clustered,
    Compression internalCompression,
    Compression tileCompression,
    TileType tileType,
    byte minZoom,
    byte maxZoom,
    int minLonE7, // Store a decimal longitude as a signed 32-bit integer by multiplying by 10,000,000.
    int minLatE7,
    int maxLonE7,
    int maxLatE7,
    byte centerZoom,
    int centerLonE7,
    int centerLatE7) {

    public byte[] toBytes() {
      ByteBuffer buf = ByteBuffer.allocate(HEADER_LEN).order(ByteOrder.LITTLE_ENDIAN);
      String magic = "PMTiles";
      buf.put(magic.getBytes(StandardCharsets.UTF_8));
      buf.put(specVersion);
      buf.putLong(rootDirOffset);
      buf.putLong(rootDirLength);
      buf.putLong(jsonMetadataOffset);
      buf.putLong(jsonMetadataLength);
      buf.putLong(leafDirectoriesOffset);
      buf.putLong(leafDirectoriesLength);
      buf.putLong(tileDataOffset);
      buf.putLong(tileDataLength);
      buf.putLong(numAddressedTiles);
      buf.putLong(numTileEntries);
      buf.putLong(numTileContents);
      buf.put((byte) (clustered ? 1 : 0));
      buf.put(internalCompression.getValue());
      buf.put(tileCompression.getValue());
      buf.put(tileType.getValue());
      buf.put(minZoom);
      buf.put(maxZoom);
      buf.putInt(minLonE7);
      buf.putInt(minLatE7);
      buf.putInt(maxLonE7);
      buf.putInt(maxLatE7);
      buf.put(centerZoom);
      buf.putInt(centerLonE7);
      buf.putInt(centerLatE7);
      return buf.array();
    }

    public static Header fromBytes(byte[] bytes) {
      ByteBuffer buffer = ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN);
      byte[] magic = new byte[7];

      try {
        buffer.get(magic);
        if (!(new String(magic, StandardCharsets.UTF_8).equals("PMTiles"))) {
          throw new FileFormatException("Incorrect magic number for PMTiles archive.");
        }

        byte specVersion = buffer.get();
        long rootDirOffset = buffer.getLong();
        long rootDirLength = buffer.getLong();
        long jsonMetadataOffset = buffer.getLong();
        long jsonMetadataLength = buffer.getLong();
        long leafDirectoriesOffset = buffer.getLong();
        long leafDirectoriesLength = buffer.getLong();
        long tileDataOffset = buffer.getLong();
        long tileDataLength = buffer.getLong();
        long numAddressedTiles = buffer.getLong();
        long numTileEntries = buffer.getLong();
        long numTileContents = buffer.getLong();
        boolean clustered = (buffer.get() == 0x1);
        Compression internalCompression = Compression.fromByte(buffer.get());
        Compression tileCompression = Compression.fromByte(buffer.get());
        TileType tileType = TileType.fromByte(buffer.get());
        byte minZoom = buffer.get();
        byte maxZoom = buffer.get();
        int minLonE7 = buffer.getInt();
        int minLatE7 = buffer.getInt();
        int maxLonE7 = buffer.getInt();
        int maxLatE7 = buffer.getInt();
        byte centerZoom = buffer.get();
        int centerLonE7 = buffer.getInt();
        int centerLatE7 = buffer.getInt();
        return new Header(
          specVersion,
          rootDirOffset,
          rootDirLength,
          jsonMetadataOffset,
          jsonMetadataLength,
          leafDirectoriesOffset,
          leafDirectoriesLength,
          tileDataOffset,
          tileDataLength,
          numAddressedTiles,
          numTileEntries,
          numTileContents,
          clustered,
          internalCompression,
          tileCompression,
          tileType,
          minZoom,
          maxZoom,
          minLonE7,
          minLatE7,
          maxLonE7,
          maxLatE7,
          centerZoom,
          centerLonE7,
          centerLatE7
        );
      } catch (BufferUnderflowException e) {
        throw new FileFormatException("Failed to read enough bytes for PMTiles header.");
      }
    }

    public Envelope bounds() {
      return new Envelope(
        minLonE7 / 1e7,
        maxLonE7 / 1e7,
        minLatE7 / 1e7,
        maxLatE7 / 1e7
      );
    }

    public CoordinateXY center() {
      return new CoordinateXY(
        centerLonE7 / 1e7,
        centerLatE7 / 1e7
      );
    }
  }

  public static final class Entry implements Comparable<Entry> {

    public static final int BYTES = (int) MemoryEstimator.CLASS_HEADER_BYTES + Long.BYTES * 2 + Integer.BYTES * 2;
    private long tileId;
    private long offset;
    private int length;
    int runLength;

    public Entry(long tileId, long offset, int length, int runLength) {
      this.tileId = tileId;
      this.offset = offset;
      this.length = length;
      this.runLength = runLength;
    }

    public long tileId() {
      return tileId;
    }

    public long offset() {
      return offset;
    }

    public int length() {
      return length;
    }

    public int runLength() {
      return runLength;
    }

    @Override
    public boolean equals(Object o) {
      return this == o || (o instanceof Entry other &&
        tileId == other.tileId &&
        offset == other.offset &&
        length == other.length &&
        runLength == other.runLength);
    }

    @Override
    public int hashCode() {
      return Objects.hash(tileId, offset, length, runLength);
    }

    @Override
    public int compareTo(Entry that) {
      return Long.compare(this.tileId, that.tileId);
    }
  }


  /**
   * Convert a range of entries from a directory to bytes.
   *
   * @param slice a list of entries sorted by ascending {@code tileId} with size > 0.
   * @param start the start index to serialize, inclusive.
   * @param end   the end index, exclusive.
   * @return the uncompressed bytes of the directory.
   */
  public static byte[] directoryToBytes(List<Entry> slice, int start, int end) {
    return directoryToBytes(start == 0 && end == slice.size() ? slice : slice.subList(start, end));
  }

  /**
   * Convert a directory of entries to bytes.
   *
   * @param slice a list of entries sorted by ascending {@code tileId} with size > 0.
   * @return the uncompressed bytes of the directory.
   */
  public static byte[] directoryToBytes(List<Entry> slice) {
    ByteArrayList dir = new ByteArrayList();

    VarInt.putVarLong(slice.size(), dir);

    long lastId = 0;
    for (var entry : slice) {
      VarInt.putVarLong(entry.tileId - lastId, dir);
      lastId = entry.tileId;
    }

    for (var entry : slice) {
      VarInt.putVarLong(entry.runLength, dir);
    }

    for (var entry : slice) {
      VarInt.putVarLong(entry.length, dir);
    }

    Entry last = null;
    for (var entry : slice) {
      if (last != null && entry.offset == last.offset + last.length) {
        VarInt.putVarLong(0, dir);
      } else {
        VarInt.putVarLong(entry.offset + 1, dir);
      }
      last = entry;
    }

    return dir.toArray();
  }

  public static List<Entry> directoryFromBytes(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int numEntries = (int) VarInt.getVarLong(buffer);
    ArrayList<Entry> result = new ArrayList<>(numEntries);

    long lastId = 0;
    for (int i = 0; i < numEntries; i++) {
      long tileId = lastId + VarInt.getVarLong(buffer);
      result.add(new Entry(tileId, 0, 0, 0));
      lastId = tileId;
    }

    for (int i = 0; i < numEntries; i++) {
      result.get(i).runLength = (int) VarInt.getVarLong(buffer);
    }

    for (int i = 0; i < numEntries; i++) {
      result.get(i).length = (int) VarInt.getVarLong(buffer);
    }

    for (int i = 0; i < numEntries; i++) {
      long tmp = VarInt.getVarLong(buffer);
      if (i > 0 && tmp == 0) {
        result.get(i).offset = result.get(i - 1).offset + result.get(i - 1).length;
      } else {
        result.get(i).offset = tmp - 1;
      }
    }
    return result;
  }

  private static final ObjectMapper objectMapper = new ObjectMapper()
    .registerModules(new Jdk8Module())
    .setSerializationInclusion(NON_ABSENT);

  /**
   * Arbitrary application-specific JSON metadata in the archive.
   * <p>
   * stores name, attribution, created_at, planetiler build SHA, vector_layers, etc.
   */
  public record JsonMetadata(
    @JsonProperty("vector_layers") List<LayerStats.VectorLayer> vectorLayers,
    @JsonAnyGetter @JsonAnySetter Map<String, String> otherMetadata
  ) {

    @JsonCreator
    JsonMetadata(@JsonProperty("vector_layers") List<LayerStats.VectorLayer> vectorLayers) {
      this(vectorLayers, new HashMap<>());
    }

    public byte[] toBytes() {

      try {
        return objectMapper.writeValueAsBytes(this);
      } catch (JsonProcessingException e) {
        throw new IllegalArgumentException("Unable to encode as string: " + this, e);
      }
    }

    public static JsonMetadata fromBytes(byte[] bytes) {
      try {
        return objectMapper.readValue(bytes, JsonMetadata.class);
      } catch (IOException e) {
        throw new IllegalStateException("Invalid metadata json: " + new String(bytes, StandardCharsets.UTF_8), e);
      }
    }
  }
}
