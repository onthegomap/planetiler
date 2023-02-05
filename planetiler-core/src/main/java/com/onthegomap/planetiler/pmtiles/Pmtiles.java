package com.onthegomap.planetiler.pmtiles;

import com.carrotsearch.hppc.ByteArrayList;
import com.onthegomap.planetiler.reader.FileFormatException;
import com.onthegomap.planetiler.util.VarInt;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * PMTiles is a single-file tile archive format designed for efficient access on cloud storage.
 *
 * @see <a href="https://github.com/protomaps/PMTiles/blob/main/spec/v3/spec.md">PMTiles Specification</a>
 */
public final class Pmtiles {
  static final int HEADER_LEN = 127;

  public static class Entry implements Comparable<Entry> {
    private long tileId;
    private long offset;
    private int length;
    private int runLength;

    public Entry(long tileId, long offset, int length, int runLength) {
      this.tileId = tileId;
      this.offset = offset;
      this.length = length;
      this.runLength = runLength;
    }

    public long getTileId() {
      return tileId;
    }

    public long getOffset() {
      return offset;
    }

    public long getLength() {
      return length;
    }

    public long getRunLength() {
      return runLength;
    }

    @Override
    public boolean equals(Object o) {
      if (this == o) {
        return true;
      }
      if (o == null || getClass() != o.getClass()) {
        return false;
      }

      Entry that = (Entry) o;

      if (tileId != that.tileId) {
        return false;
      }

      if (offset != that.offset) {
        return false;
      }

      if (length != that.length) {
        return false;
      }

      return runLength == that.runLength;
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
  }

  /**
   * Convert a directory of entries to bytes.
   *
   * @param slice a list of entries sorted by ascending {@code tileId} with size > 0.
   * @param start the start index to serialize, inclusive.
   * @param end   the end index, exclusive.
   * @return the uncompressed bytes of the directory.
   */
  public static byte[] serializeDirectory(List<Entry> slice, int start, int end) {
    ByteArrayList dir = new ByteArrayList();

    VarInt.putVarLong((long) end - start, dir);

    long lastId = 0;
    for (int i = start; i < end; i++) {
      VarInt.putVarLong(slice.get(i).tileId - lastId, dir);
      lastId = slice.get(i).tileId;
    }

    for (int i = start; i < end; i++) {
      VarInt.putVarLong(slice.get(i).runLength, dir);
    }

    for (int i = start; i < end; i++) {
      VarInt.putVarLong(slice.get(i).length, dir);
    }

    for (int i = start; i < end; i++) {
      if (i > start && slice.get(i).offset == slice.get(i - 1).offset + slice.get(i - 1).length) {
        VarInt.putVarLong(0, dir);
      } else {
        VarInt.putVarLong(slice.get(i).offset + 1, dir);
      }
    }

    return dir.toArray();
  }

  public static List<Entry> deserializeDirectory(byte[] bytes) {
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
}
