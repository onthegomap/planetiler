package com.onthegomap.planetiler.pmtiles;

import com.carrotsearch.hppc.ByteArrayList;
import com.carrotsearch.hppc.ObjectArrayList;
import com.onthegomap.planetiler.util.VarInt;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * PMTiles is a single-file tile archive format designed for efficient access on cloud storage.
 *
 * @see <a href="https://github.com/protomaps/PMTiles/blob/main/spec/v3/spec.md">PMTiles Specification</a>
 */
public final class Pmtiles {
  static final int HEADER_LEN = 127;

  public static class Entry implements Comparable<Entry> {
    long tileId;
    long offset;
    int length;
    public int runLength;

    public Entry(long tileId, long offset, int length, int runLength) {
      this.tileId = tileId;
      this.offset = offset;
      this.length = length;
      this.runLength = runLength;
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
    UNKNOWN,
    NONE,
    GZIP;

    public static Compression FromByte(byte val) {
      return switch (val) {
        case 1 -> NONE;
        case 2 -> GZIP;
        default -> UNKNOWN;
      };
    }
  }

  public enum TileType {
    UNKNOWN,
    MVT;

    public static TileType FromByte(byte val) {
      return switch (val) {
        case 1 -> MVT;
        default -> UNKNOWN;
      };
    }
  }

  public static class Header {
    byte specVersion;
    long rootDirOffset;
    long rootDirLength;
    long jsonMetadataOffset;
    long jsonMetadataLength;
    long leafDirectoriesOffset;
    long leafDirectoriesLength;
    long tileDataOffset;
    long tileDataLength;
    long numAddressedTiles;
    long numTileEntries;
    long numTileContents;
    boolean clustered;
    Compression internalCompression;
    Compression tileCompression;
    TileType tileType;
    byte minZoom;
    byte maxZoom;

    // Store a decimal longitude as a signed 32-bit integer by multiplying by 10,000,000.
    int minLonE7;
    int minLatE7;
    int maxLonE7;
    int maxLatE7;
    byte centerZoom;
    int centerLonE7;
    int centerLatE7;

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
      buf.put((byte) internalCompression.ordinal());
      buf.put((byte) tileCompression.ordinal());
      buf.put((byte) tileType.ordinal());
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
      Header header = new Header();
      byte[] magic = new byte[7];
      buffer.get(magic);
      System.out.println(magic);
      header.specVersion = buffer.get();
      header.rootDirOffset = buffer.getLong();
      header.rootDirLength = buffer.getLong();
      header.jsonMetadataOffset = buffer.getLong();
      header.jsonMetadataLength = buffer.getLong();
      header.leafDirectoriesOffset = buffer.getLong();
      header.leafDirectoriesLength = buffer.getLong();
      header.tileDataOffset = buffer.getLong();
      header.tileDataLength = buffer.getLong();
      header.numAddressedTiles = buffer.getLong();
      header.numTileEntries = buffer.getLong();
      header.numTileContents = buffer.getLong();
      header.clustered = (buffer.get() == 0x1);
      header.internalCompression = Compression.FromByte(buffer.get());
      header.tileCompression = Compression.FromByte(buffer.get());
      header.tileType = TileType.FromByte(buffer.get());
      header.minZoom = buffer.get();
      header.maxZoom = buffer.get();
      header.minLonE7 = buffer.getInt();
      header.minLatE7 = buffer.getInt();
      header.maxLonE7 = buffer.getInt();
      header.maxLatE7 = buffer.getInt();
      header.centerZoom = buffer.get();
      header.centerLonE7 = buffer.getInt();
      header.centerLatE7 = buffer.getInt();
      return header;
    }
  }

  /**
   * Convert a directory of entries to bytes.
   *
   * @param slice a list of entries sorted by ascending {@code tileId} with size > 0.
   * @param start the start index to serialize, inclusive.
   * @param end   the end index, exclusive.
   * @return
   */
  public static byte[] serializeDirectory(ObjectArrayList<Entry> slice, int start, int end) {
    ByteArrayList dir = new ByteArrayList();

    VarInt.putVarLong(end - start, dir);

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

  public static ObjectArrayList<Entry> deserializeDirectory(byte[] bytes) {
    ByteBuffer buffer = ByteBuffer.wrap(bytes);
    int num_entries = (int) VarInt.getVarLong(buffer);
    ObjectArrayList<Entry> result = new ObjectArrayList<>(num_entries);

    long last_id = 0;
    for (int i = 0; i < num_entries; i++) {
      long tile_id = last_id + VarInt.getVarLong(buffer);
      result.add(new Entry(tile_id, 0, 0, 0));
      last_id = tile_id;
    }

    for (int i = 0; i < num_entries; i++) {
      result.get(i).runLength = (int) VarInt.getVarLong(buffer);
    }

    for (int i = 0; i < num_entries; i++) {
      result.get(i).length = (int) VarInt.getVarLong(buffer);
    }

    for (int i = 0; i < num_entries; i++) {
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
