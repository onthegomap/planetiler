package com.onthegomap.planetiler.pmtiles;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.onthegomap.planetiler.reader.FileFormatException;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

class PmtilesTest {

  @Test
  void testRoundtripHeader() {

    byte specVersion = 3;
    long rootDirOffset = 1;
    long rootDirLength = 2;
    long jsonMetadataOffset = 3;
    long jsonMetadataLength = 4;
    long leafDirectoriesOffset = 5;
    long leafDirectoriesLength = 6;
    long tileDataOffset = 7;
    long tileDataLength = 8;
    long numAddressedTiles = 9;
    long numTileEntries = 10;
    long numTileContents = 11;
    boolean clustered = true;
    Pmtiles.Compression internalCompression = Pmtiles.Compression.GZIP;
    Pmtiles.Compression tileCompression = Pmtiles.Compression.GZIP;
    Pmtiles.TileType tileType = Pmtiles.TileType.MVT;
    byte minZoom = 1;
    byte maxZoom = 3;
    int minLonE7 = -10_000_000;
    int minLatE7 = -20_000_000;
    int maxLonE7 = 10_000_000;
    int maxLatE7 = 20_000_000;
    byte centerZoom = 2;
    int centerLonE7 = -5_000_000;
    int centerLatE7 = -6_000_000;

    Pmtiles.Header in = new Pmtiles.Header(
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
    Pmtiles.Header out = Pmtiles.Header.fromBytes(in.toBytes());
    assertEquals(specVersion, out.specVersion());
    assertEquals(rootDirOffset, out.rootDirOffset());
    assertEquals(rootDirLength, out.rootDirLength());
    assertEquals(jsonMetadataOffset, out.jsonMetadataOffset());
    assertEquals(jsonMetadataLength, out.jsonMetadataLength());
    assertEquals(leafDirectoriesOffset, out.leafDirectoriesOffset());
    assertEquals(leafDirectoriesLength, out.leafDirectoriesLength());
    assertEquals(tileDataOffset, out.tileDataOffset());
    assertEquals(tileDataLength, out.tileDataLength());
    assertEquals(numAddressedTiles, out.numAddressedTiles());
    assertEquals(numTileEntries, out.numTileEntries());
    assertEquals(numTileContents, out.numTileContents());
    assertEquals(clustered, out.clustered());
    assertEquals(internalCompression, out.internalCompression());
    assertEquals(tileCompression, out.tileCompression());
    assertEquals(tileType, out.tileType());
    assertEquals(minZoom, out.minZoom());
    assertEquals(maxZoom, out.maxZoom());
    assertEquals(minLonE7, out.minLonE7());
    assertEquals(minLatE7, out.minLatE7());
    assertEquals(maxLonE7, out.maxLonE7());
    assertEquals(maxLatE7, out.maxLatE7());
    assertEquals(centerZoom, out.centerZoom());
    assertEquals(centerLonE7, out.centerLonE7());
    assertEquals(centerLatE7, out.centerLatE7());
  }

  @Test
  void testBadHeader() {
    assertThrows(FileFormatException.class, () -> Pmtiles.Header.fromBytes(new byte[0]));
    assertThrows(FileFormatException.class, () -> Pmtiles.Header.fromBytes(new byte[127]));
  }

  @Test
  void testRoundtripDirectoryMinimal() {
    ArrayList<Pmtiles.Entry> in = new ArrayList<>();
    in.add(new Pmtiles.Entry(0, 0, 1, 1));

    List<Pmtiles.Entry> out = Pmtiles.deserializeDirectory(Pmtiles.serializeDirectory(in));
    assertEquals(in, out);
  }

  @Test
  void testRoundtripDirectorySimple() {
    ArrayList<Pmtiles.Entry> in = new ArrayList<>();

    // make sure there are cases of contiguous entries and non-contiguous entries.
    in.add(new Pmtiles.Entry(0, 0, 1, 0));
    in.add(new Pmtiles.Entry(1, 1, 1, 1));
    in.add(new Pmtiles.Entry(2, 3, 1, 1));

    List<Pmtiles.Entry> out = Pmtiles.deserializeDirectory(Pmtiles.serializeDirectory(in));
    assertEquals(in, out);
    out = Pmtiles.deserializeDirectory(Pmtiles.serializeDirectory(in, 0, in.size()));
    assertEquals(in, out);
  }

  @Test
  void testRoundtripDirectorySlice() {
    ArrayList<Pmtiles.Entry> in = new ArrayList<>();

    // make sure there are cases of contiguous entries and non-contiguous entries.
    in.add(new Pmtiles.Entry(0, 0, 1, 0));
    in.add(new Pmtiles.Entry(1, 1, 1, 1));
    in.add(new Pmtiles.Entry(2, 3, 1, 1));

    List<Pmtiles.Entry> out = Pmtiles.deserializeDirectory(Pmtiles.serializeDirectory(in, 1, 2));
    assertEquals(1, out.size());
  }
}
