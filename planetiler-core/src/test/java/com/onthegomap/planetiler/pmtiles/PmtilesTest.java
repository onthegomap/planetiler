package com.onthegomap.planetiler.pmtiles;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.carrotsearch.hppc.ObjectArrayList;
import org.junit.jupiter.api.Test;

class PmtilesTest {

  @Test
  void testRoundtripHeader() {
    Pmtiles.Header in = new Pmtiles.Header();
    in.specVersion = 3;
    in.rootDirOffset = 1;
    in.rootDirLength = 2;
    in.jsonMetadataOffset = 3;
    in.jsonMetadataLength = 4;
    in.leafDirectoriesOffset = 5;
    in.leafDirectoriesLength = 6;
    in.tileDataOffset = 7;
    in.tileDataLength = 8;
    in.numAddressedTiles = 9;
    in.numTileEntries = 10;
    in.numTileContents = 11;
    in.clustered = true;
    in.internalCompression = Pmtiles.Compression.GZIP;
    in.tileCompression = Pmtiles.Compression.GZIP;
    in.tileType = Pmtiles.TileType.MVT;
    in.minZoom = 1;
    in.maxZoom = 3;
    in.minLonE7 = -10_000_000;
    in.minLatE7 = -20_000_000;
    in.maxLonE7 = 10_000_000;
    in.maxLatE7 = 20_000_000;
    in.centerZoom = 2;
    in.centerLonE7 = -5_000_000;
    in.centerLatE7 = -6_000_000;
    Pmtiles.Header out = Pmtiles.Header.fromBytes(in.toBytes());
    assertEquals(in.specVersion, out.specVersion);
    assertEquals(in.rootDirOffset, out.rootDirOffset);
    assertEquals(in.rootDirLength, out.rootDirLength);
    assertEquals(in.jsonMetadataOffset, out.jsonMetadataOffset);
    assertEquals(in.jsonMetadataLength, out.jsonMetadataLength);
    assertEquals(in.leafDirectoriesOffset, out.leafDirectoriesOffset);
    assertEquals(in.leafDirectoriesLength, out.leafDirectoriesLength);
    assertEquals(in.tileDataOffset, out.tileDataOffset);
    assertEquals(in.tileDataLength, out.tileDataLength);
    assertEquals(in.numAddressedTiles, out.numAddressedTiles);
    assertEquals(in.numTileEntries, out.numTileEntries);
    assertEquals(in.numTileContents, out.numTileContents);
    assertEquals(in.clustered, out.clustered);
    assertEquals(in.internalCompression, out.internalCompression);
    assertEquals(in.tileCompression, out.tileCompression);
    assertEquals(in.tileType, out.tileType);
    assertEquals(in.minZoom, out.minZoom);
    assertEquals(in.maxZoom, out.maxZoom);
    assertEquals(in.minLonE7, out.minLonE7);
    assertEquals(in.minLatE7, out.minLatE7);
    assertEquals(in.maxLonE7, out.maxLonE7);
    assertEquals(in.maxLatE7, out.maxLatE7);
    assertEquals(in.centerZoom, out.centerZoom);
    assertEquals(in.centerLonE7, out.centerLonE7);
    assertEquals(in.centerLatE7, out.centerLatE7);
  }

  @Test
  void testRoundtripDirectoryMinimal() {
    ObjectArrayList<Pmtiles.Entry> in = new ObjectArrayList<>();
    in.add(new Pmtiles.Entry(0, 0, 1, 1));

    ObjectArrayList<Pmtiles.Entry> out = Pmtiles.deserializeDirectory(Pmtiles.serializeDirectory(in, 0, in.size()));
    assertEquals(in, out);
  }

  @Test
  void testRoundtripDirectorySimple() {
    ObjectArrayList<Pmtiles.Entry> in = new ObjectArrayList<>();

    // make sure there are cases of contiguous entries and non-contiguous entries.
    in.add(new Pmtiles.Entry(0, 0, 1, 0));
    in.add(new Pmtiles.Entry(1, 1, 1, 1));
    in.add(new Pmtiles.Entry(2, 3, 1, 1));

    ObjectArrayList<Pmtiles.Entry> out = Pmtiles.deserializeDirectory(Pmtiles.serializeDirectory(in, 0, in.size()));
    assertEquals(in, out);
  }

  @Test
  void testRoundtripDirectorySlice() {
    ObjectArrayList<Pmtiles.Entry> in = new ObjectArrayList<>();

    // make sure there are cases of contiguous entries and non-contiguous entries.
    in.add(new Pmtiles.Entry(0, 0, 1, 0));
    in.add(new Pmtiles.Entry(1, 1, 1, 1));
    in.add(new Pmtiles.Entry(2, 3, 1, 1));

    ObjectArrayList<Pmtiles.Entry> out = Pmtiles.deserializeDirectory(Pmtiles.serializeDirectory(in, 1, 2));
    assertEquals(1, out.size());
  }
}
