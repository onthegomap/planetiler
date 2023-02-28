package com.onthegomap.planetiler.pmtiles;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.reader.FileFormatException;
import com.onthegomap.planetiler.util.LayerStats;
import com.onthegomap.planetiler.util.SeekableInMemoryByteChannel;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
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
    WriteablePmtiles.Compression internalCompression = WriteablePmtiles.Compression.GZIP;
    WriteablePmtiles.Compression tileCompression = WriteablePmtiles.Compression.GZIP;
    WriteablePmtiles.TileType tileType = WriteablePmtiles.TileType.MVT;
    byte minZoom = 1;
    byte maxZoom = 3;
    int minLonE7 = -10_000_000;
    int minLatE7 = -20_000_000;
    int maxLonE7 = 10_000_000;
    int maxLatE7 = 20_000_000;
    byte centerZoom = 2;
    int centerLonE7 = -5_000_000;
    int centerLatE7 = -6_000_000;

    WriteablePmtiles.Header in = new WriteablePmtiles.Header(
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
    WriteablePmtiles.Header out = WriteablePmtiles.Header.fromBytes(in.toBytes());
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
    assertThrows(FileFormatException.class, () -> WriteablePmtiles.Header.fromBytes(new byte[0]));
    assertThrows(FileFormatException.class, () -> WriteablePmtiles.Header.fromBytes(new byte[127]));
  }

  @Test
  void testRoundtripDirectoryMinimal() {
    ArrayList<WriteablePmtiles.Entry> in = new ArrayList<>();
    in.add(new WriteablePmtiles.Entry(0, 0, 1, 1));

    List<WriteablePmtiles.Entry> out = WriteablePmtiles.deserializeDirectory(WriteablePmtiles.serializeDirectory(in));
    assertEquals(in, out);
  }

  @Test
  void testRoundtripDirectorySimple() {
    ArrayList<WriteablePmtiles.Entry> in = new ArrayList<>();

    // make sure there are cases of contiguous entries and non-contiguous entries.
    in.add(new WriteablePmtiles.Entry(0, 0, 1, 0));
    in.add(new WriteablePmtiles.Entry(1, 1, 1, 1));
    in.add(new WriteablePmtiles.Entry(2, 3, 1, 1));

    List<WriteablePmtiles.Entry> out = WriteablePmtiles.deserializeDirectory(WriteablePmtiles.serializeDirectory(in));
    assertEquals(in, out);
    out = WriteablePmtiles.deserializeDirectory(WriteablePmtiles.serializeDirectory(in, 0, in.size()));
    assertEquals(in, out);
  }

  @Test
  void testRoundtripDirectorySlice() {
    ArrayList<WriteablePmtiles.Entry> in = new ArrayList<>();

    // make sure there are cases of contiguous entries and non-contiguous entries.
    in.add(new WriteablePmtiles.Entry(0, 0, 1, 0));
    in.add(new WriteablePmtiles.Entry(1, 1, 1, 1));
    in.add(new WriteablePmtiles.Entry(2, 3, 1, 1));

    List<WriteablePmtiles.Entry> out = WriteablePmtiles.deserializeDirectory(
      WriteablePmtiles.serializeDirectory(in, 1, 2));
    assertEquals(1, out.size());
  }

  @Test
  void testBuildDirectoriesRootOnly() throws IOException {

  }

  @Test
  void testBuildDirectoriesLeaves() throws IOException {

  }

  @Test
  void testWritePmtilesSingleEntry() throws IOException {
    var bytes = new SeekableInMemoryByteChannel(0);
    var in = WriteablePmtiles.newWriteToMemory(bytes);

    var config = PlanetilerConfig.defaults();
    in.initialize(config, new TileArchiveMetadata(new Profile.NullProfile()), new LayerStats());
    var writer = in.newTileWriter();
    writer.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0xa, 0x2}, OptionalLong.empty()));

    // TODO shouldn't depend on config
    in.finish(config);
    var reader = new ReadablePmtiles(bytes);
    var header = reader.getHeader();
    assertEquals(1, header.numAddressedTiles());
    assertEquals(1, header.numTileContents());
    assertEquals(1, header.numTileEntries());
    assertArrayEquals(new byte[]{0xa, 0x2}, reader.getTile(0, 0, 0));

    Set<TileCoord> coordset = reader.getAllTileCoords().stream().collect(Collectors.toSet());
    assertEquals(1, coordset.size());
  }

  @Test
  void testWritePmtilesDuplication() throws IOException {
    var bytes = new SeekableInMemoryByteChannel(0);
    var in = WriteablePmtiles.newWriteToMemory(bytes);

    var config = PlanetilerConfig.defaults();
    in.initialize(config, new TileArchiveMetadata(new Profile.NullProfile()), new LayerStats());
    var writer = in.newTileWriter();
    writer.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0xa, 0x2}, OptionalLong.of(42)));
    writer.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 1), new byte[]{0xa, 0x2}, OptionalLong.of(42)));
    writer.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 2), new byte[]{0xa, 0x2}, OptionalLong.of(42)));

    // TODO shouldn't depend on config
    in.finish(config);
    var reader = new ReadablePmtiles(bytes);
    var header = reader.getHeader();
    assertEquals(3, header.numAddressedTiles());
    assertEquals(1, header.numTileContents());
    assertEquals(2, header.numTileEntries()); // z0 and z1 are contiguous
    assertArrayEquals(new byte[]{0xa, 0x2}, reader.getTile(0, 0, 0));
    assertArrayEquals(new byte[]{0xa, 0x2}, reader.getTile(0, 0, 1));
    assertArrayEquals(new byte[]{0xa, 0x2}, reader.getTile(0, 0, 2));

    Set<TileCoord> coordset = reader.getAllTileCoords().stream().collect(Collectors.toSet());
    assertEquals(3, coordset.size());
  }

  @Test
  void testWritePmtilesLeafDirectories() throws IOException {
    var bytes = new SeekableInMemoryByteChannel(0);
    var in = WriteablePmtiles.newWriteToMemory(bytes);

    var config = PlanetilerConfig.defaults();
    in.initialize(config, new TileArchiveMetadata(new Profile.NullProfile()), new LayerStats());
    var writer = in.newTileWriter();

    int ENTRIES = 20000;

    for (int i = 0; i < ENTRIES; i++) {
      writer.write(new TileEncodingResult(TileCoord.hilbertDecode(i), ByteBuffer.allocate(4).putInt(i).array(),
        OptionalLong.empty()));
    }

    // TODO shouldn't depend on config
    in.finish(config);
    var reader = new ReadablePmtiles(bytes);
    var header = reader.getHeader();
    assertEquals(ENTRIES, header.numAddressedTiles());
    assertEquals(ENTRIES, header.numTileContents());
    assertEquals(ENTRIES, header.numTileEntries());
    assert (header.leafDirectoriesLength() > 0);

    for (int i = 0; i < ENTRIES; i++) {
      var coord = TileCoord.hilbertDecode(i);
      assertArrayEquals(ByteBuffer.allocate(4).putInt(i).array(), reader.getTile(coord.x(), coord.y(), coord.z()));
    }

    Set<TileCoord> coordset = reader.getAllTileCoords().stream().collect(Collectors.toSet());
    assertEquals(ENTRIES, coordset.size());
  }

}
