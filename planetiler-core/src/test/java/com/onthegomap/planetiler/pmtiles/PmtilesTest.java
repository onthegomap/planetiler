package com.onthegomap.planetiler.pmtiles;

import static org.junit.jupiter.api.Assertions.*;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.onthegomap.planetiler.Profile;
import com.onthegomap.planetiler.TestUtils;
import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileCompression;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.reader.FileFormatException;
import com.onthegomap.planetiler.util.LayerStats;
import com.onthegomap.planetiler.util.SeekableInMemoryByteChannel;
import java.io.Closeable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;

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

    List<Pmtiles.Entry> out = Pmtiles.directoryFromBytes(Pmtiles.directoryToBytes(in));
    assertEquals(in, out);
  }

  @Test
  void testRoundtripDirectorySimple() {
    ArrayList<Pmtiles.Entry> in = new ArrayList<>();

    // make sure there are cases of contiguous entries and non-contiguous entries.
    in.add(new Pmtiles.Entry(0, 0, 1, 0));
    in.add(new Pmtiles.Entry(1, 1, 1, 1));
    in.add(new Pmtiles.Entry(2, 3, 1, 1));

    List<Pmtiles.Entry> out = Pmtiles.directoryFromBytes(Pmtiles.directoryToBytes(in));
    assertEquals(in, out);
    out = Pmtiles.directoryFromBytes(Pmtiles.directoryToBytes(in, 0, in.size()));
    assertEquals(in, out);
  }

  @Test
  void testRoundtripDirectorySlice() {
    ArrayList<Pmtiles.Entry> in = new ArrayList<>();

    // make sure there are cases of contiguous entries and non-contiguous entries.
    in.add(new Pmtiles.Entry(0, 0, 1, 0));
    in.add(new Pmtiles.Entry(1, 1, 1, 1));
    in.add(new Pmtiles.Entry(2, 3, 1, 1));

    List<Pmtiles.Entry> out = Pmtiles.directoryFromBytes(
      Pmtiles.directoryToBytes(in, 1, 2));
    assertEquals(1, out.size());
  }

  @Test
  void testBuildDirectoriesRootOnly() throws IOException {
    ArrayList<Pmtiles.Entry> in = new ArrayList<>();
    for (int i = 0; i < 1000; i++) {
      in.add(new Pmtiles.Entry(i, i * 100, 100, 1));
    }
    var result = WriteablePmtiles.makeDirectories(in);
    assertEquals(1, result.numAttempts());
  }

  @Test
  void testBuildDirectoriesLeavesNotTooSmall() throws IOException {
    ArrayList<Pmtiles.Entry> in = new ArrayList<>();
    for (int i = 0; i < 100000; i++) {
      in.add(new Pmtiles.Entry(i, i * 100, 100, 1));
    }
    var result = WriteablePmtiles.makeDirectories(in);
    assertTrue(result.leafSize() >= 4096, "entries in leaf: " + result.leafSize());
  }

  record FakeFile(SeekableInMemoryByteChannel b) implements ReadablePmtiles.DataProvider, Closeable {

    @Override
    public byte[] getBytes(long start, int length) throws IOException {
      var buf = ByteBuffer.allocate(length);
      b.position(start);
      b.read(buf);
      return buf.array();
    }

    @Override
    public void close() throws IOException {
      b.close();
    }
  }

  @Test
  void testWritePmtilesSingleEntry() throws IOException {
    var bytes = new SeekableInMemoryByteChannel(0);
    var in = WriteablePmtiles.newWriteToMemory(bytes);

    var config = PlanetilerConfig.defaults();
    var metadata = new TileArchiveMetadata(new Profile.NullProfile(), config);
    in.initialize(metadata);
    var writer = in.newTileWriter();
    writer.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 1), new byte[]{0xa, 0x2}, OptionalLong.empty()));

    in.finish(metadata);
    try (var reader = new ReadablePmtiles(new FakeFile(bytes), () -> {
    })) {
      var header = reader.getHeader();
      assertEquals(1, header.numAddressedTiles());
      assertEquals(1, header.numTileContents());
      assertEquals(1, header.numTileEntries());
      assertArrayEquals(new byte[]{0xa, 0x2}, reader.getTile(0, 0, 1));
      assertNull(reader.getTile(0, 0, 0));
      assertNull(reader.getTile(0, 0, 2));

      Set<TileCoord> coordset = reader.getAllTileCoords().stream().collect(Collectors.toSet());
      assertEquals(Set.of(TileCoord.ofXYZ(0, 0, 1)), coordset);
      Set<Tile> tileset = reader.getAllTiles().stream().collect(Collectors.toSet());
      assertEquals(Set.of(
        new Tile(TileCoord.ofXYZ(0, 0, 1), new byte[]{0xa, 0x2})
      ), tileset);
    }
  }

  @Test
  void testRoundtripMetadata() throws IOException {
    roundTripMetadata(new TileArchiveMetadata(
      "MyName",
      "MyDescription",
      "MyAttribution",
      "MyVersion",
      "baselayer",
      TileArchiveMetadata.MVT_FORMAT,
      new Envelope(1, 2, 3, 4),
      new CoordinateXY(5, 6),
      7d,
      8,
      9,
      List.of(new LayerStats.VectorLayer("MyLayer", Map.of())),
      Map.of("other key", "other value"),
      TileCompression.GZIP
    ));
  }

  @ParameterizedTest
  @EnumSource(value = TileCompression.class, names = {"GZIP", "NONE"})
  void testRoundtripMetadataMinimal(TileCompression tileCompression) throws IOException {
    roundTripMetadata(
      new TileArchiveMetadata(null, null, null, null, null, null, null, null, null, null, null, null, Map.of(),
        tileCompression),
      new TileArchiveMetadata(null, null, null, null, null, null,
        new Envelope(-180, 180, -85.0511287, 85.0511287),
        new CoordinateXY(0, 0),
        0d,
        0,
        15,
        null,
        Map.of(),
        tileCompression
      )
    );
  }

  private static void roundTripMetadata(TileArchiveMetadata metadata) throws IOException {
    roundTripMetadata(metadata, metadata);
  }

  private static void roundTripMetadata(TileArchiveMetadata input, TileArchiveMetadata output) throws IOException {
    try (
      var channel = new SeekableInMemoryByteChannel(0);
      var in = WriteablePmtiles.newWriteToMemory(channel)
    ) {
      in.initialize(input);
      var writer = in.newTileWriter();
      writer.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0xa, 0x2}, OptionalLong.empty()));

      in.finish(input);
      try (var reader = new ReadablePmtiles(new FakeFile(channel), () -> {
      })) {
        assertArrayEquals(new byte[]{0xa, 0x2}, reader.getTile(0, 0, 0));

        assertEquals(output, reader.metadata());
      }
    }
  }

  @Test
  void testPmtilesMetadataTopLevelKeys() throws IOException {
    var hashMap = new HashMap<String, String>();
    hashMap.put("testkey", "testvalue");
    var metadata = new Pmtiles.JsonMetadata(List.of(), hashMap);
    var bytes = metadata.toBytes();
    ObjectMapper mapper = new ObjectMapper();
    var node = mapper.readTree(bytes);
    assertEquals("testvalue", node.get("testkey").asText());
  }

  @Test
  void testReadPmtilesFromTippecanoe() throws IOException {
    // '{"type":"Polygon","coordinates":[[[0,0],[0,1],[1,1],[1,0],[0,0]]]}' | ./tippecanoe -zg -o box1degree.pmtiles
    ReadablePmtiles reader = ReadablePmtiles.newReadFromFile(TestUtils.pathToResource("box1degree.pmtiles"));
    var header = reader.getHeader();
    assertTrue(header.maxZoom() <= 15);
    assertNotNull(reader.getTile(0, 0, 0));
  }

  @Test
  void testWritePmtilesDuplication() throws IOException {
    var bytes = new SeekableInMemoryByteChannel(0);
    var in = WriteablePmtiles.newWriteToMemory(bytes);

    var config = PlanetilerConfig.defaults();
    var metadata = new TileArchiveMetadata(new Profile.NullProfile(), config);
    in.initialize(metadata);
    var writer = in.newTileWriter();
    writer.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0xa, 0x2}, OptionalLong.of(42)));
    writer.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 1), new byte[]{0xa, 0x2}, OptionalLong.of(42)));
    writer.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 2), new byte[]{0xa, 0x2}, OptionalLong.of(42)));

    in.finish(metadata);
    try (var reader = new ReadablePmtiles(new FakeFile(bytes), () -> {
    })) {
      var header = reader.getHeader();
      assertEquals(3, header.numAddressedTiles());
      assertEquals(1, header.numTileContents());
      assertEquals(2, header.numTileEntries()); // z0 and z1 are contiguous
      assertArrayEquals(new byte[]{0xa, 0x2}, reader.getTile(0, 0, 0));
      assertArrayEquals(new byte[]{0xa, 0x2}, reader.getTile(0, 0, 1));
      assertArrayEquals(new byte[]{0xa, 0x2}, reader.getTile(0, 0, 2));

      Set<TileCoord> coordset = reader.getAllTileCoords().stream().collect(Collectors.toSet());
      assertEquals(Set.of(
        TileCoord.ofXYZ(0, 0, 0),
        TileCoord.ofXYZ(0, 0, 1),
        TileCoord.ofXYZ(0, 0, 2)
      ), coordset);
      var tileset = reader.getAllTiles().stream().collect(Collectors.toSet());
      assertEquals(Set.of(
        new Tile(TileCoord.ofXYZ(0, 0, 0), new byte[]{0xa, 0x2}),
        new Tile(TileCoord.ofXYZ(0, 0, 1), new byte[]{0xa, 0x2}),
        new Tile(TileCoord.ofXYZ(0, 0, 2), new byte[]{0xa, 0x2})
      ), tileset);
      var tileset2 = new TreeSet<>();
      reader.forEachTile(tileset2::add);
      assertEquals(tileset, tileset2);
    }
  }

  @Test
  void testWritePmtilesUnclustered() throws IOException {
    var bytes = new SeekableInMemoryByteChannel(0);
    var in = WriteablePmtiles.newWriteToMemory(bytes);

    var config = PlanetilerConfig.defaults();
    var metadata = new TileArchiveMetadata(new Profile.NullProfile(), config);
    in.initialize(metadata);
    var writer = in.newTileWriter();
    writer.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 1), new byte[]{0xa, 0x2}, OptionalLong.of(42)));
    writer.write(new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0xa, 0x2}, OptionalLong.of(42)));

    in.finish(metadata);
    try (var reader = new ReadablePmtiles(new FakeFile(bytes), () -> {
    })) {
      var header = reader.getHeader();
      assertEquals(2, header.numAddressedTiles());
      assertEquals(1, header.numTileContents());
      assertEquals(2, header.numTileEntries());
      assertFalse(header.clustered());
      assertArrayEquals(new byte[]{0xa, 0x2}, reader.getTile(0, 0, 0));
      assertArrayEquals(new byte[]{0xa, 0x2}, reader.getTile(0, 0, 1));

      Set<TileCoord> coordset = reader.getAllTileCoords().stream().collect(Collectors.toSet());
      assertEquals(Set.of(
        TileCoord.ofXYZ(0, 0, 0),
        TileCoord.ofXYZ(0, 0, 1)
      ), coordset);
      var tileset = reader.getAllTiles().stream().collect(Collectors.toSet());
      assertEquals(Set.of(
        new Tile(TileCoord.ofXYZ(0, 0, 0), new byte[]{0xa, 0x2}),
        new Tile(TileCoord.ofXYZ(0, 0, 1), new byte[]{0xa, 0x2})
      ), tileset);
    }
  }

  @Test
  void testWritePmtilesLeafDirectories() throws IOException {
    var bytes = new SeekableInMemoryByteChannel(0);
    var in = WriteablePmtiles.newWriteToMemory(bytes);

    var config = PlanetilerConfig.defaults();
    var metadata = new TileArchiveMetadata(new Profile.NullProfile(), config);
    in.initialize(metadata);
    var writer = in.newTileWriter();

    int ENTRIES = 20000;
    Set<TileCoord> expectedCoords = new TreeSet<>();
    Set<Tile> expectedTiles = new TreeSet<>();

    for (int i = 0; i < ENTRIES; i++) {
      var coord = TileCoord.hilbertDecode(i);
      var data = ByteBuffer.allocate(4).putInt(i).array();
      expectedCoords.add(coord);
      expectedTiles.add(new Tile(coord, data));
      writer.write(new TileEncodingResult(coord, data, OptionalLong.empty()));
    }

    in.finish(metadata);
    try (var reader = new ReadablePmtiles(new FakeFile(bytes), () -> {
    })) {
      var header = reader.getHeader();
      assertEquals(ENTRIES, header.numAddressedTiles());
      assertEquals(ENTRIES, header.numTileContents());
      assertEquals(ENTRIES, header.numTileEntries());
      assertTrue(header.leafDirectoriesLength() > 0);

      for (int i = 0; i < ENTRIES; i++) {
        var coord = TileCoord.hilbertDecode(i);
        assertArrayEquals(ByteBuffer.allocate(4).putInt(i).array(), reader.getTile(coord.x(), coord.y(), coord.z()),
          "tileCoord=%s did not match".formatted(coord.toString()));
      }

      Set<TileCoord> coordset = reader.getAllTileCoords().stream().collect(Collectors.toCollection(TreeSet::new));
      assertEquals(expectedCoords, coordset);

      Set<Tile> tileset = reader.getAllTiles().stream().collect(Collectors.toCollection(TreeSet::new));
      assertEquals(expectedTiles, tileset);

      Set<Tile> tileset2 = new TreeSet<>();
      reader.forEachTile(tileset2::add);
      assertEquals(expectedTiles, tileset2);

      for (int i = 0; i < ENTRIES; i++) {
        var coord = TileCoord.hilbertDecode(i);
        assertTrue(coordset.contains(coord), "tileCoord=%s not in result".formatted(coord.toString()));
      }
    }
  }
}
