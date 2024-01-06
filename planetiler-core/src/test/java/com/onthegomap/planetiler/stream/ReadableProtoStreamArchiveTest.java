package com.onthegomap.planetiler.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ByteString;
import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.proto.StreamArchiveProto;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ReadableProtoStreamArchiveTest {

  @ParameterizedTest
  @ValueSource(booleans = {true, false})
  void testSimple(boolean maxMetaData, @TempDir Path tempDir) throws IOException {

    final StreamArchiveProto.Metadata metadataSerialized = maxMetaData ?
      WriteableProtoStreamArchiveTest.maxMetadataSerialized : WriteableProtoStreamArchiveTest.minMetadataSerialized;

    final TileArchiveMetadata metadataDeserialized = maxMetaData ?
      WriteableProtoStreamArchiveTest.maxMetadataDeserialized : WriteableProtoStreamArchiveTest.minMetadataDeserialized;


    final Path p = tempDir.resolve("out.proto");
    try (var out = Files.newOutputStream(p)) {
      StreamArchiveProto.Entry.newBuilder().setInitialization(
        StreamArchiveProto.InitializationEntry.newBuilder()
      ).build().writeDelimitedTo(out);
      StreamArchiveProto.Entry.newBuilder().setTile(
        StreamArchiveProto.TileEntry.newBuilder()
          .setX(0).setY(0).setZ(0).setEncodedData(ByteString.copyFrom(new byte[]{0}))
      ).build().writeDelimitedTo(out);
      StreamArchiveProto.Entry.newBuilder().setTile(
        StreamArchiveProto.TileEntry.newBuilder()
          .setX(1).setY(2).setZ(3).setEncodedData(ByteString.copyFrom(new byte[]{1}))
      ).build().writeDelimitedTo(out);
      StreamArchiveProto.Entry.newBuilder().setFinish(
        StreamArchiveProto.FinishEntry.newBuilder()
          .setMetadata(metadataSerialized)
      ).build().writeDelimitedTo(out);
    }
    final List<Tile> expectedTiles = List.of(
      new Tile(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}),
      new Tile(TileCoord.ofXYZ(1, 2, 3), new byte[]{1})
    );
    try (var reader = ReadableProtoStreamArchive.newReader(p, new StreamArchiveConfig(false, Arguments.of()))) {
      assertEquals(expectedTiles, reader.getAllTiles().stream().toList());
      assertEquals(expectedTiles, reader.getAllTiles().stream().toList());
      assertEquals(metadataDeserialized, reader.metadata());
    }
  }
}
