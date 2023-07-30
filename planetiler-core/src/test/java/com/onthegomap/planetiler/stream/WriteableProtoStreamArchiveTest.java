package com.onthegomap.planetiler.stream;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ByteString;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.proto.StreamArchiveProto;
import com.onthegomap.planetiler.util.LayerStats;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.OptionalLong;
import java.util.Set;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;

class WriteableProtoStreamArchiveTest {

  private static final StreamArchiveConfig defaultConfig = new StreamArchiveConfig(false, null);
  private static final TileArchiveMetadata maxMetadataIn =
    new TileArchiveMetadata("name", "description", "attribution", "version", "type", "format", new Envelope(0, 1, 2, 3),
      new CoordinateXY(1.3, 3.7), 1.0, 2, 3,
      List.of(
        new LayerStats.VectorLayer("vl0",
          Map.of("1", LayerStats.FieldType.BOOLEAN, "2", LayerStats.FieldType.NUMBER, "3", LayerStats.FieldType.STRING),
          Optional.of("description"), OptionalInt.of(1), OptionalInt.of(2)),
        new LayerStats.VectorLayer("vl1",
          Map.of(),
          Optional.empty(), OptionalInt.empty(), OptionalInt.empty())
      ),
      Map.of("a", "b", "c", "d"));
  private static final StreamArchiveProto.Metadata maxMetadataOut = StreamArchiveProto.Metadata.newBuilder()
    .setName("name").setDescription("description").setAttribution("attribution").setVersion("version")
    .setType("type").setFormat("format")
    .setBounds(StreamArchiveProto.Envelope.newBuilder().setMinX(0).setMaxX(1).setMinY(2).setMaxY(3).build())
    .setCenter(StreamArchiveProto.CoordinateXY.newBuilder().setX(1.3).setY(3.7))
    .setZoom(1.0).setMinZoom(2).setMaxZoom(3)
    .addVectorLayers(
      StreamArchiveProto.VectorLayer.newBuilder()
        .setId("vl0").setDescription("description").setMinZoom(1).setMaxZoom(2)
        .putFields("1", StreamArchiveProto.VectorLayer.FieldType.FIELD_TYPE_BOOLEAN)
        .putFields("2", StreamArchiveProto.VectorLayer.FieldType.FIELD_TYPE_NUMBER)
        .putFields("3", StreamArchiveProto.VectorLayer.FieldType.FIELD_TYPE_STRING)
        .build()
    )
    .addVectorLayers(StreamArchiveProto.VectorLayer.newBuilder().setId("vl1").build())
    .putOthers("a", "b").putOthers("c", "d")
    .build();

  private static final TileArchiveMetadata minMetadataIn =
    new TileArchiveMetadata(null, null, null, null, null, null, null, null, null, null, null, null, null);
  private static final StreamArchiveProto.Metadata minMetadataOut = StreamArchiveProto.Metadata.newBuilder().build();

  @Test
  void testWriteSingleFile(@TempDir Path tempDir) throws IOException {
    final Path csvFile = tempDir.resolve("mbtiles.proto");

    final var tile0 = new TileEncodingResult(TileCoord.ofXYZ(0, 0, 0), new byte[]{0}, OptionalLong.empty());
    final var tile1 = new TileEncodingResult(TileCoord.ofXYZ(1, 2, 3), new byte[]{1}, OptionalLong.of(1));
    try (var archive = WriteableProtoStreamArchive.newWriteToFile(csvFile, defaultConfig)) {
      archive.initialize(maxMetadataIn);
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(tile0);
        tileWriter.write(tile1);
      }
      archive.finish(minMetadataIn);
    }

    try (InputStream in = Files.newInputStream(csvFile)) {
      assertEquals(
        List.of(wrapInit(maxMetadataOut), toEntry(tile0), toEntry(tile1), wrapFinish(minMetadataOut)),
        readAllEntries(in)
      );
    }
  }

  @Test
  void testWriteToMultipleFiles(@TempDir Path tempDir) throws IOException {

    final Path csvFilePrimary = tempDir.resolve("mbtiles.proto");
    final Path csvFileSecondary = tempDir.resolve("mbtiles.proto1");
    final Path csvFileTertiary = tempDir.resolve("mbtiles.proto2");

    final var tile0 = new TileEncodingResult(TileCoord.ofXYZ(11, 12, 1), new byte[]{0}, OptionalLong.empty());
    final var tile1 = new TileEncodingResult(TileCoord.ofXYZ(21, 22, 2), new byte[]{1}, OptionalLong.empty());
    final var tile2 = new TileEncodingResult(TileCoord.ofXYZ(31, 32, 3), new byte[]{2}, OptionalLong.empty());
    final var tile3 = new TileEncodingResult(TileCoord.ofXYZ(41, 42, 4), new byte[]{3}, OptionalLong.empty());
    final var tile4 = new TileEncodingResult(TileCoord.ofXYZ(51, 52, 5), new byte[]{4}, OptionalLong.empty());
    try (var archive = WriteableProtoStreamArchive.newWriteToFile(csvFilePrimary, defaultConfig)) {
      archive.initialize(minMetadataIn);
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(tile0);
        tileWriter.write(tile1);
      }
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(tile2);
        tileWriter.write(tile3);
      }
      try (var tileWriter = archive.newTileWriter()) {
        tileWriter.write(tile4);
      }
      archive.finish(maxMetadataIn);
    }

    try (InputStream in = Files.newInputStream(csvFilePrimary)) {
      assertEquals(
        List.of(wrapInit(minMetadataOut), toEntry(tile0), toEntry(tile1), wrapFinish(maxMetadataOut)),
        readAllEntries(in)
      );
    }
    try (InputStream in = Files.newInputStream(csvFileSecondary)) {
      assertEquals(
        List.of(toEntry(tile2), toEntry(tile3)),
        readAllEntries(in)
      );
    }
    try (InputStream in = Files.newInputStream(csvFileTertiary)) {
      assertEquals(
        List.of(toEntry(tile4)),
        readAllEntries(in)
      );
    }

    assertEquals(
      Set.of(csvFilePrimary, csvFileSecondary, csvFileTertiary),
      Files.list(tempDir).collect(Collectors.toUnmodifiableSet())
    );
  }

  private static List<StreamArchiveProto.Entry> readAllEntries(InputStream in) throws IOException {
    final List<StreamArchiveProto.Entry> result = new ArrayList<>();
    StreamArchiveProto.Entry entry;
    while ((entry = StreamArchiveProto.Entry.parseDelimitedFrom(in)) != null) {
      result.add(entry);
    }
    return result;
  }

  private static StreamArchiveProto.Entry toEntry(TileEncodingResult result) {
    return StreamArchiveProto.Entry.newBuilder()
      .setTile(
        StreamArchiveProto.TileEntry.newBuilder()
          .setZ(result.coord().z())
          .setX(result.coord().x())
          .setY(result.coord().y())
          .setEncodedData(ByteString.copyFrom(result.tileData()))
          .build()
      )
      .build();
  }

  private static StreamArchiveProto.Entry wrapInit(StreamArchiveProto.Metadata metadata) {
    return StreamArchiveProto.Entry.newBuilder()
      .setInitialization(StreamArchiveProto.InitializationEntry.newBuilder().setMetadata(metadata).build())
      .build();
  }

  private static StreamArchiveProto.Entry wrapFinish(StreamArchiveProto.Metadata metadata) {
    return StreamArchiveProto.Entry.newBuilder()
      .setFinish(StreamArchiveProto.FinishEntry.newBuilder().setMetadata(metadata).build())
      .build();
  }
}
