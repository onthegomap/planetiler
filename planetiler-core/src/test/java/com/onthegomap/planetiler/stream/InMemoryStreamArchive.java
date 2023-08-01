package com.onthegomap.planetiler.stream;

import com.onthegomap.planetiler.archive.ReadableTileArchive;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.proto.StreamArchiveProto;
import com.onthegomap.planetiler.util.CloseableIterator;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

public class InMemoryStreamArchive implements ReadableTileArchive {

  private final List<TileEncodingResult> tileEncodings;
  private final TileArchiveMetadata metadata;

  private InMemoryStreamArchive(List<TileEncodingResult> tileEncodings, TileArchiveMetadata metadata) {
    this.tileEncodings = tileEncodings;
    this.metadata = metadata;
  }

  public static InMemoryStreamArchive fromCsv(Path p) throws IOException {
    var base64Decoder = Base64.getDecoder();
    final List<TileEncodingResult> tileEncodings = new ArrayList<>();
    try (var reader = Files.newBufferedReader(p)) {
      String line;
      while ((line = reader.readLine()) != null) {
        final String[] splits = line.split(",");
        final TileCoord tileCoord = TileCoord.ofXYZ(Integer.parseInt(splits[0]), Integer.parseInt(splits[1]),
          Integer.parseInt(splits[2]));
        tileEncodings.add(new TileEncodingResult(tileCoord, base64Decoder.decode(splits[3]), OptionalLong.empty()));
      }
    }
    return new InMemoryStreamArchive(tileEncodings, null);
  }

  public static InMemoryStreamArchive fromProtobuf(Path p) throws IOException {
    final List<TileEncodingResult> tileEncodings = new ArrayList<>();
    try (var in = Files.newInputStream(p)) {
      StreamArchiveProto.Entry entry;
      while ((entry = StreamArchiveProto.Entry.parseDelimitedFrom(in)) != null) {
        if (entry.getEntryCase() == StreamArchiveProto.Entry.EntryCase.TILE) {
          final StreamArchiveProto.TileEntry tileProto = entry.getTile();
          final TileCoord tileCoord = TileCoord.ofXYZ(tileProto.getX(), tileProto.getY(), tileProto.getZ());
          tileEncodings
            .add(new TileEncodingResult(tileCoord, tileProto.getEncodedData().toByteArray(), OptionalLong.empty()));
        }
      }
    }
    return new InMemoryStreamArchive(tileEncodings, null /* could add once the format is finalized*/);
  }

  public static InMemoryStreamArchive fromJson(Path p) throws IOException {
    final List<TileEncodingResult> tileEncodings = new ArrayList<>();
    final TileArchiveMetadata[] metadata = new TileArchiveMetadata[]{null};
    try (var reader = Files.newBufferedReader(p)) {
      WriteableJsonStreamArchive.jsonMapper
        .readerFor(WriteableJsonStreamArchive.Entry.class)
        .readValues(reader)
        .forEachRemaining(entry -> {
          if (entry instanceof WriteableJsonStreamArchive.TileEntry te) {
            final TileCoord tileCoord = TileCoord.ofXYZ(te.x(), te.y(), te.z());
            tileEncodings.add(new TileEncodingResult(tileCoord, te.encodedData(), OptionalLong.empty()));
          } else if (entry instanceof WriteableJsonStreamArchive.FinishEntry fe) {
            metadata[0] = fe.metadata();
          }
        });
    }
    return new InMemoryStreamArchive(tileEncodings, Objects.requireNonNull(metadata[0]));
  }

  @Override
  public void close() throws IOException {}

  @Override
  public byte[] getTile(int x, int y, int z) {

    final TileCoord coord = TileCoord.ofXYZ(x, y, z);

    return tileEncodings.stream()
      .filter(ter -> ter.coord().equals(coord)).findFirst()
      .map(TileEncodingResult::tileData)
      .orElse(null);
  }

  @Override
  public CloseableIterator<TileCoord> getAllTileCoords() {

    final Iterator<TileEncodingResult> it = tileEncodings.iterator();

    return new CloseableIterator<TileCoord>() {
      @Override
      public TileCoord next() {
        return it.next().coord();
      }

      @Override
      public boolean hasNext() {
        return it.hasNext();
      }

      @Override
      public void close() {}
    };
  }

  @Override
  public TileArchiveMetadata metadata() {
    return metadata;
  }

  @Override
  public boolean supportsMetadata() {
    return metadata != null;
  }

}
