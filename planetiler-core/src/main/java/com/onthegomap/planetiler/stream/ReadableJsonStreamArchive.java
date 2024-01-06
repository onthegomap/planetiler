package com.onthegomap.planetiler.stream;

import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.util.CloseableIterator;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;

/**
 * Reads tiles and metadata from a delimited JSON file. Counterpart to {@link WriteableJsonStreamArchive}.
 *
 * @see WriteableJsonStreamArchive
 */
public class ReadableJsonStreamArchive extends ReadableStreamArchive<JsonStreamArchiveEntry> {

  private ReadableJsonStreamArchive(Path basePath, StreamArchiveConfig config) {
    super(basePath, config);
  }

  public static ReadableJsonStreamArchive newReader(Path basePath, StreamArchiveConfig config) {
    return new ReadableJsonStreamArchive(basePath, config);
  }

  @Override
  CloseableIterator<JsonStreamArchiveEntry> createIterator() {
    BufferedReader reader = null;
    try {
      reader = Files.newBufferedReader(basePath);
      final var readerFinal = reader;
      final var it = StreamArchiveUtils.jsonMapperJsonStreamArchive
        .readerFor(JsonStreamArchiveEntry.class)
        .<JsonStreamArchiveEntry>readValues(readerFinal);
      return new CloseableIterator<>() {
        @Override
        public void close() {
          try {
            readerFinal.close();
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }

        @Override
        public boolean hasNext() {
          return it.hasNext();
        }

        @Override
        public JsonStreamArchiveEntry next() {
          return it.next();
        }
      };
    } catch (IOException e) {
      closeSilentlyOnError(reader);
      throw new UncheckedIOException(e);
    }
  }

  @Override
  Optional<Tile> mapEntryToTile(JsonStreamArchiveEntry entry) {
    if (entry instanceof JsonStreamArchiveEntry.TileEntry tileEntry) {
      return Optional.of(new Tile(
        TileCoord.ofXYZ(tileEntry.x(), tileEntry.y(), tileEntry.z()),
        tileEntry.encodedData()
      ));
    }
    return Optional.empty();
  }

  @Override
  Optional<TileArchiveMetadata> mapEntryToMetadata(JsonStreamArchiveEntry entry) {
    if (entry instanceof JsonStreamArchiveEntry.FinishEntry finishEntry) {
      return Optional.ofNullable(finishEntry.metadata());
    }
    return Optional.empty();
  }
}
