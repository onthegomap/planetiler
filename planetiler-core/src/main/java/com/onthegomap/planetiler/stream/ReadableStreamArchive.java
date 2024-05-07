package com.onthegomap.planetiler.stream;

import com.google.common.base.Suppliers;
import com.onthegomap.planetiler.archive.ReadableTileArchive;
import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.util.CloseableIterator;
import java.io.Closeable;
import java.io.IOException;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Supplier;

abstract class ReadableStreamArchive<E> implements ReadableTileArchive {

  private final Supplier<TileArchiveMetadata> cachedMetadata = Suppliers.memoize(this::loadMetadata);

  final Path basePath;
  final StreamArchiveConfig config;

  ReadableStreamArchive(Path basePath, StreamArchiveConfig config) {
    this.basePath = basePath;
    this.config = config;
  }

  @Override
  public final byte[] getTile(TileCoord coord) {
    try (var tiles = getAllTiles(); var s = tiles.stream()) {
      return s.filter(c -> c.coord().equals(coord)).map(Tile::bytes).findFirst().orElse(null);
    }
  }

  @Override
  public final byte[] getTile(int x, int y, int z) {
    return getTile(TileCoord.ofXYZ(x, y, z));
  }

  /**
   * Callers MUST make sure to close the iterator/derived stream!
   */
  @Override
  public final CloseableIterator<TileCoord> getAllTileCoords() {
    return getAllTiles().map(Tile::coord);
  }

  /**
   * Callers MUST make sure to close the iterator/derived stream!
   */
  @Override
  public final CloseableIterator<Tile> getAllTiles() {
    return createIterator()
      .map(this::mapEntryToTile)
      .filter(Optional::isPresent)
      .map(Optional::get);
  }

  @Override
  public final TileArchiveMetadata metadata() {
    return cachedMetadata.get();
  }

  private TileArchiveMetadata loadMetadata() {
    try (var i = createIterator(); var s = i.stream()) {
      return s.map(this::mapEntryToMetadata).flatMap(Optional::stream).findFirst().orElse(null);
    }
  }

  @Override
  public void close() throws IOException {
    // nothing to close
  }

  abstract CloseableIterator<E> createIterator();

  abstract Optional<Tile> mapEntryToTile(E entry);

  abstract Optional<TileArchiveMetadata> mapEntryToMetadata(E entry);

  void closeSilentlyOnError(Closeable c) {
    if (c == null) {
      return;
    }
    try {
      c.close();
    } catch (Exception ignored) {
      // ignore
    }
  }

  @SuppressWarnings("java:S112")
  void closeUnchecked(Closeable c) {
    if (c == null) {
      return;
    }
    try {
      c.close();
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }
}
