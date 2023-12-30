package com.onthegomap.planetiler.files;

import com.google.common.base.Preconditions;
import com.onthegomap.planetiler.archive.ReadableTileArchive;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileArchiveMetadataDeSer;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.util.CloseableIterator;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Optional;
import java.util.function.Function;
import java.util.stream.Stream;

public class ReadableFilesArchive implements ReadableTileArchive {

  private final Path basePath;
  private final Path metadataPath;
  private final Function<TileCoord, Path> tileSchemeEncoder;
  private final Function<Path, Optional<TileCoord>> tileSchemeDecoder;

  private final int searchDepth;

  private ReadableFilesArchive(Path basePath, Arguments options) {
    this.basePath = basePath;
    Preconditions.checkArgument(
      Files.isDirectory(basePath),
      "require \"" + basePath + "\" to be an existing directory"
    );
    this.metadataPath = FilesArchiveUtils.metadataPath(basePath, options).orElse(null);
    final String tileScheme = FilesArchiveUtils.tilesScheme(options);
    this.tileSchemeEncoder = FilesArchiveUtils.tileSchemeEncoder(basePath, tileScheme);
    this.tileSchemeDecoder = FilesArchiveUtils.tileSchemeDecoder(basePath, tileScheme);
    this.searchDepth = FilesArchiveUtils.searchDepth(tileScheme);
  }

  public static ReadableFilesArchive newReader(Path basePath, Arguments options) {
    return new ReadableFilesArchive(basePath, options);
  }

  @Override
  @SuppressWarnings("java:S1168") // returning null is in sync with other implementations: mbtiles and pmtiles
  public byte[] getTile(int x, int y, int z) {
    final Path absolute = tileSchemeEncoder.apply(TileCoord.ofXYZ(x, y, z));
    if (!Files.exists(absolute)) {
      return null;
    }
    try {
      return Files.readAllBytes(absolute);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public CloseableIterator<TileCoord> getAllTileCoords() {

    try {
      final Stream<TileCoord> it = Files.find(basePath, searchDepth, (p, a) -> a.isRegularFile())
        .map(tileSchemeDecoder)
        .flatMap(Optional::stream);
      return CloseableIterator.of(it);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public TileArchiveMetadata metadata() {
    if (metadataPath != null && Files.exists(metadataPath)) {
      try (InputStream is = Files.newInputStream(metadataPath)) {
        return TileArchiveMetadataDeSer.mbtilesMapper().readValue(is, TileArchiveMetadata.class);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }
    return null;
  }

  @Override
  public void close() {
    // nothing to do here
  }
}
