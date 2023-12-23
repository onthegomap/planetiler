package com.onthegomap.planetiler.files;

import static com.onthegomap.planetiler.files.FilesArchiveUtils.PBF_FILE_ENDING;
import static com.onthegomap.planetiler.files.FilesArchiveUtils.absolutePathFromTileCoord;

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
import java.util.stream.Stream;
import org.apache.commons.lang3.math.NumberUtils;

public class ReadableFilesArchive implements ReadableTileArchive {

  private final Path basePath;
  private final Path metadataPath;

  private ReadableFilesArchive(Path basePath, Arguments options) {
    this.basePath = basePath;
    Preconditions.checkArgument(
      Files.isDirectory(basePath),
      "require \"" + basePath + "\" to be an existing directory"
    );
    this.metadataPath = FilesArchiveUtils.metadataPath(basePath, options).orElse(null);
  }

  public static ReadableFilesArchive newReader(Path basePath, Arguments options) {
    return new ReadableFilesArchive(basePath, options);
  }

  @Override
  @SuppressWarnings("java:S1168") // returning null is in sync with other implementations: mbtiles and pmtiles
  public byte[] getTile(int x, int y, int z) {
    final Path absolute = absolutePathFromTileCoord(basePath, TileCoord.ofXYZ(x, y, z));
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
      final Stream<TileCoord> it = Files.find(basePath, 3, (p, a) -> a.isRegularFile())
        .map(this::mapFileToTileCoord)
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

  private Optional<TileCoord> mapFileToTileCoord(Path path) {
    final Path relative = basePath.relativize(path);
    if (relative.getNameCount() != 3) {
      return Optional.empty();
    }
    final int z = NumberUtils.toInt(relative.getName(0).toString(), -1);
    if (z < 0) {
      return Optional.empty();
    }
    final int x = NumberUtils.toInt(relative.getName(1).toString(), -1);
    if (x < 0) {
      return Optional.empty();
    }
    final String yPbf = relative.getName(2).toString();
    int dotIdx = yPbf.indexOf('.');
    if (dotIdx < 1) {
      return Optional.empty();
    }
    final int y = NumberUtils.toInt(yPbf.substring(0, dotIdx), -1);
    if (y < 0) {
      return Optional.empty();
    }
    if (!PBF_FILE_ENDING.equals(yPbf.substring(dotIdx))) {
      return Optional.empty();
    }
    return Optional.of(TileCoord.ofXYZ(x, y, z));
  }
}
