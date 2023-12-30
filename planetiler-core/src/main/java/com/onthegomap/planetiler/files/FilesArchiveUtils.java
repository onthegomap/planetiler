package com.onthegomap.planetiler.files;

import static com.onthegomap.planetiler.files.TileSchemeEncoding.X_TEMPLATE;
import static com.onthegomap.planetiler.files.TileSchemeEncoding.Y_TEMPLATE;
import static com.onthegomap.planetiler.files.TileSchemeEncoding.Z_TEMPLATE;

import com.onthegomap.planetiler.config.Arguments;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

final class FilesArchiveUtils {

  static final String OPTION_METADATA_PATH = "metadata_path";
  static final String OPTION_TILE_SCHEME = "tile_scheme";

  private FilesArchiveUtils() {}

  static Optional<Path> metadataPath(Path basePath, Arguments options) {
    final String metadataPathRaw = options.getString(
      OPTION_METADATA_PATH,
      "path to the metadata - use \"none\" to disable",
      "metadata.json"
    );
    if ("none".equals(metadataPathRaw)) {
      return Optional.empty();
    } else {
      final Path p = Paths.get(metadataPathRaw);
      if (p.isAbsolute()) {
        return Optional.of(p);
      }
      return Optional.of(basePath.resolve(p));
    }
  }

  static TileSchemeEncoding tilesSchemeEncoding(Arguments options, Path basePath) {
    final String tileScheme = options.getString(
      OPTION_TILE_SCHEME,
      "the tile scheme (e.g. {z}/{x}/{y}.pbf, {x}/{y}/{z}.pbf)" +
        " - instead of {x}/{y} {xs}/{ys} can be used which splits the x/y into 2 directories each" +
        " which ensures <1000 files per directory",
      Path.of(Z_TEMPLATE, X_TEMPLATE, Y_TEMPLATE + ".pbf").toString()
    );
    return new TileSchemeEncoding(tileScheme, basePath);
  }
}
