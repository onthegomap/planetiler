package com.onthegomap.planetiler.files;

import static com.onthegomap.planetiler.files.TileSchemeEncoding.X_TEMPLATE;
import static com.onthegomap.planetiler.files.TileSchemeEncoding.Y_TEMPLATE;
import static com.onthegomap.planetiler.files.TileSchemeEncoding.Z_TEMPLATE;

import com.onthegomap.planetiler.config.Arguments;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

public final class FilesArchiveUtils {

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

  static TileSchemeEncoding tilesSchemeEncoding(Arguments options, Path basePath, String defaultTileScheme) {
    final String tileScheme = options.getString(
      OPTION_TILE_SCHEME,
      "the tile scheme (e.g. {z}/{x}/{y}.pbf, {x}/{y}/{z}.pbf)" +
        " - instead of {x}/{y} {xs}/{ys} can be used which splits the x/y into 2 directories each" +
        " which ensures <1000 files per directory",
      defaultTileScheme
    );
    return new TileSchemeEncoding(tileScheme, basePath);
  }

  static BasePathWithTileSchemeEncoding basePathWithTileSchemeEncoding(Arguments options, Path basePath) {
    final String basePathStr = basePath.toString();
    final int curlyIndex = basePathStr.indexOf('{');
    if (curlyIndex >= 0) {
      final Path newBasePath = Paths.get(basePathStr.substring(0, curlyIndex));
      return new BasePathWithTileSchemeEncoding(
        newBasePath,
        tilesSchemeEncoding(options, newBasePath, basePathStr.substring(curlyIndex))
      );
    } else {
      return new BasePathWithTileSchemeEncoding(
        basePath,
        tilesSchemeEncoding(options, basePath, Path.of(Z_TEMPLATE, X_TEMPLATE, Y_TEMPLATE + ".pbf").toString()));
    }
  }

  public static Path cleanBasePath(Path basePath) {
    final String basePathStr = basePath.toString();
    final int curlyIndex = basePathStr.indexOf('{');
    if (curlyIndex >= 0) {
      return Paths.get(basePathStr.substring(0, curlyIndex));
    }
    return basePath;
  }

  record BasePathWithTileSchemeEncoding(Path basePath, TileSchemeEncoding tileSchemeEncoding) {}
}
