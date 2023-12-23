package com.onthegomap.planetiler.files;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;

final class FilesArchiveUtils {

  static final String PBF_FILE_ENDING = ".pbf";

  static final String OPTION_METADATA_PATH = "metadata_path";

  private FilesArchiveUtils() {}

  static Path relativePathFromTileCoord(TileCoord tc) {
    return Paths.get(Integer.toString(tc.z()), Integer.toString(tc.x()),
      tc.y() + PBF_FILE_ENDING);
  }

  static Path absolutePathFromTileCoord(Path basePath, TileCoord tc) {
    return basePath.resolve(relativePathFromTileCoord(tc));
  }

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
}
