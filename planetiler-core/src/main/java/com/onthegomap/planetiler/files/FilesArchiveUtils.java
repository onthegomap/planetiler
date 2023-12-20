package com.onthegomap.planetiler.files;

import com.onthegomap.planetiler.geo.TileCoord;
import java.nio.file.Path;
import java.nio.file.Paths;

final class FilesArchiveUtils {

  static final String PBF_FILE_ENDING = ".pbf";

  private FilesArchiveUtils() {}

  static Path relativePathFromTileCoord(TileCoord tc) {
    return Paths.get(Integer.toString(tc.z()), Integer.toString(tc.x()),
      tc.y() + PBF_FILE_ENDING);
  }

  static Path absolutePathFromTileCoord(Path basePath, TileCoord tc) {
    return basePath.resolve(relativePathFromTileCoord(tc));
  }
}
