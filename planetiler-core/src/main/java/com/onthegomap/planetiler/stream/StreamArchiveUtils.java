package com.onthegomap.planetiler.stream;

import java.nio.file.Path;
import java.nio.file.Paths;

public final class StreamArchiveUtils {

  private StreamArchiveUtils() {}

  public static Path constructIndexedPath(Path basePath, int index) {
    return index == 0 ? basePath : Paths.get(basePath.toString() + index);
  }
}
