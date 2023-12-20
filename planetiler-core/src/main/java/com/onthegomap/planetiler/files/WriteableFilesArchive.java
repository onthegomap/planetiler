package com.onthegomap.planetiler.files;

import com.google.common.base.Preconditions;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.archive.WriteableTileArchive;
import com.onthegomap.planetiler.geo.TileOrder;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;

public class WriteableFilesArchive implements WriteableTileArchive {

  private final Path basePath;

  private WriteableFilesArchive(Path basePath) {
    this.basePath = basePath;
    if (!Files.exists(basePath)) {
      mkdirs(basePath);
    }
    Preconditions.checkArgument(
      Files.isDirectory(basePath),
      "require \"" + basePath + "\" to be a directory"
    );
  }

  public static WriteableFilesArchive newWriter(Path basePath) {
    return new WriteableFilesArchive(basePath);
  }

  @Override
  public boolean deduplicates() {
    return false;
  }

  @Override
  public TileOrder tileOrder() {
    return TileOrder.TMS;
  }

  @Override
  public TileWriter newTileWriter() {
    return new FilesWriter(basePath);
  }

  @Override
  public void close() throws IOException {
    // nothing to do here
  }

  private static void mkdirs(Path p) {
    try {
      Files.createDirectories(p);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  private static class FilesWriter implements TileWriter {

    private final Path basePath;
    private Path lastCheckedFolder;

    FilesWriter(Path basePath) {
      this.basePath = basePath;
      this.lastCheckedFolder = basePath;
    }

    @Override
    public void write(TileEncodingResult encodingResult) {

      final Path file = FilesArchiveUtils.absolutePathFromTileCoord(basePath, encodingResult.coord());
      final Path folder = file.getParent();

      // tiny optimization in order to avoid too many unnecessary "folder-exists-checks" (I/O)
      if (!lastCheckedFolder.equals(folder) && !Files.exists(folder)) {
        mkdirs(folder);
      }
      lastCheckedFolder = folder;
      try {
        Files.write(file, encodingResult.tileData());
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    @Override
    public void close() {
      // nothing to do here
    }
  }
}
