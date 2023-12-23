package com.onthegomap.planetiler.files;

import com.google.common.base.Preconditions;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileArchiveMetadataDeSer;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.archive.WriteableTileArchive;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.util.FileUtils;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.function.Function;

public class WriteableFilesArchive implements WriteableTileArchive {

  private final Path basePath;
  private final Path metadataPath;

  private final Function<TileCoord, Path> tileSchemeEncoder;

  private WriteableFilesArchive(Path basePath, Arguments options, boolean overwriteMetadata) {
    this.basePath = createValidateDirectory(basePath);
    this.metadataPath = FilesArchiveUtils.metadataPath(basePath, options)
      .flatMap(p -> FilesArchiveUtils.metadataPath(p.getParent(), options))
      .orElse(null);
    if (this.metadataPath != null && Files.exists(this.metadataPath)) {
      if (!overwriteMetadata) {
        throw new IllegalArgumentException(this.metadataPath + " already exists");
      } else if (!Files.isRegularFile(this.metadataPath)) {
        throw new IllegalArgumentException("require " + this.metadataPath + " to be a regular file");
      }
    }
    final String tileScheme = FilesArchiveUtils.tilesScheme(options);
    this.tileSchemeEncoder = FilesArchiveUtils.tileSchemeEncoder(basePath, tileScheme);
  }

  public static WriteableFilesArchive newWriter(Path basePath, Arguments options, boolean overwriteMetadata) {
    return new WriteableFilesArchive(basePath, options, overwriteMetadata);
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
    return new FilesWriter(basePath, tileSchemeEncoder);
  }

  @Override
  public void finish(TileArchiveMetadata tileArchiveMetadata) {
    if (metadataPath == null) {
      return;
    }
    try (OutputStream s = Files.newOutputStream(metadataPath)) {
      TileArchiveMetadataDeSer.mbtilesMapper().writeValue(s, tileArchiveMetadata);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public void close() throws IOException {
    // nothing to do here
  }

  private static Path createValidateDirectory(Path p) {
    if (!Files.exists(p)) {
      FileUtils.createDirectory(p);
    }
    Preconditions.checkArgument(
      Files.isDirectory(p),
      "require \"" + p + "\" to be a directory"
    );
    return p;
  }

  private static class FilesWriter implements TileWriter {

    private final Function<TileCoord, Path> tileSchemeEncoder;
    private Path lastCheckedFolder;

    FilesWriter(Path basePath, Function<TileCoord, Path> tileSchemeEncoder) {
      this.tileSchemeEncoder = tileSchemeEncoder;
      this.lastCheckedFolder = basePath;
    }

    @Override
    public void write(TileEncodingResult encodingResult) {

      final Path file = tileSchemeEncoder.apply(encodingResult.coord());
      final Path folder = file.getParent();

      // tiny optimization in order to avoid too many unnecessary "folder-exists-checks" (I/O)
      // only effective when the tileScheme is z/x/y but doesn't really harm otherwise
      if (!lastCheckedFolder.equals(folder) && !Files.exists(folder)) {
        FileUtils.createDirectory(folder);
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
