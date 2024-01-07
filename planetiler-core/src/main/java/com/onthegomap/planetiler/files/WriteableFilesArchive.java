package com.onthegomap.planetiler.files;

import com.google.common.base.Preconditions;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileArchiveMetadataDeSer;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.archive.TileSchemeEncoding;
import com.onthegomap.planetiler.archive.WriteableTileArchive;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.geo.TileOrder;
import com.onthegomap.planetiler.stats.Counter;
import com.onthegomap.planetiler.util.CountingOutputStream;
import com.onthegomap.planetiler.util.FileUtils;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.function.Function;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Writes tiles as separate files. The default tile scheme is z/x/y.pbf.
 * <p/>
 * Supported arguments
 * <dl>
 * <dt>(files_)tile_scheme</dt>
 * <dd>The tile scheme e.g. {x}/{y}/{z}.pbf. The default is {z}/{x}/{y}.pbf. See {@link TileSchemeEncoding} for more
 * details.</dd>
 * <dt>(files_)metadata_path</dt>
 * <dd>The path the meta data should be written to. The default is BASEPATH/metadata.json. "none" can be used to
 * suppress writing metadata.</dd>
 * </ul>
 *
 * Usages:
 *
 * <pre>
 * --output=/path/to/tiles/ --files_tile_scheme={z}/{x}/{y}.pbf --files_metadata_path=/some/other/path/metadata.json
 * --output=/path/to/tiles/{z}/{x}/{y}.pbf
 * --output=/path/to/tiles?format=files&amp;tile_scheme={z}/{x}/{y}.pbf
 * </pre>
 *
 * @see ReadableFilesArchive
 * @see TileSchemeEncoding
 */
public class WriteableFilesArchive implements WriteableTileArchive {

  private static final Logger LOGGER = LoggerFactory.getLogger(WriteableFilesArchive.class);

  private final Counter.MultiThreadCounter bytesWritten = Counter.newMultiThreadCounter();

  private final Path basePath;
  private final Path metadataPath;

  private final Function<TileCoord, Path> tileSchemeEncoder;

  private final TileOrder tileOrder;

  private WriteableFilesArchive(Path basePath, Arguments options, boolean overwriteMetadata) {

    final var pathAndScheme = FilesArchiveUtils.basePathWithTileSchemeEncoding(options, basePath);
    basePath = pathAndScheme.basePath();

    LOGGER.atInfo().log("using {} as base files archive path", basePath);

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
    final TileSchemeEncoding tileSchemeEncoding = pathAndScheme.tileSchemeEncoding();
    this.tileSchemeEncoder = tileSchemeEncoding.encoder().andThen(Paths::get);
    this.tileOrder = tileSchemeEncoding.preferredTileOrder();
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
    return tileOrder;
  }

  @Override
  public TileWriter newTileWriter() {
    return new TileFilesWriter(basePath, tileSchemeEncoder, bytesWritten.counterForThread());
  }

  @Override
  public void finish(TileArchiveMetadata tileArchiveMetadata) {
    if (metadataPath == null) {
      return;
    }
    try (OutputStream s = new CountingOutputStream(Files.newOutputStream(metadataPath), bytesWritten::incBy)) {
      TileArchiveMetadataDeSer.mbtilesMapper().writeValue(s, tileArchiveMetadata);
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  public long bytesWritten() {
    return bytesWritten.get();
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

  private static class TileFilesWriter implements TileWriter {

    private final Function<TileCoord, Path> tileSchemeEncoder;
    private final Counter bytesWritten;
    private Path lastCheckedFolder;

    TileFilesWriter(Path basePath, Function<TileCoord, Path> tileSchemeEncoder, Counter bytesWritten) {
      this.tileSchemeEncoder = tileSchemeEncoder;
      this.lastCheckedFolder = basePath;
      this.bytesWritten = bytesWritten;
    }

    @Override
    public final void write(TileEncodingResult encodingResult) {

      final byte[] data = encodingResult.tileData();

      final Path file = tileSchemeEncoder.apply(encodingResult.coord());
      final Path folder = file.getParent();

      // tiny optimization in order to avoid too many unnecessary "folder-exists-checks" (I/O)
      // only effective when the tileScheme is z/x/y but doesn't really harm otherwise
      if (!lastCheckedFolder.equals(folder) && !Files.exists(folder)) {
        FileUtils.createDirectory(folder);
      }
      lastCheckedFolder = folder;
      try {
        Files.write(file, data);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }

      bytesWritten.incBy(data.length);
    }

    @Override
    public void close() {
      // nothing to do here
    }
  }
}
