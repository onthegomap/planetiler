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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Reads tiles from a folder structure (e.g. BASEPATH/{z}/{x}{y}.pbf). Counterpart to {@link WriteableFilesArchive}.
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
 * @see WriteableFilesArchive
 * @see TileSchemeEncoding
 */
public class ReadableFilesArchive implements ReadableTileArchive {

  private static final Logger LOGGER = LoggerFactory.getLogger(ReadableFilesArchive.class);

  private final Path basePath;
  private final Path metadataPath;
  private final Function<TileCoord, Path> tileSchemeEncoder;
  private final Function<Path, Optional<TileCoord>> tileSchemeDecoder;

  private final int searchDepth;

  private ReadableFilesArchive(Path basePath, Arguments options) {

    final var pathAndScheme = FilesArchiveUtils.basePathWithTileSchemeEncoding(options, basePath);
    basePath = pathAndScheme.basePath();

    LOGGER.atInfo().log(() -> "using " + pathAndScheme.basePath() + " as base files archive path");

    this.basePath = basePath;
    Preconditions.checkArgument(
      Files.isDirectory(basePath),
      "require \"" + basePath + "\" to be an existing directory"
    );
    this.metadataPath = FilesArchiveUtils.metadataPath(basePath, options).orElse(null);
    final TileSchemeEncoding tileSchemeEncoding = pathAndScheme.tileSchemeEncoding();
    this.tileSchemeEncoder = tileSchemeEncoding.encoder();
    this.tileSchemeDecoder = tileSchemeEncoding.decoder();
    this.searchDepth = tileSchemeEncoding.searchDepth();
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
