package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.pmtiles.ReadablePmtiles;
import com.onthegomap.planetiler.pmtiles.WriteablePmtiles;
import com.onthegomap.planetiler.stream.StreamArchiveConfig;
import com.onthegomap.planetiler.stream.WriteableCsvArchive;
import com.onthegomap.planetiler.stream.WriteableJsonStreamArchive;
import com.onthegomap.planetiler.stream.WriteableProtoStreamArchive;
import java.io.IOException;
import java.nio.file.Path;

/** Utilities for creating {@link ReadableTileArchive} and {@link WriteableTileArchive} instances. */
public class TileArchives {
  private TileArchives() {}

  /**
   * Returns a new {@link WriteableTileArchive} from the string definition in {@code archive} that will be parsed with
   * {@link TileArchiveConfig}.
   *
   * @throws IOException if an error occurs creating the resource.
   */
  public static WriteableTileArchive newWriter(String archive, PlanetilerConfig config) throws IOException {
    return newWriter(TileArchiveConfig.from(archive), config);
  }

  /**
   * Returns a new {@link ReadableTileArchive} from the string definition in {@code archive} that will be parsed with
   * {@link TileArchiveConfig}.
   *
   * @throws IOException if an error occurs opening the resource.
   */
  public static ReadableTileArchive newReader(String archive, PlanetilerConfig config) throws IOException {
    return newReader(TileArchiveConfig.from(archive), config);
  }

  /**
   * Returns a new {@link WriteableTileArchive} from the string definition in {@code archive}.
   *
   * @throws IOException if an error occurs creating the resource.
   */
  public static WriteableTileArchive newWriter(TileArchiveConfig archive, PlanetilerConfig config)
    throws IOException {
    var options = archive.applyFallbacks(config.arguments());
    var format = archive.format();
    return switch (format) {
      case MBTILES ->
        // pass-through legacy arguments for fallback
        Mbtiles.newWriteToFileDatabase(archive.getLocalPath(), options.orElse(config.arguments()
          .subset(Mbtiles.LEGACY_VACUUM_ANALYZE, Mbtiles.LEGACY_COMPACT_DB, Mbtiles.LEGACY_SKIP_INDEX_CREATION)));
      case PMTILES -> WriteablePmtiles.newWriteToFile(archive.getLocalPath());
      case CSV, TSV -> WriteableCsvArchive.newWriteToFile(format, archive.getLocalPath(),
        new StreamArchiveConfig(config, options));
      case PROTO, PBF -> WriteableProtoStreamArchive.newWriteToFile(archive.getLocalPath(),
        new StreamArchiveConfig(config, options));
      case JSON -> WriteableJsonStreamArchive.newWriteToFile(archive.getLocalPath(),
        new StreamArchiveConfig(config, options));
    };
  }

  /**
   * Returns a new {@link ReadableTileArchive} from the string definition in {@code archive}.
   *
   * @throws IOException if an error occurs opening the resource.
   */
  public static ReadableTileArchive newReader(TileArchiveConfig archive, PlanetilerConfig config)
    throws IOException {
    var options = archive.applyFallbacks(config.arguments());
    return switch (archive.format()) {
      case MBTILES -> Mbtiles.newReadOnlyDatabase(archive.getLocalPath(), options);
      case PMTILES -> ReadablePmtiles.newReadFromFile(archive.getLocalPath());
      case CSV, TSV -> throw new UnsupportedOperationException("reading CSV is not supported");
      case PROTO, PBF -> throw new UnsupportedOperationException("reading PROTO is not supported");
      case JSON -> throw new UnsupportedOperationException("reading JSON is not supported");
    };
  }

  /** Alias for {@link #newReader(String, PlanetilerConfig)}. */
  public static ReadableTileArchive newReader(Path path, PlanetilerConfig config) throws IOException {
    return newReader(path.toString(), config);
  }

  /** Alias for {@link #newWriter(String, PlanetilerConfig)}. */
  public static WriteableTileArchive newWriter(Path path, PlanetilerConfig config) throws IOException {
    return newWriter(path.toString(), config);
  }
}
