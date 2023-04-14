package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.pmtiles.ReadablePmtiles;
import com.onthegomap.planetiler.pmtiles.WriteablePmtiles;
import com.onthegomap.planetiler.util.Pgtiles;
import com.onthegomap.planetiler.util.TileStatsArchive;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
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
    return switch (archive.format()) {
      case MBTILES ->
        // pass-through legacy arguments for fallback
        Mbtiles.newWriteToFileDatabase(archive.getLocalPath(), options.orElse(config.arguments()
          .subset(Mbtiles.LEGACY_VACUUM_ANALYZE, Mbtiles.LEGACY_COMPACT_DB, Mbtiles.LEGACY_SKIP_INDEX_CREATION)));
      case PMTILES -> WriteablePmtiles.newWriteToFile(archive.getLocalPath());
      case POSTGRES -> Pgtiles.writer(archive.uri(), options);
      case STATS -> new TileStatsArchive(archive.getLocalPath());
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
      case POSTGRES -> Pgtiles.reader(archive.uri(), options);
      case STATS -> throw new UnsupportedEncodingException();
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
