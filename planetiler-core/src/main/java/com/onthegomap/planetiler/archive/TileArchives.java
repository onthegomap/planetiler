package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.CommonConfigs;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.files.ReadableFilesArchive;
import com.onthegomap.planetiler.files.WriteableFilesArchive;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.pmtiles.ReadablePmtiles;
import com.onthegomap.planetiler.pmtiles.WriteablePmtiles;
import com.onthegomap.planetiler.stream.ReadableCsvArchive;
import com.onthegomap.planetiler.stream.ReadableJsonStreamArchive;
import com.onthegomap.planetiler.stream.ReadableProtoStreamArchive;
import com.onthegomap.planetiler.stream.StreamArchiveConfig;
import com.onthegomap.planetiler.stream.WriteableCsvArchive;
import com.onthegomap.planetiler.stream.WriteableJsonStreamArchive;
import com.onthegomap.planetiler.stream.WriteableProtoStreamArchive;
import java.io.IOException;
import java.nio.file.Path;
import java.util.function.Supplier;

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

  public static WriteableTileArchive newWriter(TileArchiveConfig archive, PlanetilerConfig config)
    throws IOException {
    return newWriter(archive, config.arguments());
  }

  /**
   * Returns a new {@link WriteableTileArchive} from the string definition in {@code archive}.
   *
   * @throws IOException if an error occurs creating the resource.
   */
  public static WriteableTileArchive newWriter(TileArchiveConfig archive, Arguments baseArguments)
    throws IOException {
    var options = archive.applyFallbacks(baseArguments);
    var format = archive.format();
    return switch (format) {
      case MBTILES ->
        // pass-through legacy arguments for fallback
        Mbtiles.newWriteToFileDatabase(archive.getLocalPath(), options.orElse(baseArguments
          .subset(Mbtiles.LEGACY_VACUUM_ANALYZE, Mbtiles.LEGACY_COMPACT_DB, Mbtiles.LEGACY_SKIP_INDEX_CREATION)));
      case PMTILES -> WriteablePmtiles.newWriteToFile(archive.getLocalPath());
      case CSV, TSV -> WriteableCsvArchive.newWriteToFile(format, archive.getLocalPath(),
        new StreamArchiveConfig(baseArguments, options));
      case PROTO, PBF -> WriteableProtoStreamArchive.newWriteToFile(archive.getLocalPath(),
        new StreamArchiveConfig(baseArguments, options));
      case JSON -> WriteableJsonStreamArchive.newWriteToFile(archive.getLocalPath(),
        new StreamArchiveConfig(baseArguments, options));
      case FILES -> WriteableFilesArchive.newWriter(archive.getLocalPath(), options,
        CommonConfigs.appendToArchive(baseArguments) || CommonConfigs.force(baseArguments));
    };
  }

  public static ReadableTileArchive newReader(TileArchiveConfig archive, PlanetilerConfig config)
    throws IOException {
    return newReader(archive, config.arguments());
  }

  /**
   * Returns a new {@link ReadableTileArchive} from the string definition in {@code archive}.
   *
   * @throws IOException if an error occurs opening the resource.
   */
  public static ReadableTileArchive newReader(TileArchiveConfig archive, Arguments baseArguments)
    throws IOException {
    var options = archive.applyFallbacks(baseArguments);
    Supplier<StreamArchiveConfig> streamArchiveConfig = () -> new StreamArchiveConfig(baseArguments, options);
    return switch (archive.format()) {
      case MBTILES -> Mbtiles.newReadOnlyDatabase(archive.getLocalPath(), options);
      case PMTILES -> ReadablePmtiles.newReadFromFile(archive.getLocalPath());
      case CSV, TSV ->
        ReadableCsvArchive.newReader(archive.format(), archive.getLocalPath(), streamArchiveConfig.get());
      case PROTO, PBF -> ReadableProtoStreamArchive.newReader(archive.getLocalPath(), streamArchiveConfig.get());
      case JSON -> ReadableJsonStreamArchive.newReader(archive.getLocalPath(), streamArchiveConfig.get());
      case FILES -> ReadableFilesArchive.newReader(archive.getLocalPath(), options);
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
