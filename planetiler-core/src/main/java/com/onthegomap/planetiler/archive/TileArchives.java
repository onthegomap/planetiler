package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.config.ArgumentGroup;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.files.ReadableFilesArchive;
import com.onthegomap.planetiler.files.WriteableFilesArchive;
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
   * Reads the options that the writer for {@code archive} reads when it opens, without opening it, so that
   * {@code --help} lists them and unknown options can be reported before the run starts.
   */
  public static void parseWriterOptions(TileArchiveConfig archive, PlanetilerConfig config) {
    var options = writerOptions(archive, config);
    switch (archive.format()) {
      case MBTILES -> Mbtiles.Options.from(options);
      case CSV, TSV -> WriteableCsvArchive.Options.from(archive.format(), options);
      case JSON -> WriteableJsonStreamArchive.Options.from(options);
      case FILES -> WriteableFilesArchive.Options.from(archive.getLocalPath(), options);
      case PMTILES, PROTO, PBF -> {
        // no options
      }
    }
  }

  private static Arguments writerOptions(TileArchiveConfig archive, PlanetilerConfig config) {
    var arguments = config.arguments().withGroup(ArgumentGroup.OUTPUT_ADVANCED);
    var options = archive.applyFallbacks(arguments);
    // mbtiles options used to have other names, which still work
    return archive.format() == TileArchiveConfig.Format.MBTILES ? options.orElse(arguments
      .subset(Mbtiles.LEGACY_VACUUM_ANALYZE, Mbtiles.LEGACY_COMPACT_DB, Mbtiles.LEGACY_SKIP_INDEX_CREATION)) : options;
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
    var options = writerOptions(archive, config);
    var format = archive.format();
    return switch (format) {
      case MBTILES -> Mbtiles.newWriteToFileDatabase(archive.getLocalPath(), options);
      case PMTILES -> WriteablePmtiles.newWriteToFile(archive.getLocalPath());
      case CSV, TSV -> WriteableCsvArchive.newWriteToFile(format, archive.getLocalPath(),
        new StreamArchiveConfig(config, options));
      case PROTO, PBF -> WriteableProtoStreamArchive.newWriteToFile(archive.getLocalPath(),
        new StreamArchiveConfig(config, options));
      case JSON -> WriteableJsonStreamArchive.newWriteToFile(archive.getLocalPath(),
        new StreamArchiveConfig(config, options));
      case FILES -> WriteableFilesArchive.newWriter(archive.getLocalPath(), options, config.force() || config.append());
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
