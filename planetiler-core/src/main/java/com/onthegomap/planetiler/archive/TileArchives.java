package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.pmtiles.ReadablePmtiles;
import com.onthegomap.planetiler.pmtiles.WriteablePmtiles;
import java.io.IOException;
import java.nio.file.Path;

public class TileArchives {
  private TileArchives() {}

  public static WriteableTileArchive newWriter(String archive, PlanetilerConfig config) throws IOException {
    return newWriter(TileArchiveConfig.from(archive), config);
  }

  public static ReadableTileArchive newReader(String archive, PlanetilerConfig config) throws IOException {
    return newReader(TileArchiveConfig.from(archive), config);
  }

  public static WriteableTileArchive newWriter(TileArchiveConfig archive, PlanetilerConfig config)
    throws IOException {
    var options = archive.applyFallbacks(config.arguments());
    return switch (archive.format()) {
      case MBTILES -> Mbtiles.newWriteToFileDatabase(archive.getLocalPath(), options);
      case PMTILES -> WriteablePmtiles.newWriteToFile(archive.getLocalPath());
    };
  }

  public static ReadableTileArchive newReader(TileArchiveConfig archive, PlanetilerConfig config)
    throws IOException {
    var options = archive.applyFallbacks(config.arguments());
    return switch (archive.format()) {
      case MBTILES -> Mbtiles.newReadOnlyDatabase(archive.getLocalPath(), options);
      case PMTILES -> ReadablePmtiles.newReadFromFile(archive.getLocalPath());
    };
  }

  public static ReadableTileArchive newReader(Path path, PlanetilerConfig config) throws IOException {
    return newReader(path.toString(), config);
  }

  public static WriteableTileArchive newWriter(Path path, PlanetilerConfig config) throws IOException {
    return newWriter(path.toString(), config);
  }

}
