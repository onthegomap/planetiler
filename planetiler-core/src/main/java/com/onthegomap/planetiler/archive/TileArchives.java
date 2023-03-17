package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.mbtiles.Mbtiles;
import com.onthegomap.planetiler.pmtiles.ReadablePmtiles;
import com.onthegomap.planetiler.pmtiles.WriteablePmtiles;
import com.onthegomap.planetiler.util.FileUtils;
import com.onthegomap.planetiler.util.Parse;
import java.io.IOException;
import java.nio.file.Path;

public class TileArchives {
  private TileArchives() {}

  //  public static WriteableTileArchive newWriter(TileArchiveConfig config) {
  //    switch (config.format()) {
  //      case MBTILES -> Mbtiles.newWriteToFileDatabase(config.getLocalPath(), )
  //    }
  //  }

  public static WriteableTileArchive newWriter(String archive, PlanetilerConfig config) throws IOException {
    return newWriter(TileArchiveConfig.from(archive), config);
  }

  public static ReadableTileArchive newReader(String archive) throws IOException {
    return newReader(TileArchiveConfig.from(archive));
  }

  public static WriteableTileArchive newWriter(TileArchiveConfig archive, PlanetilerConfig config)
    throws IOException {
    return switch (archive.format()) {
      case MBTILES -> {
        var compact = archive.options().get("compact");
        yield Mbtiles.newWriteToFileDatabase(archive.getLocalPath(),
          compact != null ? Parse.bool(compact) : config.compactDb());
      }
      case PMTILES -> WriteablePmtiles.newWriteToFile(archive.getLocalPath());
    };
  }

  public static ReadableTileArchive newReader(TileArchiveConfig archive)
    throws IOException {
    return switch (archive.format()) {
      case MBTILES -> Mbtiles.newWriteToFileDatabase(archive.getLocalPath(),
        Parse.bool(archive.options().get("compact")));
      case PMTILES -> ReadablePmtiles.newReadFromFile(archive.getLocalPath());
    };
  }

  public static WriteableTileArchive newWriter(Path path, PlanetilerConfig config) throws IOException {
    return isPmtiles(path) ? WriteablePmtiles.newWriteToFile(path) :
      Mbtiles.newWriteToFileDatabase(path, config.compactDb());
  }

  private static boolean isPmtiles(Path path) {
    return FileUtils.hasExtension(path, "pmtiles");
  }

  public static ReadableTileArchive newReader(Path path, PlanetilerConfig config) throws IOException {
    return isPmtiles(path) ? ReadablePmtiles.newReadFromFile(path) : Mbtiles.newReadOnlyDatabase(path);
  }

}
