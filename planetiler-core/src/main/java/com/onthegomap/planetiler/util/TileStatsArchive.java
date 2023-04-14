package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileEncodingResult;
import com.onthegomap.planetiler.archive.WriteableTileArchive;
import com.onthegomap.planetiler.geo.TileOrder;
import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TileStatsArchive implements WriteableTileArchive {
  private static final Logger LOGGER = LoggerFactory.getLogger(TileStatsArchive.class);
  private final BufferedWriter writer;

  public TileStatsArchive(Path output) throws IOException {
    this.writer = Files.newBufferedWriter(output, StandardOpenOption.WRITE, StandardOpenOption.CREATE);
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
    return new BulkLoader();
  }

  @Override
  public void close() throws IOException {
    writer.close();
  }

  @Override
  public void initialize(TileArchiveMetadata metadata) {
    try {
      writer.write("z\tx\ty\tinbytes\tinfeatures\tcached\toutbytes\ttime\n");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private class BulkLoader implements TileWriter {

    @Override
    public void write(TileEncodingResult encodingResult) {
      try {
        writer.write("%d\t%d\t%d\t%d\t%d\t%d\t%d\t%d%n".formatted(
          encodingResult.coord().z(),
          encodingResult.coord().x(),
          encodingResult.coord().y(),
          encodingResult.inputBytes(),
          encodingResult.inputFeatures(),
          encodingResult.cached() ? 1 : 0,
          encodingResult.tileData().length,
          encodingResult.time()
        ));
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @Override
    public void close() {}
  }
}
