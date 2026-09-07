package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.pmtiles.Pmtiles;
import com.onthegomap.planetiler.pmtiles.ReadablePmtiles;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class MapterhornFix {

  public static void main(String[] args) throws IOException {

    Pmtiles.Header metadata;
    try (var in = ReadablePmtiles.newReadFromFile(Path.of("mapterhorn-png-z11-fixed.pmtiles"))) {
      metadata = in.getHeader();
    }
    var newMetadata = new Pmtiles.Header(
      metadata.specVersion(),
      metadata.rootDirOffset(),
      metadata.rootDirLength(),
      metadata.jsonMetadataOffset(),
      metadata.jsonMetadataLength(),
      metadata.leafDirectoriesOffset(),
      metadata.leafDirectoriesLength(),
      metadata.tileDataOffset(),
      metadata.tileDataLength(),
      metadata.numAddressedTiles(),
      metadata.numTileEntries(),
      metadata.numTileContents(),
      metadata.clustered(),
      metadata.internalCompression(),
      metadata.tileCompression(),
      Pmtiles.TileType.PNG,
      metadata.minZoom(),
      metadata.maxZoom(),
      metadata.minLonE7(),
      metadata.minLatE7(),
      metadata.maxLonE7(),
      metadata.maxLatE7(),
      metadata.centerZoom(),
      metadata.centerLonE7(),
      metadata.centerLatE7()
    );
    try (var fc = FileChannel.open(Path.of("mapterhorn-png-z11-fixed.pmtiles"), StandardOpenOption.WRITE)) {
      fc.position(0);
      fc.write(ByteBuffer.wrap(newMetadata.toBytes()));
    }
  }
}
