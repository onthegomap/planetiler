package com.onthegomap.planetiler.copy;

import com.onthegomap.planetiler.archive.ReadableTileArchive;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileCompression;
import com.onthegomap.planetiler.archive.WriteableTileArchive;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import org.locationtech.jts.geom.Envelope;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

record TileCopyContext(
  TileArchiveMetadata inMetadata,
  TileArchiveMetadata outMetadata,
  TileCopyConfig config,
  ReadableTileArchive reader,
  WriteableTileArchive writer
) {

  private static final Logger LOGGER = LoggerFactory.getLogger(TileCopyContext.class);

  TileCompression inputCompression() {
    return inMetadata.tileCompression();
  }

  TileCompression outputCompression() {
    return outMetadata.tileCompression();
  }

  static TileCopyContext create(ReadableTileArchive reader, WriteableTileArchive writer, TileCopyConfig config) {
    final TileArchiveMetadata inMetadata = getInMetadata(reader, config);
    final TileArchiveMetadata outMetadata = getOutMetadata(inMetadata, config);
    return new TileCopyContext(
      inMetadata,
      outMetadata,
      config,
      reader,
      writer
    );
  }

  private static TileArchiveMetadata getInMetadata(ReadableTileArchive inArchive, TileCopyConfig config) {
    TileArchiveMetadata inMetadata = config.inMetadata();
    if (inMetadata == null) {
      inMetadata = inArchive.metadata();
      if (inMetadata == null) {
        LOGGER.atWarn()
          .log("the input archive does not contain any metadata using fallback - consider passing one via in_metadata");
        inMetadata = fallbackMetadata();
      }
    }
    if (inMetadata.tileCompression() == null) {
      inMetadata = inMetadata.withTileCompression(config.inCompression());
    }

    return inMetadata;
  }

  private static TileArchiveMetadata getOutMetadata(TileArchiveMetadata inMetadata, TileCopyConfig config) {

    final TileCompression tileCompression;
    if (config.outCompression() == TileCompression.UNKNOWN && inMetadata.tileCompression() == TileCompression.UNKNOWN) {
      tileCompression = TileCompression.GZIP;
    } else if (config.outCompression() != TileCompression.UNKNOWN) {
      tileCompression = config.outCompression();
    } else {
      tileCompression = inMetadata.tileCompression();
    }

    final Envelope bounds;
    if (config.filterBounds() != null) {
      bounds = config.filterBounds();
    } else if (inMetadata.bounds() != null) {
      bounds = inMetadata.bounds();
    } else {
      bounds = null;
    }

    final int minzoom = Stream.of(inMetadata.minzoom(), config.filterMinzoom()).filter(Objects::nonNull)
      .mapToInt(Integer::intValue).max().orElse(0);

    final int maxzoom = Stream.of(inMetadata.maxzoom(), config.filterMaxzoom()).filter(Objects::nonNull)
      .mapToInt(Integer::intValue).min().orElse(0);

    return new TileArchiveMetadata(
      inMetadata.name(),
      inMetadata.description(),
      inMetadata.attribution(),
      inMetadata.version(),
      inMetadata.type(),
      inMetadata.format(),
      bounds,
      inMetadata.center(),
      minzoom,
      maxzoom,
      inMetadata.json(),
      inMetadata.others(),
      tileCompression
    );
  }

  private static TileArchiveMetadata fallbackMetadata() {
    return new TileArchiveMetadata(
      "unknown",
      null,
      null,
      null,
      null,
      TileArchiveMetadata.MVT_FORMAT, // have to guess here that it's pbf
      null,
      null,
      0,
      14,
      new TileArchiveMetadata.TileArchiveMetadataJson(List.of()), // cannot provide any vector layers
      Map.of(),
      null
    );
  }

}
