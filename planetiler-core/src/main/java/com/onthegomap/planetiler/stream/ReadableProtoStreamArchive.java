package com.onthegomap.planetiler.stream;

import com.onthegomap.planetiler.archive.Tile;
import com.onthegomap.planetiler.archive.TileArchiveMetadata;
import com.onthegomap.planetiler.archive.TileCompression;
import com.onthegomap.planetiler.geo.TileCoord;
import com.onthegomap.planetiler.proto.StreamArchiveProto;
import com.onthegomap.planetiler.util.CloseableIterator;
import com.onthegomap.planetiler.util.LayerAttrStats;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;

/**
 * Reads tiles and metadata from a delimited protobuf file. Counterpart to {@link WriteableProtoStreamArchive}.
 *
 * @see WriteableProtoStreamArchive
 */
public class ReadableProtoStreamArchive extends ReadableStreamArchive<StreamArchiveProto.Entry> {

  private ReadableProtoStreamArchive(Path basePath, StreamArchiveConfig config) {
    super(basePath, config);
  }

  public static ReadableProtoStreamArchive newReader(Path basePath, StreamArchiveConfig config) {
    return new ReadableProtoStreamArchive(basePath, config);
  }

  @Override
  CloseableIterator<StreamArchiveProto.Entry> createIterator() {
    try {
      @SuppressWarnings("java:S2095") var in = new FileInputStream(basePath.toFile());
      return new CloseableIterator<>() {
        private StreamArchiveProto.Entry nextValue;

        @Override
        public void close() {
          closeUnchecked(in);
        }

        @Override
        public boolean hasNext() {
          if (nextValue != null) {
            return true;
          }
          try {
            nextValue = StreamArchiveProto.Entry.parseDelimitedFrom(in);
            return nextValue != null;
          } catch (IOException e) {
            throw new UncheckedIOException(e);
          }
        }

        @Override
        public StreamArchiveProto.Entry next() {
          if (!hasNext()) {
            throw new NoSuchElementException();
          }
          final StreamArchiveProto.Entry returnValue = nextValue;
          nextValue = null;
          return returnValue;
        }
      };
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  @Override
  Optional<Tile> mapEntryToTile(StreamArchiveProto.Entry entry) {
    if (entry.getEntryCase() != StreamArchiveProto.Entry.EntryCase.TILE) {
      return Optional.empty();
    }
    final StreamArchiveProto.TileEntry tileEntry = entry.getTile();
    return Optional.of(new Tile(
      TileCoord.ofXYZ(tileEntry.getX(), tileEntry.getY(), tileEntry.getZ()),
      tileEntry.getEncodedData().toByteArray()
    ));
  }

  @Override
  Optional<TileArchiveMetadata> mapEntryToMetadata(StreamArchiveProto.Entry entry) {
    if (entry.getEntryCase() != StreamArchiveProto.Entry.EntryCase.FINISH) {
      return Optional.empty();
    }
    final StreamArchiveProto.Metadata metadata = entry.getFinish().getMetadata();
    return Optional.of(new TileArchiveMetadata(
      StringUtils.trimToNull(metadata.getName()),
      StringUtils.trimToNull(metadata.getDescription()),
      StringUtils.trimToNull(metadata.getAttribution()),
      StringUtils.trimToNull(metadata.getVersion()),
      StringUtils.trimToNull(metadata.getType()),
      StringUtils.trimToNull(metadata.getFormat()),
      deserializeEnvelope(metadata.hasBounds() ? metadata.getBounds() : null),
      deserializeCoordinate(metadata.hasCenter() ? metadata.getCenter() : null),
      metadata.hasMinZoom() ? metadata.getMinZoom() : null,
      metadata.hasMaxZoom() ? metadata.getMaxZoom() : null,
      extractMetadataJson(metadata),
      metadata.getOthersMap(),
      deserializeTileCompression(metadata.getTileCompression())
    ));
  }

  private Envelope deserializeEnvelope(StreamArchiveProto.Envelope s) {
    return s == null ? null : new Envelope(s.getMinX(), s.getMaxX(), s.getMinY(), s.getMaxY());
  }

  private Coordinate deserializeCoordinate(StreamArchiveProto.Coordinate s) {
    if (s == null) {
      return null;
    }
    return s.hasZ() ? new Coordinate(s.getX(), s.getY(), s.getZ()) : new CoordinateXY(s.getX(), s.getY());
  }

  private TileCompression deserializeTileCompression(StreamArchiveProto.TileCompression s) {
    return switch (s) {
      case TILE_COMPRESSION_UNSPECIFIED, UNRECOGNIZED -> TileCompression.UNKNWON;
      case TILE_COMPRESSION_GZIP -> TileCompression.GZIP;
      case TILE_COMPRESSION_NONE -> TileCompression.NONE;
    };
  }

  private TileArchiveMetadata.TileArchiveMetadataJson extractMetadataJson(StreamArchiveProto.Metadata s) {
    final List<LayerAttrStats.VectorLayer> vl = deserializeVectorLayers(s.getVectorLayersList());
    return vl.isEmpty() ? null : new TileArchiveMetadata.TileArchiveMetadataJson(vl);
  }

  private List<LayerAttrStats.VectorLayer> deserializeVectorLayers(List<StreamArchiveProto.VectorLayer> s) {
    return s.stream()
      .map(vl -> new LayerAttrStats.VectorLayer(
        vl.getId(),
        vl.getFieldsMap().entrySet().stream()
          .collect(Collectors.toMap(Map.Entry::getKey, e -> deserializeFieldType(e.getValue()))),
        Optional.ofNullable(StringUtils.trimToNull(vl.getDescription())),
        vl.hasMinZoom() ? OptionalInt.of(vl.getMinZoom()) : OptionalInt.empty(),
        vl.hasMaxZoom() ? OptionalInt.of(vl.getMaxZoom()) : OptionalInt.empty()
      ))
      .toList();
  }

  private LayerAttrStats.FieldType deserializeFieldType(StreamArchiveProto.VectorLayer.FieldType s) {
    return switch (s) {
      case FIELD_TYPE_UNSPECIFIED, UNRECOGNIZED -> throw new IllegalArgumentException("unknown type");
      case FIELD_TYPE_NUMBER -> LayerAttrStats.FieldType.NUMBER;
      case FIELD_TYPE_BOOLEAN -> LayerAttrStats.FieldType.BOOLEAN;
      case FIELD_TYPE_STRING -> LayerAttrStats.FieldType.STRING;
    };
  }
}
