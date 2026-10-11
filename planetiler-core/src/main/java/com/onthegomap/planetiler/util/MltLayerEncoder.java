package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import org.maplibre.mlt.ffi.MltEncoder;
import org.maplibre.mlt.ffi.MltEncoderOptions;
import org.maplibre.mlt.ffi.MvtGeometryType;
import org.maplibre.mlt.ffi.NativeLibrary;

/**
 * Encodes planetiler tile features to MLT with the native {@code MltEncoder} of {@code mlt-ffi-java}.
 * <p>
 * The JVM needs {@code --enable-native-access=ALL-UNNAMED}. The native library is extracted from the
 * {@code mlt-ffi-java} jar, or loaded from the file named by {@code -Dmlt.ffi.library}.
 * <p>
 * Not thread-safe: every thread that encodes tiles creates its own instance, which owns a native encoder.
 * Only the thread that created an instance may use it and {@link #close()} it.
 * <p>
 * Property values of different kinds under one key are merged by the native encoder the way the MVT importer does:
 * integers and floats widen to a double, any other mix becomes text.
 */
public class MltLayerEncoder implements AutoCloseable {

  private static final int EXTENT = 4096;

  private final MltEncoder encoder;

  /** Loads the native library, throwing if it cannot be loaded, so callers fail before encoding any tiles. */
  public static void load() {
    NativeLibrary.load();
  }

  /** Builds v1 options from the {@code mlt_*} settings in {@code config}. */
  public MltLayerEncoder(PlanetilerConfig config) {
    // set every option explicitly since the native defaults enable FSST, FastPFOR and shared dictionaries
    var options = MltEncoderOptions.builder()
      .allowFsst(config.mltFsst())
      .allowFastPfor(config.mltFastPfor())
      .tessellate(config.mltTessellatePolygons())
      .attemptSpatialMortonSort(config.mltReorderFeature())
      .attemptSpatialHilbertSort(config.mltReorderFeature())
      .attemptIdSort(config.mltReorderFeature())
      .allowSharedDict(config.mltSharedDictionaries())
      .build();
    encoder = new MltEncoder(options);
  }

  /** Returns the MLT encoding of {@code tile} with the standard extent, one layer per non-empty layer. */
  public byte[] encode(VectorTile tile, boolean includeIds) {
    tile.forEachFeature(layer -> encoder.beginLayer(layer, EXTENT), feature -> addFeature(feature, includeIds));
    return encoder.toByteArray();
  }

  private void addFeature(VectorTile.Feature feature, boolean includeIds) {
    var geometry = feature.geometry();
    MvtGeometryType geometryType = switch (geometry.geomType()) {
      case POINT -> MvtGeometryType.POINT;
      case LINE -> MvtGeometryType.LINE_STRING;
      case POLYGON -> MvtGeometryType.POLYGON;
      case UNKNOWN -> throw new IllegalArgumentException("unknown geometry type in layer " + feature.layer());
    };
    if (includeIds && feature.id() != VectorTile.NO_FEATURE_ID) {
      encoder.beginFeature(geometryType, geometry.commands(), feature.id());
    } else {
      encoder.beginFeature(geometryType, geometry.commands());
    }
    for (var tag : feature.tags().entrySet()) {
      Object value = tag.getValue();
      String key = tag.getKey();
      if (value == null) {
        continue;
      }
      if (value instanceof Boolean b) {
        encoder.setBool(key, b);
      } else if (value instanceof Float f) {
        encoder.setFloat(key, f);
      } else if (value instanceof Double d) {
        encoder.setDouble(key, d);
      } else if (value instanceof Number n) {
        encoder.setLong(key, n.longValue());
      } else {
        encoder.setString(key, value.toString());
      }
    }
  }

  @Override
  public void close() {
    encoder.close();
  }
}
