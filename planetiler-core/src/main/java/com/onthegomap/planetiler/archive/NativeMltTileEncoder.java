package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.TileSizeStats;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.maplibre.mlt.ffi.MltEncoder;
import org.maplibre.mlt.ffi.MltException;
import org.maplibre.mlt.ffi.MltLayerStats;
import org.maplibre.mlt.ffi.MltWireVersion;
import org.maplibre.mlt.ffi.MvtGeometryType;
import org.maplibre.mlt.ffi.NativeLibrary;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Encodes tiles to MLT with the native (Rust) encoder of {@code mlt-ffi-java}, which only the creating thread may use.
 * Mixed integer and float values of one key widen to doubles, any other mix of value types becomes text.
 */
final class NativeMltTileEncoder implements TileEncoder {

  private static final Logger LOGGER = LoggerFactory.getLogger(NativeMltTileEncoder.class);
  private static boolean checked;

  private final MltEncoder encoder;
  private final Stats stats;
  /** Whether to fill in the layer stats that only {@code output_layerstats} uses, which costs a parse of each tile. */
  private final boolean detailedStats;

  NativeMltTileEncoder(PlanetilerConfig config, Stats stats) {
    this.stats = stats;
    this.detailedStats = config.outputLayerStats();
    // native defaults enable FSST, FastPFOR and shared dicts; v1 has no other options and always keeps polygon outlines
    encoder = MltEncoder.builder()
      .wireVersion(MltWireVersion.V1)
      .allowFsst(config.mltFsst())
      .allowFastPfor(config.mltFastPfor())
      .tessellate(config.mltTessellatePolygons())
      .attemptSpatialMortonSort(config.mltReorderFeature())
      .attemptSpatialHilbertSort(config.mltReorderFeature())
      .attemptIdSort(config.mltReorderFeature())
      .allowSharedDict(config.mltSharedDictionaries())
      .build();
  }

  /**
   * Encodes a tile once per JVM, to find out at startup rather than hours into a run that the native encoder does not
   * work on this platform.
   *
   * @throws IllegalStateException if the native encoder does not work, saying what to do instead
   */
  static synchronized void check() {
    if (!checked) {
      check(NativeMltTileEncoder::encodeOneTile);
      checked = true;
    }
  }

  /** Runs {@code encodeOneTile}, turning its failure into an {@link IllegalStateException} that says what to do. */
  static void check(Runnable encodeOneTile) {
    try {
      encodeOneTile.run();
    } catch (IllegalCallerException e) {
      throw new IllegalStateException(
        "Native MLT encoder needs the JVM option --enable-native-access=ALL-UNNAMED, or use --mlt-encoder=java", e);
    } catch (UnsatisfiedLinkError | MltException e) {
      throw new IllegalStateException("Native MLT encoder is not available on " + System.getProperty("os.name") + " " +
        System.getProperty("os.arch") + " (" + e.getMessage() + "), use --mlt-encoder=java instead", e);
    }
  }

  private static void encodeOneTile() {
    // load explicitly, since a failure inside MltEncoder's static initializer surfaces as an ExceptionInInitializerError
    NativeLibrary.load();
    try (var encoder = MltEncoder.builder().build()) {
      encoder.beginLayer("check", VectorTile.EXTENT);
      encoder.beginFeature(MvtGeometryType.POINT, VectorTile.encodeGeometry(GeoUtils.point(1, 2)).commands());
      encoder.setProperty("key", "value");
      encoder.toByteArray();
    }
  }

  @Override
  public Encoded encode(VectorTile tile, boolean includeIds) {
    var writer = new FeatureWriter(includeIds);
    tile.visitFeatures(writer);
    byte[] bytes = encoder.toByteArray();
    return new Encoded(bytes, writer.layerStats(bytes));
  }

  @Override
  public void close() {
    encoder.close();
  }

  /** Writes a tile to the native encoder, counting the features of each layer. */
  private final class FeatureWriter implements VectorTile.FeatureVisitor {

    private final boolean includeIds;
    private final List<LayerCounts> layers = new ArrayList<>();
    private LayerCounts layer;
    private List<String> keys;
    private List<Object> values;

    FeatureWriter(boolean includeIds) {
      this.includeIds = includeIds;
    }

    @Override
    public void visitLayer(String name, List<String> keys, List<Object> values) {
      this.keys = keys;
      this.values = values;
      encoder.beginLayer(name, VectorTile.EXTENT);
      layer = new LayerCounts(name, keys.size(), values.size());
      layers.add(layer);
    }

    @Override
    public void visitFeature(long id, VectorTile.VectorGeometry geometry, int[] tags) {
      MvtGeometryType geometryType = switch (geometry.geomType()) {
        case POINT -> MvtGeometryType.POINT;
        case LINE -> MvtGeometryType.LINE_STRING;
        case POLYGON -> MvtGeometryType.POLYGON;
        case UNKNOWN -> throw new IllegalArgumentException("MLT cannot store unknown geometries");
      };
      try {
        if (includeIds && id != VectorTile.NO_FEATURE_ID) {
          encoder.beginFeature(geometryType, geometry.commands(), id);
        } else {
          encoder.beginFeature(geometryType, geometry.commands());
        }
      } catch (MltException e) {
        // the encoder did not begin this feature, so its properties would land on the previous one
        stats.dataError("mlt_feature_native_encode");
        LOGGER.warn("Error converting feature {} of layer {} to MLT: {}", id, layer.name, e.getMessage());
        return;
      }
      layer.features++;
      if (detailedStats) {
        layer.geometries += VectorTile.countGeometries(geometry.commands());
      }
      for (int i = 0; i < tags.length; i += 2) {
        encoder.setProperty(keys.get(tags[i]), values.get(tags[i + 1]));
      }
    }

    /** Returns the stats of each layer of {@code bytes}, the tile the encoder just wrote. */
    List<TileSizeStats.LayerStats> layerStats(byte[] bytes) {
      // like the encoder's layer sizes, leave out layers without features as the tile does not hold them
      var written = layers.stream().filter(counts -> counts.features > 0).toList();
      var sizes = encoder.lastLayerSizes();
      var details = detailedStats ? MltLayerStats.of(bytes) : null;
      return IntStream.range(0, written.size()).mapToObj(i -> {
        var counts = written.get(i);
        return new TileSizeStats.LayerStats(counts.name, sizes.get(i).bytes(), counts.features, counts.geometries,
          details == null ? 0 : details.get(i).propertyBytes(), counts.keys, counts.values);
      }).toList();
    }
  }

  private static final class LayerCounts {
    final String name;
    final int keys;
    final int values;
    int features;
    int geometries;

    LayerCounts(String name, int keys, int values) {
      this.name = name;
      this.keys = keys;
      this.values = values;
    }
  }
}
