package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.TileSizeStats;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.maplibre.mlt.ffi.MltEncoder;
import org.maplibre.mlt.ffi.MltLayerStats;
import org.maplibre.mlt.ffi.MltWireVersion;
import org.maplibre.mlt.ffi.MvtGeometryType;

/**
 * Encodes tiles to MLT with the native (Rust) encoder of {@code mlt-ffi-java}, which only the creating thread may use.
 * Mixed integer and float values of one key widen to doubles, any other mix of value types becomes text.
 */
final class NativeMltTileEncoder implements TileEncoder {

  private final MltEncoder encoder;
  private final Stats stats;

  /** Creates a native encoder with the {@code mlt_*} settings in {@code config} that logs skipped features to stats. */
  NativeMltTileEncoder(PlanetilerConfig config, Stats stats) {
    this.stats = stats;
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

  @Override
  public Encoded encode(VectorTile tile, boolean includeIds) {
    var writer = new FeatureWriter(encoder, includeIds, stats);
    tile.visitFeatures(writer);
    byte[] bytes = encoder.toByteArray();
    return new Encoded(bytes, writer.layerStats(tile, bytes));
  }

  @Override
  public void close() {
    encoder.close();
  }

  /** Adds the layers, features and attributes of a tile to a native encoder, counting the features of each layer. */
  private static final class FeatureWriter implements VectorTile.FeatureVisitor {

    private final MltEncoder encoder;
    private final boolean includeIds;
    private final Stats stats;
    private final List<LayerCounts> layers = new ArrayList<>();
    private LayerCounts layer;
    private List<String> keys;
    private List<Object> values;

    FeatureWriter(MltEncoder encoder, boolean includeIds, Stats stats) {
      this.encoder = encoder;
      this.includeIds = includeIds;
      this.stats = stats;
    }

    @Override
    public boolean accept(VectorTile.VectorGeometry geometry) {
      return VectorTile.acceptMltGeometry(geometry, stats);
    }

    @Override
    public void visitLayer(String name, List<String> keys, List<Object> values) {
      this.keys = keys;
      this.values = values;
      encoder.beginLayer(name, VectorTile.EXTENT);
      layer = new LayerCounts(name);
      layers.add(layer);
    }

    @Override
    public void visitFeature(long id, VectorTile.VectorGeometry geometry, int[] tags) {
      MvtGeometryType geometryType = switch (geometry.geomType()) {
        case POINT -> MvtGeometryType.POINT;
        case LINE -> MvtGeometryType.LINE_STRING;
        case POLYGON -> MvtGeometryType.POLYGON;
        case UNKNOWN -> throw new IllegalArgumentException("accept leaves out unknown geometries");
      };
      if (includeIds && id != VectorTile.NO_FEATURE_ID) {
        encoder.beginFeature(geometryType, geometry.commands(), id);
      } else {
        encoder.beginFeature(geometryType, geometry.commands());
      }
      layer.features++;
      layer.geometries += VectorTile.countGeometries(geometry.commands());
      for (int i = 0; i < tags.length; i += 2) {
        encoder.setProperty(keys.get(tags[i]), values.get(tags[i + 1]));
      }
    }

    /** Returns the stats of each layer written to {@code bytes}, the MLT encoding of {@code tile}, in name order. */
    List<TileSizeStats.LayerStats> layerStats(VectorTile tile, byte[] bytes) {
      Map<String, MltLayerStats> sizes = MltLayerStats.of(bytes).stream()
        .collect(Collectors.toMap(MltLayerStats::name, Function.identity()));
      return layers.stream().map(counts -> {
        var size = Objects.requireNonNull(sizes.get(counts.name), () -> "no layer " + counts.name + " in MLT output");
        return new TileSizeStats.LayerStats(counts.name, size.bytes(), counts.features, counts.geometries,
          size.propertyBytes(), tile.getNumKeys(counts.name), tile.getNumValues(counts.name));
      }).toList();
    }
  }

  private static final class LayerCounts {
    final String name;
    int features;
    int geometries;

    LayerCounts(String name) {
      this.name = name;
    }
  }
}
