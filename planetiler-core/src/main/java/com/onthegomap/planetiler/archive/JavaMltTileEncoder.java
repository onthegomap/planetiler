package com.onthegomap.planetiler.archive;

import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.util.TileSizeStats;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.maplibre.mlt.converter.ConversionConfig;
import org.maplibre.mlt.converter.FeatureTableOptimizations;
import org.maplibre.mlt.converter.MltConverter;
import org.maplibre.mlt.converter.mvt.ColumnMapping;
import org.maplibre.mlt.converter.mvt.ColumnMappingConfig;
import org.maplibre.mlt.converter.mvt.MapboxVectorTile;
import org.maplibre.mlt.data.Layer;

/**
 * Encodes tiles to MLT with the pure-Java encoder of {@code org.maplibre:mlt}, which works on every platform. Mixed
 * value types of one key become text.
 */
final class JavaMltTileEncoder implements TileEncoder {

  private static final Pattern MATCH_ALL = Pattern.compile(".*");
  private static final ColumnMappingConfig EMPTY_COLUMN_MAPPING = new ColumnMappingConfig();
  // testing showed a shared dictionary only pays off with >~50 repeated strings
  private static final int MIN_REPEATED_STRINGS = 50;

  private final PlanetilerConfig config;
  private final Stats stats;

  /** Creates an encoder with the {@code mlt_*} settings in {@code config} that logs conversion errors to stats. */
  JavaMltTileEncoder(PlanetilerConfig config, Stats stats) {
    this.config = config;
    this.stats = stats;
  }

  @Override
  public Encoded encode(VectorTile tile, boolean includeIds) {
    MapboxVectorTile mltInput = tile.toMltInput(stats);
    StringColumns stringColumns = config.mltSharedDictionaries() ? StringColumns.find(mltInput) : StringColumns.NONE;
    ColumnMappingConfig columnMappings = stringColumns.all().isEmpty() ? EMPTY_COLUMN_MAPPING :
      ColumnMappingConfig.of(MATCH_ALL, List.of(new ColumnMapping(stringColumns.all(), true)));
    var tilesetMetadata = MltConverter.createTilesetMetadata(mltInput, columnMappings, includeIds, true, false);
    var conversionConfig = ConversionConfig.builder()
      .includeIds(includeIds)
      .useFastPFOR(config.mltFastPfor())
      .useFSST(config.mltFsst())
      .mismatchPolicy(ConversionConfig.TypeMismatchPolicy.COERCE)
      .optimizations(mltInput.layers().stream().collect(Collectors.toMap(
        Layer::name,
        layer -> {
          Set<String> layerStringColumns = stringColumns.byLayer().get(layer.name());
          return new FeatureTableOptimizations(config.mltReorderFeature(), !includeIds,
            layerStringColumns == null || layerStringColumns.isEmpty() ? null :
              List.of(new ColumnMapping(layerStringColumns, true)));
        }
      )))
      .preTessellatePolygons(config.mltTessellatePolygons())
      .outlineFeatureTableNames(config.mltPolygonOutline() ? List.of("ALL") : null)
      .useMortonEncoding(true)
      .build();
    try {
      var mlt = MltConverter.convertMvt(mltInput, tilesetMetadata, conversionConfig, null);
      return new Encoded(mlt, TileSizeStats.computeMltTileStats(tile, mltInput, mlt));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /** String-only columns to share dictionaries between, in the whole tile and by layer. */
  private record StringColumns(Set<String> all, Map<String, Set<String>> byLayer) {

    static final StringColumns NONE = new StringColumns(Set.of(), Map.of());

    /** Returns the columns of {@code mltInput} that only hold strings, or none if too few strings repeat. */
    static StringColumns find(MapboxVectorTile mltInput) {
      Set<String> all = new HashSet<>();
      Map<String, Set<String>> byLayer = new HashMap<>();
      Map<String, Integer> valueCounts = new HashMap<>();
      Set<String> notStringColumns = new HashSet<>();
      int numRepeats = 0;
      for (var layer : mltInput.layers()) {
        for (var feature : layer.features()) {
          for (var entry : feature.properties().entrySet()) {
            if (entry.getValue() instanceof String value) {
              if (numRepeats < MIN_REPEATED_STRINGS) {
                int count = valueCounts.merge(value, 1, Integer::sum);
                if (count > 1) {
                  numRepeats += count == 2 ? 2 : 1;
                }
              }
              all.add(entry.getKey());
              byLayer.computeIfAbsent(layer.name(), name -> new HashSet<>()).add(entry.getKey());
            } else {
              notStringColumns.add(entry.getKey());
            }
          }
        }
      }
      if (numRepeats < MIN_REPEATED_STRINGS) {
        return NONE;
      }
      all.removeAll(notStringColumns);
      byLayer.values().forEach(columns -> columns.removeAll(notStringColumns));
      return new StringColumns(all, byLayer);
    }
  }
}
