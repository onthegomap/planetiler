package com.onthegomap.flatmap.openmaptiles.layers;

import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

import com.carrotsearch.hppc.LongObjectMap;
import com.graphhopper.coll.GHLongObjectHashMap;
import com.graphhopper.reader.ReaderElementUtils;
import com.graphhopper.reader.ReaderRelation;
import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.FeatureMerge;
import com.onthegomap.flatmap.MemoryEstimator;
import com.onthegomap.flatmap.Parse;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.VectorTileEncoder;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.read.OpenStreetMapReader;
import com.onthegomap.flatmap.read.ReaderFeature;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.geom.prep.PreparedGeometry;
import org.locationtech.jts.geom.prep.PreparedGeometryFactory;
import org.locationtech.jts.operation.linemerge.LineMerger;
import org.locationtech.jts.operation.polygonize.Polygonizer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Boundary implements
  OpenMapTilesSchema.Boundary,
  OpenMapTilesProfile.NaturalEarthProcessor,
  OpenMapTilesProfile.OsmRelationPreprocessor,
  OpenMapTilesProfile.OsmAllProcessor,
  OpenMapTilesProfile.FeaturePostProcessor,
  OpenMapTilesProfile.FinishHandler {

  private static final Logger LOGGER = LoggerFactory.getLogger(Boundary.class);
  private static final double COUNTRY_TEST_OFFSET = GeoUtils.metersToPixelAtEquator(0, 100) / 256d;
  private final Map<Long, String> regionNames = new HashMap<>();
  private final Map<Long, List<Geometry>> regionGeometries = new HashMap<>();
  private final Map<CountryBoundaryComponent, List<Geometry>> boundariesToMerge = new HashMap<>();
  private final Stats stats;

  public Boundary(Translations translations, Arguments args, Stats stats) {
    this.stats = stats;
  }

  private static boolean isDisputed(Map<String, Object> tags) {
    return Parse.bool(tags.get("disputed")) ||
      Parse.bool(tags.get("dispute")) ||
      "dispute".equals(tags.get("border_status")) ||
      tags.containsKey("disputed_by") ||
      tags.containsKey("claimed_by");
  }

  private static String editName(String name) {
    return name == null ? null : name.replace(" at ", "")
      .replaceAll("\\s+", "")
      .replace("Extentof", "");
  }

  @Override
  public void release() {
    regionGeometries.clear();
    boundariesToMerge.clear();
    regionNames.clear();
  }

  @Override
  public void processNaturalEarth(String table, SourceFeature feature, FeatureCollector features) {
    boolean disputed = feature.getString("featurecla", "").startsWith("Disputed");
    record BoundaryInfo(int adminLevel, int minzoom, int maxzoom) {}
    BoundaryInfo info = switch (table) {
      case "ne_110m_admin_0_boundary_lines_land" -> new BoundaryInfo(2, 0, 0);
      case "ne_50m_admin_0_boundary_lines_land" -> new BoundaryInfo(2, 1, 3);
      case "ne_10m_admin_0_boundary_lines_land" -> feature.hasTag("featurecla", "Lease Limit") ? null
        : new BoundaryInfo(2, 4, 4);
      case "ne_10m_admin_1_states_provinces_lines" -> {
        Double minZoom = Parse.parseDoubleOrNull(feature.getTag("min_zoom"));
        yield minZoom != null && minZoom <= 7 ? new BoundaryInfo(4, 1, 4) : null;
      }
      default -> null;
    };
    if (info != null) {
      features.line(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
        .setZoomRange(info.minzoom, info.maxzoom)
        .setMinPixelSizeAtAllZooms(0)
        .setAttr(Fields.ADMIN_LEVEL, info.adminLevel)
        .setAttr(Fields.MARITIME, 0)
        .setAttr(Fields.DISPUTED, disputed ? 1 : 0);
    }
  }

  @Override
  public List<OpenStreetMapReader.RelationInfo> preprocessOsmRelation(ReaderRelation relation) {
    String typeTag = relation.getTag("type");
    if ("boundary".equals(typeTag) && relation.hasTag("admin_level") && relation.hasTag("boundary", "administrative")) {
      Integer adminLevelValue = Parse.parseIntSubstring(relation.getTag("admin_level"));
      String code = relation.getTag("ISO3166-1:alpha3");
      if (adminLevelValue != null && adminLevelValue >= 2 && adminLevelValue <= 10) {
        boolean disputed = isDisputed(ReaderElementUtils.getProperties(relation));
        if (code != null) {
          synchronized (regionNames) {
            regionNames.put(relation.getId(), code);
          }
        }
        return List.of(new BoundaryRelation(
          relation.getId(),
          adminLevelValue,
          disputed,
          relation.getTag("name"),
          disputed ? relation.getTag("claimed_by") : null,
          code
        ));
      }
    }
    return null;
  }

  @Override
  public void processAllOsm(SourceFeature feature, FeatureCollector features) {
    if (!feature.canBeLine()) {
      return;
    }
    var relationInfos = feature.relationInfo(BoundaryRelation.class);
    if (!relationInfos.isEmpty()) {
      int minAdminLevel = Integer.MAX_VALUE;
      String disputedName = null, claimedBy = null;
      Set<Long> regionIds = new HashSet<>();
      boolean disputed = false;
      for (var info : relationInfos) {
        BoundaryRelation rel = info.relation();
        disputed |= rel.disputed;
        if (rel.adminLevel < minAdminLevel) {
          minAdminLevel = rel.adminLevel;
        }
        if (rel.disputed) {
          disputedName = disputedName == null ? rel.name : disputedName;
          claimedBy = claimedBy == null ? rel.claimedBy : claimedBy;
        }
        if (minAdminLevel == 2 && regionNames.containsKey(info.relation().id)) {
          regionIds.add(info.relation().id);
        }
      }

      if (minAdminLevel <= 10) {
        boolean wayIsDisputed = isDisputed(feature.properties());
        disputed |= wayIsDisputed;
        if (wayIsDisputed) {
          disputedName = disputedName == null ? feature.getString("name") : disputedName;
          claimedBy = claimedBy == null ? feature.getString("claimed_by") : claimedBy;
        }
        boolean maritime = feature.getBoolean("maritime") ||
          feature.hasTag("natural", "coastline") ||
          feature.hasTag("boundary_type", "maritime");
        int minzoom =
          (maritime && minAdminLevel == 2) ? 4 :
            minAdminLevel <= 4 ? 5 :
              minAdminLevel <= 6 ? 9 :
                minAdminLevel <= 8 ? 11 : 12;
        if (!regionIds.isEmpty()) {
          // save for later
          try {
            CountryBoundaryComponent component = new CountryBoundaryComponent(
              minAdminLevel,
              disputed,
              maritime,
              minzoom,
              feature.line(),
              regionIds,
              claimedBy,
              disputedName
            );
            synchronized (regionGeometries) {
              boundariesToMerge.computeIfAbsent(component.groupingKey(), key -> new ArrayList<>()).add(component.line);
              for (var info : relationInfos) {
                var rel = info.relation();
                if (rel.adminLevel <= 2) {
                  regionGeometries.computeIfAbsent(rel.id, id -> new ArrayList<>()).add(component.line);
                }
              }
            }
          } catch (GeometryException e) {
            LOGGER.warn("Cannot extract boundary line from " + feature);
          }
        } else {
          features.line(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
            .setAttr(Fields.ADMIN_LEVEL, minAdminLevel)
            .setAttr(Fields.DISPUTED, disputed ? 1 : 0)
            .setAttr(Fields.MARITIME, maritime ? 1 : 0)
            .setMinPixelSizeAtAllZooms(0)
            .setZoomRange(minzoom, 14)
            .setAttr(Fields.CLAIMED_BY, claimedBy)
            .setAttr(Fields.DISPUTED_NAME, editName(disputedName));
        }
      }
    }
  }

  @Override
  public void finish(String sourceName, FeatureCollector.Factory featureCollectors,
    Consumer<FeatureCollector.Feature> next) {
    if (OpenMapTilesProfile.OSM_SOURCE.equals(sourceName)) {
      var timer = stats.startTimer("boundaries");
      LongObjectMap<PreparedGeometry> countryBoundaries = prepareRegionPolygons();

      long number = 0;
      for (var entry : boundariesToMerge.entrySet()) {
        number++;
        CountryBoundaryComponent key = entry.getKey();
        LineMerger merger = new LineMerger();
        for (Geometry geom : entry.getValue()) {
          merger.add(geom);
        }
        entry.getValue().clear();
        for (Object merged : merger.getMergedLineStrings()) {
          if (merged instanceof LineString lineString) {
            BorderingRegions borderingRegions = getBorderingRegions(countryBoundaries, key.regions, lineString);

            var features = featureCollectors.get(new ReaderFeature(
              GeoUtils.worldToLatLonCoords(lineString),
              Map.of(),
              number
            ));
            features.line(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
              .setAttr(Fields.ADMIN_LEVEL, key.adminLevel)
              .setAttr(Fields.DISPUTED, key.disputed ? 1 : 0)
              .setAttr(Fields.MARITIME, key.maritime ? 1 : 0)
              .setAttr(Fields.CLAIMED_BY, key.claimedBy)
              .setAttr(Fields.DISPUTED_NAME, key.disputed ? editName(key.name) : null)
              .setAttr(Fields.ADM0_L, regionNames.get(borderingRegions.left))
              .setAttr(Fields.ADM0_R, regionNames.get(borderingRegions.right))
              .setMinPixelSizeAtAllZooms(0)
              .setZoomRange(key.minzoom, 14);
            for (var feature : features) {
              next.accept(feature);
            }
          }
        }
      }
      timer.stop();
    }
  }

  @Override
  public List<VectorTileEncoder.Feature> postProcess(int zoom, List<VectorTileEncoder.Feature> items)
    throws GeometryException {
    double tolerance = zoom >= 14 ? 256d / 4096d : 0.1;
    return FeatureMerge.mergeLineStrings(items, 1, tolerance, BUFFER_SIZE);
  }

  @NotNull
  private BorderingRegions getBorderingRegions(
    LongObjectMap<PreparedGeometry> countryBoundaries,
    Set<Long> regions,
    LineString lineString
  ) {
    Long rightCountry = null, leftCountry = null;
    List<Long> rights = new ArrayList<>();
    List<Long> lefts = new ArrayList<>();
    int steps = 10;
    for (int i = 0; i < steps; i++) {
      double ratio = (double) (i + 1) / (steps + 2);
      Point right = GeoUtils.pointAlongOffset(lineString, ratio, COUNTRY_TEST_OFFSET);
      Point left = GeoUtils.pointAlongOffset(lineString, ratio, -COUNTRY_TEST_OFFSET);
      for (Long regionId : regions) {
        PreparedGeometry geom = countryBoundaries.get(regionId);
        if (geom != null) {
          if (geom.contains(right)) {
            rights.add(regionId);
          } else if (geom.contains(left)) {
            lefts.add(regionId);
          }
        }
      }
    }

    var right = mode(rights);
    if (right != null) {
      rightCountry = right.getKey();
      lefts.removeAll(List.of(rightCountry));
    }
    var left = mode(lefts);
    if (left != null) {
      leftCountry = left.getKey();
    }

    if (leftCountry == null && rightCountry == null) {
      LOGGER.warn("[boundaries] no left or right country for border between " + regions);
    }

    return new BorderingRegions(leftCountry, rightCountry);
  }

  @NotNull
  private LongObjectMap<PreparedGeometry> prepareRegionPolygons() {
    LOGGER.info("[boundaries] Creating polygons for " + regionGeometries.size() + " boundaries");
    LongObjectMap<PreparedGeometry> countryBoundaries = new GHLongObjectHashMap<>();
    for (var entry : regionGeometries.entrySet()) {
      Long regionId = entry.getKey();
      Polygonizer polygonizer = new Polygonizer();
      polygonizer.add(entry.getValue());
      try {
        Geometry combined = polygonizer.getGeometry().union();
        if (combined.isEmpty()) {
          LOGGER.warn("[boundaries] No valid polygons found for " + regionId);
        } else {
          countryBoundaries.put(regionId, PreparedGeometryFactory.prepare(combined));
        }
      } catch (TopologyException e) {
        LOGGER.warn("[boundaries] Unable to build boundary polygon for " + regionId + ": " + e.getMessage());
      }
    }
    LOGGER.info("[boundaries] Finished creating polygons");
    return countryBoundaries;
  }

  private Map.Entry<Long, Long> mode(List<Long> rights) {
    return rights.stream()
      .collect(groupingBy(Function.identity(), counting())).entrySet().stream()
      .max(Map.Entry.comparingByValue())
      .orElse(null);
  }

  private static record BorderingRegions(Long left, Long right) {}

  private static record BoundaryRelation(
    long id,
    int adminLevel,
    boolean disputed,
    String name,
    String claimedBy,
    String iso3166alpha3
  ) implements OpenStreetMapReader.RelationInfo {

    @Override
    public long estimateMemoryUsageBytes() {
      return 29 + 8 + MemoryEstimator.size(name)
        + 8 + MemoryEstimator.size(claimedBy)
        + 8 + MemoryEstimator.size(iso3166alpha3);
    }
  }

  private static record CountryBoundaryComponent(
    int adminLevel,
    boolean disputed,
    boolean maritime,
    int minzoom,
    Geometry line,
    Set<Long> regions,
    String claimedBy,
    String name
  ) {

    CountryBoundaryComponent groupingKey() {
      return new CountryBoundaryComponent(adminLevel, disputed, maritime, minzoom, null, regions, claimedBy, name);
    }

  }
}
