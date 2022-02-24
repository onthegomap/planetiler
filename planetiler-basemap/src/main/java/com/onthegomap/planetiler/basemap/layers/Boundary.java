/*
Copyright (c) 2021, MapTiler.com & OpenMapTiles contributors.
All rights reserved.

Code license: BSD 3-Clause License

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are met:

* Redistributions of source code must retain the above copyright notice, this
  list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright notice,
  this list of conditions and the following disclaimer in the documentation
  and/or other materials provided with the distribution.

* Neither the name of the copyright holder nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE LIABLE
FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL
DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR
SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER
CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY,
OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

Design license: CC-BY 4.0

See https://github.com/openmaptiles/openmaptiles/blob/master/LICENSE.md for details on usage
*/
package com.onthegomap.planetiler.basemap.layers;

import static com.onthegomap.planetiler.util.MemoryEstimator.CLASS_HEADER_BYTES;
import static com.onthegomap.planetiler.util.MemoryEstimator.POINTER_BYTES;
import static com.onthegomap.planetiler.util.MemoryEstimator.estimateSize;
import static java.util.stream.Collectors.counting;
import static java.util.stream.Collectors.groupingBy;

import com.carrotsearch.hppc.LongObjectMap;
import com.graphhopper.coll.GHLongObjectHashMap;
import com.onthegomap.planetiler.FeatureCollector;
import com.onthegomap.planetiler.FeatureMerge;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.basemap.BasemapProfile;
import com.onthegomap.planetiler.basemap.generated.OpenMapTilesSchema;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SimpleFeature;
import com.onthegomap.planetiler.reader.SourceFeature;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import com.onthegomap.planetiler.stats.Stats;
import com.onthegomap.planetiler.util.Format;
import com.onthegomap.planetiler.util.MemoryEstimator;
import com.onthegomap.planetiler.util.Parse;
import com.onthegomap.planetiler.util.Translations;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.locationtech.jts.geom.Coordinate;
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

/**
 * Defines the logic for generating map elements for country, state, and town boundaries in the {@code boundary} layer
 * from source features.
 * <p>
 * This class is ported to Java from <a href="https://github.com/openmaptiles/openmaptiles/tree/master/layers/boundary">OpenMapTiles
 * boundary sql files</a>.
 */
public class Boundary implements
  OpenMapTilesSchema.Boundary,
  BasemapProfile.NaturalEarthProcessor,
  BasemapProfile.OsmRelationPreprocessor,
  BasemapProfile.OsmAllProcessor,
  BasemapProfile.FeaturePostProcessor,
  BasemapProfile.FinishHandler {

  /*
   * Uses natural earth at lower zoom levels and OpenStreetMap at higher zoom levels.
   *
   * For OpenStreetMap data at higher zoom levels:
   * 1) Preprocess relations on the first pass to extract info for relations where
   *    type=boundary and boundary=administrative and store the admin_level for
   *    later.
   * 2) When processing individual ways, take the minimum (most important) admin
   *    level of every relation they are a part of and use that as the admin level
   *    for the way.
   * 3) If boundary_country_names argument is true and the way is part of a country
   *    (admin_level=2) boundary, then hold onto it for later
   * 4) When we finish processing the OSM source, build country polygons from the
   *    saved ways and use that to determine which country is on the left and right
   *    side of each way, then emit the way with ADM0_L and ADM0_R keys set.
   * 5) Before emitting boundary lines, merge linestrings with the same tags.
   */

  private static final Logger LOGGER = LoggerFactory.getLogger(Boundary.class);
  private static final double COUNTRY_TEST_OFFSET = GeoUtils.metersToPixelAtEquator(0, 10) / 256d;
  private final Stats stats;
  private final boolean addCountryNames;
  // may be updated concurrently by multiple threads
  private final Map<Long, String> regionNames = new ConcurrentHashMap<>();
  // need to synchronize updates to these shared data structures:
  private final Map<Long, List<Geometry>> regionGeometries = new HashMap<>();
  private final Map<CountryBoundaryComponent, List<Geometry>> boundariesToMerge = new HashMap<>();
  private final PlanetilerConfig config;

  public Boundary(Translations translations, PlanetilerConfig config, Stats stats) {
    this.config = config;
    this.addCountryNames = config.arguments().getBoolean(
      "boundary_country_names",
      "boundary layer: add left/right codes of neighboring countries",
      true
    );
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
        yield minZoom != null && minZoom <= 7 ? new BoundaryInfo(4, 1, 4) :
          minZoom != null && minZoom <= 7.7 ? new BoundaryInfo(4, 4, 4) :
            null;
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
  public List<OsmRelationInfo> preprocessOsmRelation(OsmElement.Relation relation) {
    if (relation.hasTag("type", "boundary") &&
      relation.hasTag("admin_level") &&
      relation.hasTag("boundary", "administrative")) {
      Integer adminLevelValue = Parse.parseRoundInt(relation.getTag("admin_level"));
      String code = relation.getString("ISO3166-1:alpha3");
      if (adminLevelValue != null && adminLevelValue >= 2 && adminLevelValue <= 10) {
        boolean disputed = isDisputed(relation.tags());
        if (code != null) {
          regionNames.put(relation.id(), code);
        }
        return List.of(new BoundaryRelation(
          relation.id(),
          adminLevelValue,
          disputed,
          relation.getString("name"),
          disputed ? relation.getString("claimed_by") : null,
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
      // aggregate all borders this way is a part of - take the lowest
      // admin level, and assume it is disputed if any relation is disputed.
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
        boolean wayIsDisputed = isDisputed(feature.tags());
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
        if (addCountryNames && !regionIds.isEmpty()) {
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
            // multiple threads may update this concurrently
            synchronized (this) {
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
            .setMinZoom(minzoom)
            .setAttr(Fields.CLAIMED_BY, claimedBy)
            .setAttr(Fields.DISPUTED_NAME, editName(disputedName));
        }
      }
    }
  }

  @Override
  public void finish(String sourceName, FeatureCollector.Factory featureCollectors,
    Consumer<FeatureCollector.Feature> emit) {
    if (BasemapProfile.OSM_SOURCE.equals(sourceName)) {
      var timer = stats.startStage("boundaries");
      LongObjectMap<PreparedGeometry> countryBoundaries = prepareRegionPolygons();

      for (var entry : boundariesToMerge.entrySet()) {
        CountryBoundaryComponent key = entry.getKey();
        LineMerger merger = new LineMerger();
        for (Geometry geom : entry.getValue()) {
          merger.add(geom);
        }
        entry.getValue().clear();
        for (Object merged : merger.getMergedLineStrings()) {
          if (merged instanceof LineString lineString) {
            BorderingRegions borderingRegions = getBorderingRegions(countryBoundaries, key.regions, lineString);

            var features = featureCollectors.get(SimpleFeature.fromWorldGeometry(lineString));
            features.line(LAYER_NAME).setBufferPixels(BUFFER_SIZE)
              .setAttr(Fields.ADMIN_LEVEL, key.adminLevel)
              .setAttr(Fields.DISPUTED, key.disputed ? 1 : 0)
              .setAttr(Fields.MARITIME, key.maritime ? 1 : 0)
              .setAttr(Fields.CLAIMED_BY, key.claimedBy)
              .setAttr(Fields.DISPUTED_NAME, key.disputed ? editName(key.name) : null)
              .setAttr(Fields.ADM0_L, borderingRegions.left == null ? null : regionNames.get(borderingRegions.left))
              .setAttr(Fields.ADM0_R, borderingRegions.right == null ? null : regionNames.get(borderingRegions.right))
              .setMinPixelSizeAtAllZooms(0)
              .setMinZoom(key.minzoom);
            for (var feature : features) {
              emit.accept(feature);
            }
          }
        }
      }
      timer.stop();
    }
  }

  @Override
  public List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) {
    double minLength = config.minFeatureSize(zoom);
    double tolerance = config.tolerance(zoom);
    return FeatureMerge.mergeLineStrings(items, attrs -> minLength, tolerance, BUFFER_SIZE);
  }

  /** Returns the left and right country for {@code lineString}. */
  private BorderingRegions getBorderingRegions(
    LongObjectMap<PreparedGeometry> countryBoundaries,
    Set<Long> allRegions,
    LineString lineString
  ) {
    Set<Long> validRegions = allRegions.stream()
      .filter(countryBoundaries::containsKey)
      .collect(Collectors.toSet());
    if (validRegions.isEmpty()) {
      return BorderingRegions.empty();
    }
    List<Long> rights = new ArrayList<>();
    List<Long> lefts = new ArrayList<>();
    int steps = 10;
    for (int i = 0; i < steps; i++) {
      double ratio = (double) (i + 1) / (steps + 2);
      Point right = GeoUtils.pointAlongOffset(lineString, ratio, COUNTRY_TEST_OFFSET);
      Point left = GeoUtils.pointAlongOffset(lineString, ratio, -COUNTRY_TEST_OFFSET);
      for (Long regionId : validRegions) {
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
      lefts.removeAll(List.of(right));
    }
    var left = mode(lefts);

    if (left == null && right == null) {
      Coordinate point = GeoUtils.worldToLatLonCoords(GeoUtils.pointAlongOffset(lineString, 0.5, 0)).getCoordinate();
      LOGGER.warn("no left or right country for border between OSM country relations: %s around %s"
        .formatted(
          validRegions,
          Format.osmDebugUrl(10, point)
        ));
    }

    return new BorderingRegions(left, right);
  }

  /** Returns a map from region ID to prepared geometry optimized for {@code contains} queries. */
  private LongObjectMap<PreparedGeometry> prepareRegionPolygons() {
    LOGGER.info("Creating polygons for " + regionGeometries.size() + " boundaries");
    LongObjectMap<PreparedGeometry> countryBoundaries = new GHLongObjectHashMap<>();
    for (var entry : regionGeometries.entrySet()) {
      Long regionId = entry.getKey();
      Polygonizer polygonizer = new Polygonizer();
      polygonizer.add(entry.getValue());
      try {
        Geometry combined = polygonizer.getGeometry().union();
        if (combined.isEmpty()) {
          LOGGER.warn("Unable to form closed polygon for OSM relation " + regionId
            + " (likely missing edges)");
        } else {
          countryBoundaries.put(regionId, PreparedGeometryFactory.prepare(combined));
        }
      } catch (TopologyException e) {
        LOGGER
          .warn("Unable to build boundary polygon for OSM relation " + regionId + ": " + e.getMessage());
      }
    }
    LOGGER.info("Finished creating " + countryBoundaries.size() + " country polygons");
    return countryBoundaries;
  }

  /** Returns most frequently-occurring element in {@code list}. */
  private static Long mode(List<Long> list) {
    return list.stream()
      .collect(groupingBy(Function.identity(), counting())).entrySet().stream()
      .max(Map.Entry.comparingByValue())
      .map(Map.Entry::getKey)
      .orElse(null);
  }

  private record BorderingRegions(Long left, Long right) {

    public static BorderingRegions empty() {
      return new BorderingRegions(null, null);
    }
  }

  /**
   * Minimal set of information extracted from a boundary relation to be used when processing each way in that
   * relation.
   */
  private record BoundaryRelation(
    long id,
    int adminLevel,
    boolean disputed,
    String name,
    String claimedBy,
    String iso3166alpha3
  ) implements OsmRelationInfo {

    @Override
    public long estimateMemoryUsageBytes() {
      return CLASS_HEADER_BYTES
        + MemoryEstimator.estimateSizeLong(id)
        + MemoryEstimator.estimateSizeInt(adminLevel)
        + estimateSize(disputed)
        + POINTER_BYTES + estimateSize(name)
        + POINTER_BYTES + estimateSize(claimedBy)
        + POINTER_BYTES + estimateSize(iso3166alpha3);
    }
  }

  /** Information to hold onto from processing a way in a boundary relation to determine the left/right region ID later. */
  private record CountryBoundaryComponent(
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
