package com.onthegomap.flatmap.openmaptiles.layers;

import static com.onthegomap.flatmap.openmaptiles.Utils.nullIfEmpty;

import com.carrotsearch.hppc.LongObjectMap;
import com.graphhopper.coll.GHLongObjectHashMap;
import com.onthegomap.flatmap.Arguments;
import com.onthegomap.flatmap.FeatureCollector;
import com.onthegomap.flatmap.Parse;
import com.onthegomap.flatmap.SourceFeature;
import com.onthegomap.flatmap.Translations;
import com.onthegomap.flatmap.geo.GeoUtils;
import com.onthegomap.flatmap.geo.GeometryException;
import com.onthegomap.flatmap.monitoring.Stats;
import com.onthegomap.flatmap.openmaptiles.LanguageUtils;
import com.onthegomap.flatmap.openmaptiles.OpenMapTilesProfile;
import com.onthegomap.flatmap.openmaptiles.generated.OpenMapTilesSchema;
import com.onthegomap.flatmap.openmaptiles.generated.Tables;
import java.util.Map;
import java.util.TreeMap;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class WaterName implements OpenMapTilesSchema.WaterName,
  Tables.OsmMarinePoint.Handler,
  Tables.OsmWaterPolygon.Handler,
  OpenMapTilesProfile.NaturalEarthProcessor,
  OpenMapTilesProfile.LakeCenterlineProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(WaterName.class);

  private final Translations translations;
  private final LongObjectMap<Geometry> lakeCenterlines = new GHLongObjectHashMap<>();
  private final TreeMap<String, Integer> importantMarinePoints = new TreeMap<>();

  @Override
  public void release() {
    lakeCenterlines.release();
    importantMarinePoints.clear();
  }

  public WaterName(Translations translations, Arguments args, Stats stats) {
    this.translations = translations;
  }

  @Override
  public void processNaturalEarth(String table, SourceFeature feature,
    FeatureCollector features) {
    if ("ne_10m_geography_marine_polys".equals(table)) {
      String name = feature.getString("name");
      Integer scalerank = Parse.parseIntOrNull(feature.getTag("scalerank"));
      if (name != null && scalerank != null) {
        name = name.replaceAll("\\s+", " ").trim().toLowerCase();
        synchronized (importantMarinePoints) {
          importantMarinePoints.put(name, scalerank);
        }
      }
    }
  }

  @Override
  public void processLakeCenterline(SourceFeature feature, FeatureCollector features) {
    long osmId = Math.abs(feature.getLong("OSM_ID"));
    if (osmId == 0L) {
      LOGGER.warn("Bad lake centerline: " + feature);
    } else {
      synchronized (lakeCenterlines) {
        try {
          lakeCenterlines.put(osmId, feature.worldGeometry());
        } catch (GeometryException e) {
          LOGGER.warn("Bad lake centerline geometry: " + feature, e);
        }
      }
    }
  }

  @Override
  public void process(Tables.OsmMarinePoint element, FeatureCollector features) {
    if (!element.name().isBlank()) {
      String place = element.place();
      var source = element.source();
      // use name from OSM, but min zoom from natural earth if it exists
      Integer rank = Parse.parseIntOrNull(source.getTag("rank"));
      Integer nerank;
      String name = element.name().toLowerCase();
      if ((nerank = importantMarinePoints.get(name)) != null) {
        rank = nerank;
      } else if ((nerank = importantMarinePoints.get(source.getString("name:en", "").toLowerCase())) != null) {
        rank = nerank;
      } else if ((nerank = importantMarinePoints.get(source.getString("name:es", "").toLowerCase())) != null) {
        rank = nerank;
      } else {
        Map.Entry<String, Integer> next = importantMarinePoints.ceilingEntry(name);
        if (next != null && next.getKey().startsWith(name)) {
          rank = next.getValue();
        }
      }
      int minZoom = "ocean".equals(place) ? 0 : rank != null ? rank : 8;
      features.point(LAYER_NAME)
        .setBufferPixels(BUFFER_SIZE)
        .setAttrs(LanguageUtils.getNames(source.properties(), translations))
        .setAttr(Fields.CLASS, place)
        .setAttr(Fields.INTERMITTENT, element.isIntermittent() ? 1 : 0)
        .setZoomRange(minZoom, 14);
    }
  }

  private static final double WORLD_AREA_FOR_70K_SQUARE_METERS =
    Math.pow(GeoUtils.metersToPixelAtEquator(0, Math.sqrt(70_000)) / 256d, 2);
  private static final double LOG2 = Math.log(2);

  @Override
  public void process(Tables.OsmWaterPolygon element, FeatureCollector features) {
    if (nullIfEmpty(element.name()) != null) {
      try {
        Geometry centerlineGeometry = lakeCenterlines.get(element.source().id());
        FeatureCollector.Feature feature;
        int minzoom = 9;
        if (centerlineGeometry != null) {
          feature = features.geometry(LAYER_NAME, centerlineGeometry)
            .setMinPixelSizeBelowZoom(13, 6 * element.name().length());
        } else {
          feature = features.pointOnSurface(LAYER_NAME);
          Geometry geometry = element.source().worldGeometry();
          double area = geometry.getArea();
          minzoom = (int) Math.floor(20 - Math.log(area / WORLD_AREA_FOR_70K_SQUARE_METERS) / LOG2);
          minzoom = Math.min(14, Math.max(9, minzoom));
        }
        feature
          .setAttr(Fields.CLASS, "lake")
          .setBufferPixels(BUFFER_SIZE)
          .setAttrs(LanguageUtils.getNames(element.source().properties(), translations))
          .setAttr(Fields.INTERMITTENT, element.isIntermittent() ? 1 : 0)
          .setZoomRange(minzoom, 14);
      } catch (GeometryException e) {
        LOGGER.warn("Unable to get geometry for " + element + ": " + e);
      }
    }
  }
}
