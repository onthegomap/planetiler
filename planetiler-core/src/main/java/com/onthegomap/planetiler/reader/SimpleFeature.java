package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.Lineal;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.Polygonal;
import org.locationtech.jts.geom.Puntal;

/**
 * An input feature read from a data source with geometry and tags known at creation-time.
 */
public class SimpleFeature extends SourceFeature {

  private static final AtomicLong idGenerator = new AtomicLong(0);
  private final Map<String, Object> tags;
  private Geometry worldGeometry;
  private Geometry latLonGeometry;

  private SimpleFeature(Geometry latLonGeometry, Geometry worldGeometry, Map<String, Object> tags, String source,
    String sourceLayer,
    long id, List<OsmReader.RelationMember<OsmRelationInfo>> relations) {
    super(tags, source, sourceLayer, relations, id);
    assert latLonGeometry == null || worldGeometry == null : "Cannot specify both a world and lat/lon geometry";
    this.latLonGeometry = latLonGeometry;
    // we expect outer polygons to appear before inner ones, so process ones with larger areas first
    this.worldGeometry = worldGeometry == null ? null : GeoUtils.sortPolygonsByAreaDescending(worldGeometry);
    this.tags = tags;
  }

  /**
   * Returns a new feature with a lat/lon geometry, tags, and source information.
   *
   * @param latLonGeometry geometry in latitude/longitude coordinates, will be converted to world web mercator on read
   * @param tags           key/value pairs of data for this feature from input source
   * @param source         input source ID (i.e. "natural_earth")
   * @param sourceLayer    input source layer (i.e. natural earth table name) or null if source does not have layers
   * @param id             numeric ID within the source
   * @return the new feature
   */
  public static SimpleFeature create(Geometry latLonGeometry, Map<String, Object> tags, String source,
    String sourceLayer, long id) {
    return new SimpleFeature(latLonGeometry, null, tags, source, sourceLayer, id, null);
  }

  /** Returns a new feature with no tags and a geometry specified in latitude/longitide coordinates. */
  public static SimpleFeature fromLatLonGeometry(Geometry latLonGeometry) {
    return new SimpleFeature(latLonGeometry, null, Map.of(), null, null, idGenerator.incrementAndGet(), null);
  }

  /**
   * Returns a new feature with no tags and a geometry specified in world web mercator coordinates where (0,0) is the
   * northwest and (1,1) is the southeast corner of the planet.
   */
  public static SimpleFeature fromWorldGeometry(Geometry worldGeometry) {
    return new SimpleFeature(null, worldGeometry, Map.of(), null, null, idGenerator.incrementAndGet(), null);
  }

  /** Returns a new feature with empty geometry and no tags. */
  public static SimpleFeature empty() {
    return fromWorldGeometry(GeoUtils.JTS_FACTORY.createGeometryCollection());
  }

  /**
   * Returns a new feature without source information if you need a {@code SimpleFeature} but don't plan on passing it
   * to a profile.
   */
  public static SimpleFeature create(Geometry latLonGeometry, Map<String, Object> tags) {
    return new SimpleFeature(latLonGeometry, null, tags, null, null, idGenerator.incrementAndGet(), null);
  }

  /** Returns a new feature with OSM relation info. Useful for setting up inputs for OSM unit tests. */
  public static SimpleFeature createFakeOsmFeature(Geometry latLonGeometry, Map<String, Object> tags, String source,
    String sourceLayer, long id, List<OsmReader.RelationMember<OsmRelationInfo>> relations) {
    String area = (String) tags.get("area");
    return new SimpleFeature(latLonGeometry, null, tags, source, sourceLayer, id, relations) {
      @Override
      public boolean canBePolygon() {
        return latLonGeometry instanceof Polygonal || (latLonGeometry instanceof LineString line
          && OsmReader.canBePolygon(line.isClosed(), area, latLonGeometry.getNumPoints()));
      }

      @Override
      public boolean canBeLine() {
        return latLonGeometry instanceof MultiLineString || (latLonGeometry instanceof LineString line
          && OsmReader.canBeLine(line.isClosed(), area, latLonGeometry.getNumPoints()));
      }

      @Override
      protected Geometry computePolygon() {
        var geom = worldGeometry();
        return geom instanceof LineString line ? GeoUtils.JTS_FACTORY.createPolygon(line.getCoordinates()) : geom;
      }
    };
  }

  @Override
  public Geometry latLonGeometry() {
    return latLonGeometry != null ? latLonGeometry
      : (latLonGeometry = GeoUtils.worldToLatLonCoords(worldGeometry));
  }

  @Override
  public Geometry worldGeometry() {
    // we expect outer polygons to appear before inner ones, so process ones with larger areas first
    return worldGeometry != null ? worldGeometry
      : (worldGeometry = GeoUtils.sortPolygonsByAreaDescending(GeoUtils.latLonToWorldCoords(latLonGeometry)));
  }

  @Override
  public Map<String, Object> tags() {
    return tags;
  }

  @Override
  public boolean isPoint() {
    return latLonGeometry instanceof Puntal || worldGeometry instanceof Puntal;
  }

  @Override
  public boolean canBePolygon() {
    return latLonGeometry instanceof Polygonal || worldGeometry instanceof Polygonal;
  }

  @Override
  public boolean canBeLine() {
    return latLonGeometry instanceof Lineal || worldGeometry instanceof Lineal;
  }

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    if (obj == null || obj.getClass() != this.getClass()) {
      return false;
    }
    var that = (SimpleFeature) obj;
    return Objects.equals(this.latLonGeometry, that.latLonGeometry) &&
      Objects.equals(this.tags, that.tags);
  }

  @Override
  public int hashCode() {
    return Objects.hash(latLonGeometry, tags);
  }

  @Override
  public String toString() {
    return "SimpleFeature[" +
      "geometry type=" + latLonGeometry().getGeometryType() + ", " +
      "tags=" + tags + ']';
  }
}
