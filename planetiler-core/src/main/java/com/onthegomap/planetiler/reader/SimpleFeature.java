package com.onthegomap.planetiler.reader;

import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryType;
import com.onthegomap.planetiler.reader.osm.OsmElement;
import com.onthegomap.planetiler.reader.osm.OsmReader;
import com.onthegomap.planetiler.reader.osm.OsmRelationInfo;
import com.onthegomap.planetiler.reader.osm.OsmSourceFeature;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.Polygonal;

/**
 * An input feature read from a data source with geometry and tags known at creation-time.
 */
public class SimpleFeature extends SourceFeature {

  private final Map<String, Object> tags;
  private Geometry worldGeometry;
  private Geometry latLonGeometry;

  private SimpleFeature(Geometry latLonGeometry, Geometry worldGeometry, Map<String, Object> tags, String source,
    String sourceLayer, long id, List<OsmReader.RelationMember<OsmRelationInfo>> relations) {
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

  /** Returns a new feature with no tags and a geometry specified in latitude/longitude coordinates. */
  public static SimpleFeature fromLatLonGeometry(Geometry latLonGeometry, long id) {
    return new SimpleFeature(latLonGeometry, null, Map.of(), null, null, id, null);
  }

  /** Alias for {@link #fromLatLonGeometry(Geometry, long)} with no ID set. */
  public static SimpleFeature fromLatLonGeometry(Geometry worldGeometry) {
    return fromLatLonGeometry(worldGeometry, VectorTile.NO_FEATURE_ID);
  }

  /**
   * Returns a new feature with no tags and a geometry specified in world web mercator coordinates where (0,0) is the
   * northwest and (1,1) is the southeast corner of the planet.
   */
  public static SimpleFeature fromWorldGeometry(Geometry worldGeometry, long id) {
    return new SimpleFeature(null, worldGeometry, Map.of(), null, null, id, null);
  }

  /** Alias for {@link #fromWorldGeometry(Geometry, long)} with no ID set. */
  public static SimpleFeature fromWorldGeometry(Geometry worldGeometry) {
    return fromWorldGeometry(worldGeometry, VectorTile.NO_FEATURE_ID);
  }

  /**
   * Returns a new feature without source information if you need a {@code SimpleFeature} but don't plan on passing it
   * to a profile.
   */
  public static SimpleFeature create(Geometry latLonGeometry, Map<String, Object> tags, long id) {
    return new SimpleFeature(latLonGeometry, null, tags, null, null, id, null);
  }

  /** Alias for {@link #create(Geometry, Map, long)} with no ID set. */
  public static SimpleFeature create(Geometry latLonGeometry, Map<String, Object> tags) {
    return create(latLonGeometry, tags, VectorTile.NO_FEATURE_ID);
  }

  private static class SimpleOsmFeature extends SimpleFeature implements OsmSourceFeature {

    private final String area;
    private final OsmElement.Info info;

    private SimpleOsmFeature(Geometry latLonGeometry, Geometry worldGeometry, Map<String, Object> tags, String source,
      String sourceLayer, long id, List<OsmReader.RelationMember<OsmRelationInfo>> relations, OsmElement.Info info) {
      super(latLonGeometry, worldGeometry, tags, source, sourceLayer, id, relations);
      this.area = (String) tags.get("area");
      this.info = info;
    }

    @Override
    public boolean canBePolygon() {
      return latLonGeometry() instanceof Polygonal || (latLonGeometry() instanceof LineString line &&
        OsmReader.canBePolygon(line.isClosed(), area, latLonGeometry().getNumPoints()));
    }

    @Override
    public boolean canBeLine() {
      return latLonGeometry() instanceof MultiLineString || (latLonGeometry() instanceof LineString line &&
        OsmReader.canBeLine(line.isClosed(), area, latLonGeometry().getNumPoints()));
    }

    @Override
    protected Geometry computePolygon() {
      var geom = worldGeometry();
      return geom instanceof LineString line ? GeoUtils.JTS_FACTORY.createPolygon(line.getCoordinates()) : geom;
    }


    @Override
    public OsmElement originalElement() {
      return new OsmElement() {
        @Override
        public long id() {
          return SimpleOsmFeature.this.id();
        }

        @Override
        public Info info() {
          return info;
        }

        @Override
        public int cost() {
          return 1;
        }

        @Override
        public Type type() {
          return isPoint() ? Type.NODE : Type.WAY;
        }

        @Override
        public Map<String, Object> tags() {
          return tags();
        }
      };
    }

    @Override
    public boolean equals(Object o) {
      return this == o || (o instanceof SimpleOsmFeature other && super.equals(other) &&
        Objects.equals(area, other.area) && Objects.equals(info, other.info));
    }

    @Override
    public int hashCode() {
      int result = super.hashCode();
      result = 31 * result + (area != null ? area.hashCode() : 0);
      result = 31 * result + (info != null ? info.hashCode() : 0);
      return result;
    }
  }

  /** Returns a new feature with OSM relation info. Useful for setting up inputs for OSM unit tests. */
  public static SimpleFeature createFakeOsmFeature(Geometry latLonGeometry, Map<String, Object> tags, String source,
    String sourceLayer, long id, List<OsmReader.RelationMember<OsmRelationInfo>> relations) {
    return createFakeOsmFeature(latLonGeometry, tags, source, sourceLayer, id, relations, null);
  }

  /** Returns a new feature with OSM relation info and metadata. Useful for setting up inputs for OSM unit tests. */
  public static SimpleFeature createFakeOsmFeature(Geometry latLonGeometry, Map<String, Object> tags, String source,
    String sourceLayer, long id, List<OsmReader.RelationMember<OsmRelationInfo>> relations, OsmElement.Info info) {
    return new SimpleOsmFeature(latLonGeometry, null, tags, source, sourceLayer, id, relations, info);
  }

  @Override
  public Geometry latLonGeometry() {
    return latLonGeometry != null ? latLonGeometry : (latLonGeometry = GeoUtils.worldToLatLonCoords(worldGeometry));
  }

  @Override
  public Geometry worldGeometry() {
    // we expect outer polygons to appear before inner ones, so process ones with larger areas first
    return worldGeometry != null ? worldGeometry :
      (worldGeometry = GeoUtils.sortPolygonsByAreaDescending(GeoUtils.latLonToWorldCoords(latLonGeometry)));
  }

  @Override
  public Map<String, Object> tags() {
    return tags;
  }

  @Override
  public boolean isPoint() {
    return GeometryType.POINT.equals(GeometryType.typeOf(latLonGeometry != null ? latLonGeometry : worldGeometry));
  }

  @Override
  public boolean canBePolygon() {
    return GeometryType.POLYGON.equals(GeometryType.typeOf(latLonGeometry != null ? latLonGeometry : worldGeometry));
  }

  @Override
  public boolean canBeLine() {
    return GeometryType.LINE.equals(GeometryType.typeOf(latLonGeometry != null ? latLonGeometry : worldGeometry));
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
    return "Feature[source=" + getSource() +
      ", source layer=" + getSourceLayer() +
      ", id=" + id() +
      ", geometry type=" + latLonGeometry().getGeometryType() +
      ", tags=" + tags + ']';
  }
}
