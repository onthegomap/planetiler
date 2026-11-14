package com.onthegomap.planetiler.reader.shapefile;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.reader.SourceFeature;
import java.util.List;
import java.util.Map;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.api.referencing.operation.TransformException;
import org.geotools.geometry.jts.JTS;
import org.locationtech.jts.geom.Geometry;

/**
 * A single record read from a shapefile, whose geometry gets decoded and reprojected the first time it is requested.
 * <p>
 * {@link #latLonGeometry()} and {@link #worldGeometry()} throw {@link GeometryException} when the record's geometry
 * can't be read.
 */
public class ShapefileFeature extends SourceFeature {

  private final byte[] rawShape;
  private final MathTransform transformToLatLon;
  private Geometry latLonGeometry;
  private Geometry worldGeometry;
  private ShapeType shapeType;

  /**
   * @param rawShape          the record contents from the {@code .shp} file, starting with the shape type
   * @param transformToLatLon transform from the file's coordinate reference system to lat/lon, or null if it is lat/lon
   */
  ShapefileFeature(byte[] rawShape, MathTransform transformToLatLon, Map<String, Object> tags, String source,
    String sourceLayer, long id) {
    super(tags, source, sourceLayer, List.of(), id);
    this.rawShape = rawShape;
    this.transformToLatLon = transformToLatLon;
  }

  @Override
  public Geometry latLonGeometry() throws GeometryException {
    if (latLonGeometry == null) {
      try {
        Geometry geometry = ShpGeometryReader.read(rawShape);
        latLonGeometry = transformToLatLon == null ? geometry : JTS.transform(geometry, transformToLatLon);
      } catch (RuntimeException | TransformException e) {
        // a broken record in the input isn't a bug, so it's non-fatal
        var exception = new GeometryException("shapefile_read_geometry", "Can't read geometry of " + this + ": " + e,
          true);
        exception.initCause(e);
        throw exception;
      }
    }
    return latLonGeometry;
  }

  @Override
  public Geometry worldGeometry() throws GeometryException {
    // same as SimpleFeature: larger polygons first
    return worldGeometry != null ? worldGeometry :
      (worldGeometry = GeoUtils.sortPolygonsByAreaDescending(GeoUtils.latLonToWorldCoords(latLonGeometry())));
  }

  @Override
  public boolean isPoint() {
    var type = recordShapeType();
    return type.isPointType() || type.isMultiPointType();
  }

  @Override
  public boolean canBePolygon() {
    return recordShapeType().isPolygonType();
  }

  @Override
  public boolean canBeLine() {
    return recordShapeType().isLineType();
  }

  /** Returns the number of points in this record without decoding its geometry. */
  public int numPoints() {
    return ShpRecord.parseNumPoints(rawShape);
  }

  private ShapeType recordShapeType() {
    if (shapeType == null) {
      // unknown shape types count as null shapes here, decoding reports them
      shapeType = ShpRecord.parseShapeType(rawShape);
    }
    return shapeType;
  }
}
