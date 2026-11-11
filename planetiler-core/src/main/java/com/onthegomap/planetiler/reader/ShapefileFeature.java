package com.onthegomap.planetiler.reader;

import static com.onthegomap.planetiler.geo.GeoUtils.JTS_FACTORY;

import com.onthegomap.planetiler.geo.GeoUtils;
import com.onthegomap.planetiler.geo.GeometryException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.List;
import java.util.Map;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.api.referencing.operation.TransformException;
import org.geotools.data.shapefile.shp.ShapeHandler;
import org.geotools.data.shapefile.shp.ShapeType;
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
  private final ShapeType fileShapeType;
  private final MathTransform transformToLatLon;
  private Geometry latLonGeometry;
  private Geometry worldGeometry;
  private ShapeType shapeType;

  /**
   * @param rawShape          the record contents from the {@code .shp} file, starting with the shape type
   * @param fileShapeType     the shape type in the file header, which picks the decoder
   * @param transformToLatLon transform from the file's coordinate reference system to lat/lon, or null if it is lat/lon
   */
  ShapefileFeature(byte[] rawShape, ShapeType fileShapeType, MathTransform transformToLatLon, Map<String, Object> tags,
    String source, String sourceLayer, long id) {
    super(tags, source, sourceLayer, List.of(), id);
    this.rawShape = rawShape;
    this.fileShapeType = fileShapeType;
    this.transformToLatLon = transformToLatLon;
  }

  @Override
  public Geometry latLonGeometry() throws GeometryException {
    if (latLonGeometry == null) {
      try {
        // first 4 bytes contain the shape type, then geometry content is after that
        Geometry geometry = (Geometry) newHandler().read(contentBuffer().position(4), recordShapeType(), false);
        latLonGeometry = transformToLatLon == null ? geometry : JTS.transform(geometry, transformToLatLon);
      } catch (RuntimeException | IOException | TransformException e) {
        // a broken record in the input isn't a bug, so it's non-fatal
        var exception = new GeometryException("shapefile_read_geometry", "Can't read geometry of " + this + ": " + e,
          true);
        exception.initCause(e);
        throw exception;
      }
    }
    return latLonGeometry;
  }

  private ShapeHandler newHandler() throws IOException {
    // GeoTools' handlers are not thread safe, so each decode gets its own
    return fileShapeType == ShapeType.POLYGON ? new ShapefilePolygonHandler() :
      fileShapeType.getShapeHandler(JTS_FACTORY);
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
    var type = recordShapeType();
    // from spec "Shape Types in X, Y Space": a point has X and Y; a multipoint has a 32 byte box then NumPoints; a
    // polyline or polygon has the box, NumParts, then NumPoints. Z and M types add their values after the same fields.
    int numPointsOffset = type.isPointType() ? -1 : type.isMultiPointType() ? 36 : 40;
    return numPointsOffset < 0 || rawShape.length < numPointsOffset + 4 ? 1 :
      Math.max(1, contentBuffer().getInt(numPointsOffset));
  }

  private ShapeType recordShapeType() {
    return shapeType != null ? shapeType : (shapeType = ShapeType.forID(contentBuffer().getInt(0)));
  }

  private ByteBuffer contentBuffer() {
    // from spec "Organization of the Main File": record contents are little endian
    return ByteBuffer.wrap(rawShape).order(ByteOrder.LITTLE_ENDIAN);
  }
}
