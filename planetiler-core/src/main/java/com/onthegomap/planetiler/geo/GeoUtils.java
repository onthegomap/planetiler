package com.onthegomap.planetiler.geo;

import com.onthegomap.planetiler.collection.LongLongMap;
import com.onthegomap.planetiler.config.PlanetilerConfig;
import com.onthegomap.planetiler.stats.Stats;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;
import org.geotools.api.referencing.operation.MathTransform;
import org.geotools.geometry.jts.JTS;
import org.geotools.referencing.CRS;
import org.locationtech.jts.algorithm.Area;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.CoordinateXY;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.LineSegment;
import org.locationtech.jts.geom.LineString;
import org.locationtech.jts.geom.LinearRing;
import org.locationtech.jts.geom.MultiLineString;
import org.locationtech.jts.geom.MultiPoint;
import org.locationtech.jts.geom.MultiPolygon;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.PrecisionModel;
import org.locationtech.jts.geom.TopologyException;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.locationtech.jts.geom.impl.PackedCoordinateSequenceFactory;
import org.locationtech.jts.geom.util.GeometryFixer;
import org.locationtech.jts.geom.util.GeometryTransformer;
import org.locationtech.jts.io.WKBReader;
import org.locationtech.jts.io.WKTReader;
import org.locationtech.jts.precision.GeometryPrecisionReducer;

/**
 * A collection of utilities for working with JTS data structures and geographic data.
 * <p>
 * "world" coordinates in this class refer to web mercator coordinates where the top-left/northwest corner of the map is
 * (0,0) and bottom-right/southeast corner is (1,1).
 */
public class GeoUtils {

  /** Rounding precision for 256x256px tiles encoded using 4096 values. */
  public static final PrecisionModel TILE_PRECISION = new PrecisionModel(4096d / 256d);

  public static final GeometryFactory JTS_FACTORY = new GeometryFactory(PackedCoordinateSequenceFactory.DOUBLE_FACTORY);

  public static final Geometry EMPTY_GEOMETRY = JTS_FACTORY.createGeometryCollection();
  public static final CoordinateSequence EMPTY_COORDINATE_SEQUENCE = new PackedCoordinateSequence.Double(0, 2, 0);
  public static final Point EMPTY_POINT = JTS_FACTORY.createPoint();
  public static final LineString EMPTY_LINE = JTS_FACTORY.createLineString();
  public static final Polygon EMPTY_POLYGON = JTS_FACTORY.createPolygon();
  private static final LineString[] EMPTY_LINE_STRING_ARRAY = new LineString[0];
  private static final Polygon[] EMPTY_POLYGON_ARRAY = new Polygon[0];
  private static final Point[] EMPTY_POINT_ARRAY = new Point[0];
  private static final double WORLD_RADIUS_METERS = 6_378_137;
  public static final double WORLD_CIRCUMFERENCE_METERS = Math.PI * 2 * WORLD_RADIUS_METERS;
  private static final double RADIANS_PER_DEGREE = Math.PI / 180;
  private static final double DEGREES_PER_RADIAN = 180 / Math.PI;
  private static final double LOG2 = Math.log(2);
  /**
   * Transform web mercator coordinates where top-left corner of the planet is (0,0) and bottom-right is (1,1) to
   * latitude/longitude coordinates.
   */
  private static final GeometryTransformer UNPROJECT_WORLD_COORDS = new GeometryTransformer() {
    @Override
    protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
      CoordinateSequence copy = new PackedCoordinateSequence.Double(coords.size(), 2, 0);
      for (int i = 0; i < coords.size(); i++) {
        copy.setOrdinate(i, 0, getWorldLon(coords.getX(i)));
        copy.setOrdinate(i, 1, getWorldLat(coords.getY(i)));
      }
      return copy;
    }
  };
  /**
   * Transform latitude/longitude coordinates to web mercator where top-left corner of the planet is (0,0) and
   * bottom-right is (1,1).
   */
  private static final GeometryTransformer PROJECT_WORLD_COORDS = new GeometryTransformer() {
    @Override
    protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
      CoordinateSequence copy = new PackedCoordinateSequence.Double(coords.size(), 2, 0);
      for (int i = 0; i < coords.size(); i++) {
        copy.setOrdinate(i, 0, getWorldX(coords.getX(i)));
        copy.setOrdinate(i, 1, getWorldY(coords.getY(i)));
      }
      return copy;
    }
  };

  private static class TileToLatLonTransformer extends GeometryTransformer {

    private final int tileX;
    private final int tileY;
    private final int zoom;

    public TileToLatLonTransformer(int tileX, int tileY, int zoom) {
      this.tileX = tileX;
      this.tileY = tileY;
      this.zoom = zoom;
    }

    @Override
    protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
      CoordinateSequence copy = new PackedCoordinateSequence.Double(coords.size(), 2, 0);
      for (int i = 0; i < coords.size(); i++) {
        double x = (tileX + coords.getX(i) / 256d) / Math.pow(2, zoom);
        double y = (tileY + coords.getY(i) / 256d) / Math.pow(2, zoom);
        copy.setOrdinate(i, 0, getWorldLon(x));
        copy.setOrdinate(i, 1, getWorldLat(y));
      }
      return copy;
    }
  }

  private static final double MAX_LAT = getWorldLat(-0.1);
  private static final double MIN_LAT = getWorldLat(1.1);
  // to pack latitude/longitude into a single long, we round them to 31 bits of precision
  private static final double QUANTIZED_WORLD_SIZE = Math.pow(2, 31);
  private static final double HALF_QUANTIZED_WORLD_SIZE = QUANTIZED_WORLD_SIZE / 2;
  private static final long LOWER_32_BIT_MASK = (1L << 32) - 1L;
  /**
   * Bounds for the entire area of the planet that a web mercator projection covers, where top left is (0,0) and bottom
   * right is (1,1).
   */
  public static final Envelope WORLD_BOUNDS = new Envelope(0, 1, 0, 1);
  /**
   * Bounds for the entire area of the planet that a web mercator projection covers in latitude/longitude coordinates.
   */
  public static final Envelope WORLD_LAT_LON_BOUNDS = toLatLonBoundsBounds(WORLD_BOUNDS);

  // should not instantiate
  private GeoUtils() {
  }

  /**
   * Returns a copy of {@code geom} transformed from latitude/longitude coordinates to web mercator where top-left
   * corner of the planet is (0,0) and bottom-right is (1,1).
   */
  public static Geometry latLonToWorldCoords(Geometry geom) {
    return PROJECT_WORLD_COORDS.transform(geom);
  }

  /**
   * Returns a copy of {@code geom} transformed from web mercator where top-left corner of the planet is (0,0) and
   * bottom-right is (1,1) to latitude/longitude.
   */
  public static Geometry worldToLatLonCoords(Geometry geom) {
    return UNPROJECT_WORLD_COORDS.transform(geom);
  }

  /**
   * Returns a copy of {@code worldBounds} transformed from web mercator where top-left corner of the planet is (0,0)
   * and bottom-right is (1,1) to latitude/longitude.
   */
  public static Envelope toLatLonBoundsBounds(Envelope worldBounds) {
    return new Envelope(
      getWorldLon(worldBounds.getMinX()),
      getWorldLon(worldBounds.getMaxX()),
      getWorldLat(worldBounds.getMinY()),
      getWorldLat(worldBounds.getMaxY()));
  }

  /**
   * Returns a copy of {@code lonLatBounds} transformed from latitude/longitude coordinates to web mercator where
   * top-left corner of the planet is (0,0) and bottom-right is (1,1).
   */
  public static Envelope toWorldBounds(Envelope lonLatBounds) {
    return new Envelope(
      getWorldX(lonLatBounds.getMinX()),
      getWorldX(lonLatBounds.getMaxX()),
      getWorldY(lonLatBounds.getMinY()),
      getWorldY(lonLatBounds.getMaxY())
    );
  }

  public static Geometry tileToLatLonCoords(Geometry geom, int tileX, int tileY, int zoom) {
    TileToLatLonTransformer transformer = new TileToLatLonTransformer(tileX, tileY, zoom);
    return transformer.transform(geom);
  }


  /**
   * Returns the longitude for a web mercator coordinate {@code x} where 0 is the international date line on the west
   * side, 1 is the international date line on the east side, and 0.5 is the prime meridian.
   */
  public static double getWorldLon(double x) {
    return x * 360 - 180;
  }

  /**
   * Returns the latitude for a web mercator {@code y} coordinate where 0 is the north edge of the map, 0.5 is the
   * equator, and 1 is the south edge of the map.
   */
  public static double getWorldLat(double y) {
    double n = Math.PI - 2 * Math.PI * y;
    return DEGREES_PER_RADIAN * Math.atan(0.5 * (Math.exp(n) - Math.exp(-n)));
  }

  /**
   * Returns the web mercator X coordinate for {@code longitude} where 0 is the international date line on the west
   * side, 1 is the international date line on the east side, and 0.5 is the prime meridian.
   */
  public static double getWorldX(double longitude) {
    return (longitude + 180) / 360;
  }

  /**
   * Returns the web mercator Y coordinate for {@code latitude} where 0 is the north edge of the map, 0.5 is the
   * equator, and 1 is the south edge of the map.
   */
  public static double getWorldY(double latitude) {
    if (latitude <= MIN_LAT) {
      return 1.1;
    }
    if (latitude >= MAX_LAT) {
      return -0.1;
    }
    double sin = Math.sin(latitude * RADIANS_PER_DEGREE);
    return 0.5 - 0.25 * Math.log((1 + sin) / (1 - sin)) / Math.PI;
  }

  /**
   * Returns a latitude/longitude coordinate encoded into a single 64-bit long for storage in a {@link LongLongMap} that
   * can be decoded using {@link #decodeWorldX(long)} and {@link #decodeWorldY(long)}.
   */
  public static long encodeFlatLocation(double lon, double lat) {
    double worldX = getWorldX(lon) + 1;
    double worldY = getWorldY(lat) + 1;
    long x = (long) (worldX * HALF_QUANTIZED_WORLD_SIZE);
    long y = (long) (worldY * HALF_QUANTIZED_WORLD_SIZE);
    return (x << 32) | (y & LOWER_32_BIT_MASK);
  }

  /**
   * Returns the web mercator Y coordinate of the latitude/longitude encoded with
   * {@link #encodeFlatLocation(double, double)}.
   */
  public static double decodeWorldY(long encoded) {
    return ((encoded & LOWER_32_BIT_MASK) / HALF_QUANTIZED_WORLD_SIZE) - 1;
  }

  /**
   * Returns the web mercator X coordinate of the latitude/longitude encoded with
   * {@link #encodeFlatLocation(double, double)}.
   */
  public static double decodeWorldX(long encoded) {
    return ((encoded >>> 32) / HALF_QUANTIZED_WORLD_SIZE) - 1;
  }

  /**
   * Returns an approximate zoom level that a map should be displayed at to show all of {@code envelope}, specified in
   * latitude/longitude coordinates.
   */
  public static double getZoomFromLonLatBounds(Envelope envelope) {
    Envelope worldBounds = GeoUtils.toWorldBounds(envelope);
    return getZoomFromWorldBounds(worldBounds);
  }

  /**
   * Returns an approximate zoom level that a map should be displayed at to show all of {@code envelope}, specified in
   * web mercator coordinates.
   */
  public static double getZoomFromWorldBounds(Envelope worldBounds) {
    double maxEdge = Math.max(worldBounds.getWidth(), worldBounds.getHeight());
    return Math.max(0, -Math.log(maxEdge) / Math.log(2));
  }

  /** Returns the width in meters of a single pixel of a 256x256 px tile at the given {@code zoom} level. */
  public static double metersPerPixelAtEquator(int zoom) {
    return WORLD_CIRCUMFERENCE_METERS / Math.pow(2d, zoom + 8d);
  }

  /**
   * 计算给定网格分辨率下每像素的地面距离
   * @param zoom 缩放级别
   * @param tileResolution 单瓦片的像素分辨率（如 256, 512, 1024）
   * @return 每像素的地面距离（米）
   */
  public static double metersPerPixelAtEquator(int zoom, int tileResolution) {
    return WORLD_CIRCUMFERENCE_METERS / (Math.pow(2d, zoom) * tileResolution);
  }

  /**
   * 计算给定网格分辨率下每像素的地面距离
   * @param zoom 缩放级别
   * @param tileResolution 单瓦片的像素分辨率（如 256, 512, 1024）
   * @return 每像素的地面距离（米）
   */
  public static double areaPerPixelAtEquator(int zoom, int tileResolution) {
    double meters = metersPerPixelAtEquator(zoom, tileResolution);
    return meters * meters;
  }

  /**
   * 计算给定网格分辨率和纬度下每像素的地面距离
   * @param zoom 缩放级别
   * @param tileResolution 单瓦片的像素分辨率（如 256, 512, 1024）
   * @param latitude 纬度（角度）
   * @return 每像素的地面面积（米^2）
   */
  public static double areaPerPixelAtLatitude(int zoom, int tileResolution, double latitude) {
    // 使用地球的椭球体模型来考虑纬度影响
    double metersPerPixelAtEquator = metersPerPixelAtEquator(zoom, tileResolution);

    // 计算纬度上的调整因子
    double latitudeAdjustment = Math.cos(Math.toRadians(latitude));

    // 调整后的每像素的地面距离
    double adjustedMetersPerPixel = metersPerPixelAtEquator * latitudeAdjustment;

    // 返回每像素的地面面积
    return adjustedMetersPerPixel * adjustedMetersPerPixel;
  }

  /**
   * 转换 Geometry 的经纬度顺序，将 (x, y) 转换为 (y, x)
   */
  private static final GeometryTransformer SWITCH_LAT_LON_ORDER = new GeometryTransformer() {
    @Override
    protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
      // 使用相同的坐标序列类型，创建新的序列
      CoordinateSequence copy = new PackedCoordinateSequence.Double(coords.size(), 2, 0);
      for (int i = 0; i < coords.size(); i++) {
        copy.setOrdinate(i, 0, coords.getOrdinate(i, 1));
        copy.setOrdinate(i, 1, coords.getOrdinate(i, 0));
      }
      return copy;
    }
  };

  /**
   * 切换经纬度顺序
   *
   * @param geometry 输入的 Geometry 对象
   * @return 返回经纬度顺序切换后的 Geometry 对象
   */
  public static Geometry switchLatLonOrder(Geometry geometry) {
    return SWITCH_LAT_LON_ORDER.transform(geometry);
  }

  public static double calculateRealAreaLatLonImproved(Geometry geometry) {
    try {
      geometry = switchLatLonOrder(geometry);
      MathTransform transform = CRS.findMathTransform(CRS.decode("EPSG:4326"), CRS.decode("EPSG:3857"), true);
      Geometry projected = JTS.transform(geometry, transform);
      return projected.getArea();
    } catch (Exception e) {
      throw new RuntimeException("Error calculating real area of geometry", e);
    }
  }

  public static double getGridGeographicArea(int x, int y, int z, int extend, int width) {
    Envelope bounds = getTileGeographicBounds(x, y, z);
    double tileArea = calculateTileArea(bounds);
    double pixelArea = calculatePixelArea(tileArea, extend);
    return pixelArea * width * width;
  }


  /**
   * 获取瓦片的地理边界 (经纬度范围)。
   *
   * @param x 瓦片的列号
   * @param y 瓦片的行号
   * @param z 缩放级别
   * @return 包含地理边界的 Envelope (minLon, maxLon, minLat, maxLat)
   */
  public static Envelope getTileGeographicBounds(int x, int y, int z) {
    double minLon = tileToLongitude(x, z);
    double maxLon = tileToLongitude(x + 1, z);
    double minLat = tileToLatitude(y + 1, z);
    double maxLat = tileToLatitude(y, z);
    return new Envelope(minLon, maxLon, minLat, maxLat);
  }

  /**
   * 计算瓦片的经度 (longitude)。
   */
  private static double tileToLongitude(int x, int z) {
    return x / Math.pow(2.0, z) * 360.0 - 180;
  }

  /**
   * 计算瓦片的纬度 (latitude)。
   */
  private static double tileToLatitude(int y, int z) {
    double n = Math.PI - (2.0 * Math.PI * y) / Math.pow(2.0, z);
    return Math.toDegrees(Math.atan(Math.sinh(n)));
  }

  /**
   * 计算瓦片的真实面积 (平方米)。
   *
   * @param bounds 瓦片的地理边界 (Envelope)
   * @return 瓦片的真实面积 (平方米)
   */
  public static double calculateTileArea(Envelope bounds) {
    // 获取瓦片的四个角点
    double minLon = bounds.getMinX();
    double maxLon = bounds.getMaxX();
    double minLat = bounds.getMinY();
    double maxLat = bounds.getMaxY();

    // 将纬度转换为弧度
    double minLatRad = Math.toRadians(minLat);
    double maxLatRad = Math.toRadians(maxLat);

    // 计算瓦片宽度 (经度跨度)
    double width = Math.toRadians(maxLon - minLon) * WORLD_RADIUS_METERS;

    // 计算瓦片高度 (纬度跨度，考虑球面投影)
    double height = WORLD_RADIUS_METERS * (Math.sin(maxLatRad) - Math.sin(minLatRad));

    // 返回瓦片的面积
    return Math.abs(width * height);
  }

  /**
   * TODO 计算瓦片中每像素的面积 (平方米)。 当前计算方式可能存在一定偏差？？？
   *
   * @param tileArea 瓦片的总面积
   * @return 每像素的面积 (平方米)
   */
  public static double calculatePixelArea(double tileArea, Integer extend) {
    return tileArea / (extend * extend);
  }

  /**
   * 计算 EPSG:4326 几何的真实地理面积（单位：平方米）
   *
   * @param geometry EPSG:4326 几何对象（支持 Polygon 或 MultiPolygon）
   * @return 真实地理面积（单位：平方米）
   */
  public static double calculateRealAreaLatLon(Geometry geometry) {
    if (geometry instanceof Polygon polygon) {
      return calculatePolygonArea(polygon);
    } else if (geometry.getGeometryType().equals("MultiPolygon")) {
      double totalArea = 0.0;
      for (int i = 0; i < geometry.getNumGeometries(); i++) {
        totalArea += calculatePolygonArea((Polygon) geometry.getGeometryN(i));
      }
      return totalArea;
    } else {
      return 0.0;
    }
  }

  /**
   * 计算单个多边形的球面面积
   *
   * @param polygon EPSG:4326 的多边形
   * @return 球面面积（平方米）
   */
  private static double calculatePolygonArea(Polygon polygon) {
    Coordinate[] coords = polygon.getExteriorRing().getCoordinates();
    double area = 0.0;

    for (int i = 0; i < coords.length - 1; i++) {
      Coordinate p1 = coords[i];
      Coordinate p2 = coords[i + 1];

      double lon1 = Math.toRadians(p1.x);
      double lat1 = Math.toRadians(p1.y);
      double lon2 = Math.toRadians(p2.x);
      double lat2 = Math.toRadians(p2.y);

      area += (lon2 - lon1) * (2 + Math.sin(lat1) + Math.sin(lat2));
    }

    // 转换为实际面积
    area = Math.abs(area) * 6371000 * 6371000 / 2.0;
    return area;
  }


  /** Returns the length in pixels for a given number of meters on a 256x256 px tile at the given {@code zoom} level. */
  public static double metersToPixelAtEquator(int zoom, double meters) {
    return meters / metersPerPixelAtEquator(zoom);
  }

  public static Point point(double x, double y) {
    return JTS_FACTORY.createPoint(new CoordinateXY(x, y));
  }

  public static Point point(Coordinate coord) {
    return JTS_FACTORY.createPoint(coord);
  }

  public static MultiLineString createMultiLineString(List<LineString> lineStrings) {
    return JTS_FACTORY.createMultiLineString(lineStrings.toArray(EMPTY_LINE_STRING_ARRAY));
  }

  public static MultiPolygon createMultiPolygon(List<Polygon> polygon) {
    return JTS_FACTORY.createMultiPolygon(polygon.toArray(EMPTY_POLYGON_ARRAY));
  }

  /**
   * Attempt to fix any self-intersections or overlaps in {@code geom}.
   *
   * @throws GeometryException if a robustness error occurred
   */
  public static Geometry fixPolygon(Geometry geom) throws GeometryException {
    try {
      return geom.buffer(0);
    } catch (TopologyException e) {
      throw new GeometryException("fix_polygon_topology_error", "robustness error fixing polygon: " + e);
    }
  }

  /**
   * More aggressive fix for self-intersections than {@link #fixPolygon(Geometry)} that expands then contracts the shape
   * by {@code buffer}.
   *
   * @throws GeometryException if a robustness error occurred
   */
  public static Geometry fixPolygon(Geometry geom, double buffer) throws GeometryException {
    try {
      return geom.buffer(buffer).buffer(-buffer);
    } catch (TopologyException e) {
      throw new GeometryException("fix_polygon_buffer_topology_error", "robustness error fixing polygon: " + e);
    }
  }

  public static Geometry combineLineStrings(List<LineString> lineStrings) {
    return lineStrings.size() == 1 ? lineStrings.getFirst() : createMultiLineString(lineStrings);
  }

  public static Geometry combinePolygons(List<Polygon> polys) {
    return polys.size() == 1 ? polys.getFirst() : createMultiPolygon(polys);
  }

  public static Geometry combinePoints(List<Point> points) {
    return points.size() == 1 ? points.getFirst() : createMultiPoint(points);
  }

  /**
   * Returns a copy of {@code geom} with coordinates rounded to {@link #TILE_PRECISION} and fixes any polygon
   * self-intersections or overlaps that may have caused.
   */
  public static Geometry snapAndFixPolygon(Geometry geom, Stats stats, String stage) throws GeometryException {
    return snapAndFixPolygon(geom, TILE_PRECISION, stats, stage);
  }

  /**
   * todo linespace
   *
   * Returns a copy of {@code geom} with coordinates rounded to {@link #TILE_PRECISION } and fixes any polygon
   * self-intersections or overlaps that may have caused.
   */
  public static Geometry snapAndFixPolygon(Geometry geom, Stats stats, String stage, PrecisionModel precisionModel)
    throws GeometryException {
    return snapAndFixPolygon(geom, precisionModel, stats, stage);
  }


  /**
   * Returns a copy of {@code geom} with coordinates rounded to {@code #tilePrecision} and fixes any polygon
   * self-intersections or overlaps that may have caused.
   *
   * @throws GeometryException if an unrecoverable robustness exception prevents us from fixing the geometry
   */
  public static Geometry snapAndFixPolygon(Geometry geom, PrecisionModel tilePrecision, Stats stats, String stage)
    throws GeometryException {
    try {
      if (!geom.isValid()) {
        geom = fixPolygon(geom);
        stats.dataError(stage + "_snap_fix_input");
      }
      return GeometryPrecisionReducer.reduceKeepCollapsed(geom, tilePrecision);
    } catch (TopologyException | IllegalArgumentException e) {
      // precision reduction fails if geometry is invalid, so attempt
      // to fix it then try again
      geom = GeometryFixer.fix(geom);
      stats.dataError(stage + "_snap_fix_input2");
      try {
        return GeometryPrecisionReducer.reduceKeepCollapsed(geom, tilePrecision);
      } catch (TopologyException | IllegalArgumentException e2) {
        // give it one last try but with more aggressive fixing, just in case (see issue #511)
        geom = fixPolygon(geom, tilePrecision.gridSize() / 2);
        stats.dataError(stage + "_snap_fix_input3");
        try {
          return GeometryPrecisionReducer.reduceKeepCollapsed(geom, tilePrecision);
        } catch (TopologyException | IllegalArgumentException e3) {
          stats.dataError(stage + "_snap_fix_input3_failed");
          throw new GeometryException("snap_third_time_failed", "Error reducing precision");
        }
      }
    }
  }


  /**
   * 这个方法处理 X 坐标的环绕（wrap-around）。地理坐标的 X 坐标（经度）在全球范围内可能会超过可显示的范围
   * （例如，超过180度或小于-180度），所以需要用 wrapDouble 来处理这种情况。它将 X 坐标归一化到 [0, max) 范围内。
   * @param value
   * @param max
   * @return
   */
  private static double wrapDouble(double value, double max) {
    value %= max;
    if (value < 0) {
      value += max;
    }
    return value;
  }


  private static long longPair(int a, int b) {
    return (((long) a) << 32L) | (b & LOWER_32_BIT_MASK);
  }

  /**
   * Breaks the world up into a grid and returns an ID for the square that {@code coord} falls into.
   *
   * @param tilesAtZoom       the tile width of the world at this zoom level
   * @param labelGridTileSize the tile width of each grid square
   * @param coord             the coordinate, scaled to this zoom level
   * @return an ID representing the grid square that {@code coord} falls into.
   */
  public static long labelGridId(int tilesAtZoom, double labelGridTileSize, Coordinate coord) {
    return GeoUtils.longPair(
      (int) Math.floor(wrapDouble(coord.getX(), tilesAtZoom) / labelGridTileSize),
      (int) Math.floor((coord.getY()) / labelGridTileSize)
    );
  }

  /** Returns a {@link CoordinateSequence} from a list of {@code x, y, x, y, ...} coordinates. */
  public static CoordinateSequence coordinateSequence(double... coords) {
    return new PackedCoordinateSequence.Double(coords, 2, 0);
  }

  public static MultiPoint createMultiPoint(List<Point> points) {
    return JTS_FACTORY.createMultiPoint(points.toArray(EMPTY_POINT_ARRAY));
  }

  /**
   * Returns line strings for every inner and outer ring contained in a polygon.
   *
   * @throws GeometryException if {@code geom} contains anything other than polygons or line strings
   */
  public static Geometry polygonToLineString(Geometry geom) throws GeometryException {
    List<LineString> lineStrings = new ArrayList<>();
    getLineStrings(geom, lineStrings);
    if (lineStrings.isEmpty()) {
      throw new GeometryException("polygon_to_linestring_empty", "No line strings");
    } else if (lineStrings.size() == 1) {
      return lineStrings.getFirst();
    } else {
      return createMultiLineString(lineStrings);
    }
  }

  private static void getLineStrings(Geometry input, List<LineString> output) throws GeometryException {
    switch (input) {
      case LinearRing linearRing -> output.add(JTS_FACTORY.createLineString(linearRing.getCoordinateSequence()));
      case LineString lineString -> output.add(lineString);
      case Polygon polygon -> {
        getLineStrings(polygon.getExteriorRing(), output);
        for (int i = 0; i < polygon.getNumInteriorRing(); i++) {
          getLineStrings(polygon.getInteriorRingN(i), output);
        }
      }
      case GeometryCollection gc -> {
        for (int i = 0; i < gc.getNumGeometries(); i++) {
          getLineStrings(gc.getGeometryN(i), output);
        }
      }
      case null, default -> throw new GeometryException("get_line_strings_bad_type",
        "unrecognized geometry type: " + (input == null ? "null" : input.getGeometryType()));
    }
  }

  public static Geometry createGeometryCollection(List<Geometry> polygonGroup) {
    return JTS_FACTORY.createGeometryCollection(polygonGroup.toArray(Geometry[]::new));
  }

  /** Returns a point approximately {@code ratio} of the way from start to end and {@code offset} units to the right. */
  public static Point pointAlongOffset(LineString lineString, double ratio, double offset) {
    int numPoints = lineString.getNumPoints();
    int middle = Math.clamp((int) (numPoints * ratio), 0, numPoints - 2);
    Coordinate a = lineString.getCoordinateN(middle);
    Coordinate b = lineString.getCoordinateN(middle + 1);
    LineSegment segment = new LineSegment(a, b);
    return JTS_FACTORY.createPoint(segment.pointAlongOffset(0.5, offset));
  }

  public static Polygon createPolygon(LinearRing exteriorRing, List<LinearRing> rings) {
    return JTS_FACTORY.createPolygon(exteriorRing, rings.toArray(LinearRing[]::new));
  }

  public static Polygon createPolygon(Coordinate[] coordinates) {
    return JTS_FACTORY.createPolygon(createLinearRing(coordinates), new LinearRing[0]);
  }

  public static Polygon createPolygon(Coordinate[] coordinates, List<LinearRing> rings) {
    return JTS_FACTORY.createPolygon(createLinearRing(coordinates), rings.toArray(LinearRing[]::new));
  }

  public static LinearRing createLinearRing(Coordinate[] coordinates) {
    return JTS_FACTORY.createLinearRing(coordinates);
  }

  /**
   * Returns {@code true} if the signed area of the triangle formed by 3 sequential points changes sign anywhere along
   * {@code ring}, ignoring repeated and collinear points.
   */
  public static boolean isConvex(LinearRing ring) {
    double threshold = 1e-3;
    double minPointsToCheck = 10;
    CoordinateSequence seq = ring.getCoordinateSequence();
    int size = seq.size();
    if (size <= 3) {
      return false;
    }

    // ignore leading repeated points
    double c0x = seq.getX(0);
    double c0y = seq.getY(0);
    double c1x = Double.NaN, c1y = Double.NaN;
    int i;
    for (i = 1; i < size; i++) {
      c1x = seq.getX(i);
      c1y = seq.getY(i);
      if (c1x != c0x || c1y != c0y) {
        break;
      }
    }

    double dx1 = c1x - c0x;
    double dy1 = c1y - c0y;

    double negZ = 1e-20, posZ = 1e-20;

    // need to wrap around to make sure the triangle formed by last and first points does not change sign
    for (; i <= size + 1; i++) {
      // first and last point should be the same, so skip index 0
      int idx = i < size ? i : (i + 1 - size);
      double c2x = seq.getX(idx);
      double c2y = seq.getY(idx);

      double dx2 = c2x - c1x;
      double dy2 = c2y - c1y;
      double z = dx1 * dy2 - dy1 * dx2;

      double absZ = Math.abs(z);

      // look for sign changes in the triangles formed by sequential points
      // but, we want to allow for rounding errors and small concavities relative to the overall shape
      // so track the largest positive and negative threshold for triangle area and compare them once we
      // have enough points
      boolean extendedBounds = false;
      if (z < 0 && absZ > negZ) {
        negZ = absZ;
        extendedBounds = true;
      } else if (z > 0 && absZ > posZ) {
        posZ = absZ;
        extendedBounds = true;
      }

      if (i == minPointsToCheck || (i > minPointsToCheck && extendedBounds)) {
        double ratio = negZ < posZ ? negZ / posZ : posZ / negZ;
        if (ratio > threshold) {
          return false;
        }
      }

      c1x = c2x;
      c1y = c2y;
      dx1 = dx2;
      dy1 = dy2;
    }
    return (negZ < posZ ? negZ / posZ : posZ / negZ) < threshold;
  }

  /**
   * If {@code geometry} is a {@link MultiPolygon}, returns a copy with polygons sorted by descending area of the outer
   * shell, otherwise returns the input geometry.
   */
  public static Geometry sortPolygonsByAreaDescending(Geometry geometry) {
    if (geometry instanceof MultiPolygon multiPolygon) {
      PolyAndArea[] areas = new PolyAndArea[multiPolygon.getNumGeometries()];
      for (int i = 0; i < multiPolygon.getNumGeometries(); i++) {
        areas[i] = new PolyAndArea((Polygon) multiPolygon.getGeometryN(i));
      }
      return JTS_FACTORY.createMultiPolygon(
        Stream.of(areas).sorted().map(d -> d.poly).toArray(Polygon[]::new)
      );
    } else {
      return geometry;
    }
  }

  /** Combines multiple geometries into one {@link GeometryCollection}. */
  public static Geometry combine(Geometry... geometries) {
    List<Geometry> innerGeometries = new ArrayList<>();
    // attempt to flatten out nested geometry collections
    for (var geom : geometries) {
      if (geom instanceof GeometryCollection collection) {
        for (int i = 0; i < collection.getNumGeometries(); i++) {
          innerGeometries.add(collection.getGeometryN(i));
        }
      } else {
        innerGeometries.add(geom);
      }
    }
    return innerGeometries.size() == 1 ? innerGeometries.getFirst() :
      JTS_FACTORY.createGeometryCollection(innerGeometries.toArray(Geometry[]::new));
  }

  /**
   * For a feature of size {@code worldGeometrySize} (where 1=full planet), determine the minimum zoom level at which
   * the feature appears at least {@code minPixelSize} pixels large.
   * <p>
   * The result will be clamped to the range [0, {@link PlanetilerConfig#MAX_MAXZOOM}].
   */
  public static int minZoomForPixelSize(double worldGeometrySize, double minPixelSize) {
    double worldPixels = worldGeometrySize * 256;
    return Math.clamp((int) Math.ceil(Math.log(minPixelSize / worldPixels) / LOG2), 0,
      PlanetilerConfig.MAX_MAXZOOM);
  }

  public static WKBReader wkbReader() {
    return new WKBReader(JTS_FACTORY);
  }

  public static WKTReader wktReader() {
    return new WKTReader(JTS_FACTORY);
  }

  public static Geometry createSmallSquareWithCentroid(Coordinate coordinate, double precisionScale, int zoom) {
    // 当前缩放级别下的每个像素所代表的相对单位长度
    double tileUnit = 1.0 / Math.pow(2, zoom);
    // 计算最小分辨单位
    double minResolvableUnit = 1.0 / Math.max(256d, precisionScale) * tileUnit;
    // 计算正方形的边长，确保不会被简化
    double halfSideLength = minResolvableUnit / 2.0;

    // 构造正方形的四个顶点
    Coordinate[] coordinates = new Coordinate[5];
    coordinates[0] = new Coordinate(coordinate.x - halfSideLength, coordinate.y - halfSideLength);
    coordinates[1] = new Coordinate(coordinate.x + halfSideLength, coordinate.y - halfSideLength);
    coordinates[2] = new Coordinate(coordinate.x + halfSideLength, coordinate.y + halfSideLength);
    coordinates[3] = new Coordinate(coordinate.x - halfSideLength, coordinate.y + halfSideLength);
    coordinates[4] = coordinates[0];

    GeometryFactory geometryFactory = new GeometryFactory();
    return geometryFactory.createPolygon(coordinates);
  }

  /** Helper class to sort polygons by area of their outer shell. */
  private record PolyAndArea(Polygon poly, double area) implements Comparable<PolyAndArea> {

    PolyAndArea(Polygon poly) {
      this(poly, Area.ofRing(poly.getExteriorRing().getCoordinateSequence()));
    }

    @Override
    public int compareTo(PolyAndArea o) {
      return -Double.compare(area, o.area);
    }
  }
}
