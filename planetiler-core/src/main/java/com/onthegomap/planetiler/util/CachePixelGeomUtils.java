package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.geo.GeoUtils;
import java.util.Collections;
import java.util.concurrent.ConcurrentHashMap;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Polygon;

/**
 * @description:
 * @author: xmm
 * @date: 2024/10/9 11:14
 */
public class CachePixelGeomUtils {

  private static final int SCALE_FACTOR = 10000; // 4位小数精度

  private static final ConcurrentHashMap<String, Polygon> cachePixelGeom = new ConcurrentHashMap<>();

  static {
    //    initCachePixelGeom(-4, 260, -4, 260, 1d);
    initCachePixelGeom(-8, 520, -8, 520, 0.5d);
//    initCachePixelGeom(-16, 1040, -16, 1040, 0.25d);
//    initCachePixelGeom(-32, 2080, -32, 2080, 0.125d);
  }

  /**
   * 初始化网格集，提高效率
   */
  public static void initCachePixelGeom(int startRow, int endRow, int startCol, int endCol, double pixelSize) {
    for (int x = startCol; x < endCol; x++) {
      for (int y = startRow; y < endRow; y++) {
        double minX = x * pixelSize;
        double minY = y * pixelSize;
        double maxX = minX + pixelSize;
        double maxY = minY + pixelSize;

        // 创建栅格单元的多边形
        Polygon cell = GeoUtils.createPolygon(new Coordinate[]{
          new Coordinate(minX, minY),
          new Coordinate(minX, maxY),
          new Coordinate(maxX, maxY),
          new Coordinate(maxX, minY),
          new Coordinate(minX, minY)
        }, Collections.emptyList());

        cachePixelGeom.put(generateKey(x, y, pixelSize), cell);
      }
    }
  }

  public static Polygon getGeom(double x, double y, double pixelSize) {
    return cachePixelGeom.get(generateKey(x, y, pixelSize));
  }

  /**
   * 防止double类型转换精度丢失
   *
   * @param x
   * @param y
   * @return
   */
  public static String generateKey(double x, double y, double pixelSize) {
    int scaledX = (int) Math.round(x * SCALE_FACTOR);
    int scaledY = (int) Math.round(y * SCALE_FACTOR);
    int scaledSize = (int) Math.round(pixelSize * SCALE_FACTOR);

    return scaledX + ":" + scaledY + ":" + scaledSize;
  }

}
