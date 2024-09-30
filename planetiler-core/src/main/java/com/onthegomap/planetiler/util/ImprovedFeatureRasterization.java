package com.onthegomap.planetiler.util;

import com.onthegomap.planetiler.geo.TileCoord;
import java.util.ArrayList;
import java.util.List;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.CoordinateSequence;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;
import org.locationtech.jts.geom.Polygon;
import org.locationtech.jts.geom.impl.PackedCoordinateSequence;
import org.locationtech.jts.geom.util.GeometryTransformer;
import org.locationtech.jts.index.strtree.STRtree;

public class ImprovedFeatureRasterization {

  private static final GeometryFactory geometryFactory = new GeometryFactory();
  private static final int BATCH_SIZE = 1000; // 可以根据实际情况调整

  public static Geometry rasterizeGeometry(Geometry geom, TileCoord tileCoord, double pixelSize, double threshold) {
    List<Geometry> rasterizedGeometries = new ArrayList<>();
    Envelope envelope = geom.getEnvelopeInternal();

    STRtree index = new STRtree();
    index.insert(geom.getEnvelopeInternal(), geom);

    // 对齐起点到像素网格
//    double startX = Math.floor(envelope.getMinX() / pixelSize) * pixelSize;
//    double startY = Math.floor(envelope.getMinY() / pixelSize) * pixelSize;

    // 批量创建像素
    List<Polygon> pixels = new ArrayList<>();
    for (double x = envelope.getMinX(); x < envelope.getMaxX(); x += pixelSize) {
      for (double y = envelope.getMinY(); y < envelope.getMaxY(); y += pixelSize) {
        Polygon pixel = createPixelPolygon(x, y, pixelSize);
        pixels.add(pixel);
      }
    }

    // 批量处理像素
    for (Polygon pixel : pixels) {
      List<Geometry> candidates = index.query(pixel.getEnvelopeInternal());
      for (Geometry candidate : candidates) {
        if (candidate.intersects(pixel)) {
          Geometry intersection = candidate.intersection(pixel);
          double coverage = intersection.getArea() / pixel.getArea();
//          rasterizedGeometries.add(createSmallShape(intersection, pixelSize, coverage));
          rasterizedGeometries.add(pixel);
//          if (coverage >= threshold) {
//            rasterizedGeometries.add(pixel);
//          } else if (coverage > 0) {
//            rasterizedGeometries.add(createSmallShape(intersection, pixelSize, coverage));
//          }
          break; // 假设每个像素只需要处理一次
        }
      }
    }

    Geometry geometry = combineGeometries(rasterizedGeometries, geom.getFactory());
    return convertLocalToWebMercator(geometry, tileCoord);

  }

  private static Polygon createPixelPolygon(double x, double y, double pixelSize) {
    Coordinate[] coords = new Coordinate[5];
    coords[0] = new Coordinate(x, y);
    coords[1] = new Coordinate(x + pixelSize, y);
    coords[2] = new Coordinate(x + pixelSize, y + pixelSize);
    coords[3] = new Coordinate(x, y + pixelSize);
    coords[4] = coords[0];
    return geometryFactory.createPolygon(coords);
  }

  private static Geometry createSmallShape(Geometry geom, double pixelSize, double coverage) {
    // 根据覆盖率决定使用正方形还是三角形
    if (coverage > 0.5) {
      return createSmallSquare(geom.getCentroid(), pixelSize * coverage);
    } else {
      return createSmallTriangle(geom.getCentroid(), pixelSize * coverage);
    }
  }

  private static Geometry createSmallSquare(Point center, double size) {
    double halfSize = size / 2;
    Coordinate[] coords = new Coordinate[5];
    coords[0] = new Coordinate(center.getX() - halfSize, center.getY() - halfSize);
    coords[1] = new Coordinate(center.getX() + halfSize, center.getY() - halfSize);
    coords[2] = new Coordinate(center.getX() + halfSize, center.getY() + halfSize);
    coords[3] = new Coordinate(center.getX() - halfSize, center.getY() + halfSize);
    coords[4] = coords[0];
    return geometryFactory.createPolygon(coords);
  }

  private static Geometry createSmallTriangle(Point center, double size) {
    double height = size * Math.sqrt(3) / 2;
    Coordinate[] coords = new Coordinate[4];
    coords[0] = new Coordinate(center.getX(), center.getY() + height / 3);
    coords[1] = new Coordinate(center.getX() - size / 2, center.getY() - height * 2 / 3);
    coords[2] = new Coordinate(center.getX() + size / 2, center.getY() - height * 2 / 3);
    coords[3] = coords[0];
    return geometryFactory.createPolygon(coords);
  }

  // 将局部坐标转换为 Web Mercator 坐标
  private static Geometry convertLocalToWebMercator(Geometry geom, TileCoord tileCoord) {
    GeometryTransformer transformer = new GeometryTransformer() {
      @Override
      protected CoordinateSequence transformCoordinates(CoordinateSequence coords, Geometry parent) {
        CoordinateSequence transformedCoords = new PackedCoordinateSequence.Double(coords.size(), 2, 0);

        double tileSize = 256.0;
        double worldWidthAtZoom = Math.pow(2, tileCoord.z());
        double scaleFactor = 1.0 / worldWidthAtZoom;

        for (int i = 0; i < coords.size(); i++) {
          double worldX = (tileCoord.x() + coords.getX(i) / tileSize) * scaleFactor;
          double worldY = (tileCoord.y() + coords.getY(i) / tileSize) * scaleFactor;

          transformedCoords.setOrdinate(i, 0, worldX);
          transformedCoords.setOrdinate(i, 1, worldY);
        }
        return transformedCoords;
      }
    };

    return transformer.transform(geom);
  }

  public static Geometry combineGeometries(List<Geometry> geometries, GeometryFactory factory) {
    if (geometries.size() <= BATCH_SIZE) {
      return factory.createGeometryCollection(geometries.toArray(new Geometry[0])).union();
    }

    List<Geometry> combinedGeometries = new ArrayList<>();
    List<Geometry> batch = new ArrayList<>(BATCH_SIZE);

    for (Geometry geom : geometries) {
      batch.add(geom);
      if (batch.size() == BATCH_SIZE) {
        combinedGeometries.add(factory.createGeometryCollection(batch.toArray(new Geometry[0])).union());
        batch.clear();
      }
    }

    if (!batch.isEmpty()) {
      combinedGeometries.add(factory.createGeometryCollection(batch.toArray(new Geometry[0])).union());
    }

    return combineGeometries(combinedGeometries, factory);
  }
}
