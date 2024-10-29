package com.onthegomap.planetiler.render;

import com.onthegomap.planetiler.geo.MutableCoordinateSequence;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

/**
 * 全局通用网格化对象，提升性能
 */
public class GridEntity {

  private static final int EXTENT = 4096;

  // 创建GeometryFactory实例
  private static final GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

  private static final Map<Integer, GridEntity> gridEntityMap = new ConcurrentHashMap<>();

  private final int gridSize;

  private final int gridWidth;

  private Geometry[][] vectorGrid = new Geometry[EXTENT][EXTENT];

  public static GridEntity getGridEntity(int gridSize) {
    if (gridEntityMap.containsKey(gridSize)) {
      return gridEntityMap.get(gridSize);
    }
    GridEntity gridEntity = new GridEntity(gridSize);
    gridEntityMap.putIfAbsent(gridSize, gridEntity);
    return gridEntity;
  }

  public int getGridWidth() {
    return gridWidth;
  }

  public Geometry[][] getVectorGrid() {
    return vectorGrid;
  }

  public GridEntity(int gridSize) {
    // 网格256
    this.gridSize = gridSize;
    // 一个网格宽度
    this.gridWidth = EXTENT / gridSize;
    for (int i = 0; i < EXTENT; i += gridWidth) {
      vectorGrid[i] = new Geometry[EXTENT];
      for (int j = 0; j < EXTENT; j += gridWidth) {
        if (gridWidth == 1) {
          Coordinate coord = new Coordinate(i, j);
          Point point = geometryFactory.createPoint(coord);
          vectorGrid[i][j] = point;
        } else {
          // 创建地理点
          MutableCoordinateSequence sequence = new MutableCoordinateSequence();
          int left = i;
          int up = j;
          // 避免溢出，
          int right = Math.min((i + 1), EXTENT);
          int down = Math.min((j + 1), EXTENT);
          sequence.addPoint(left, up);
          sequence.addPoint(left, down);
          sequence.addPoint(right, down);
          sequence.addPoint(right, up);
          sequence.addPoint(left, up);
          if (left == right && up == down) {
            Coordinate coord = new Coordinate(left, up);
            Point point = geometryFactory.createPoint(coord);
            vectorGrid[i][j] = point;
          } else {
            vectorGrid[i][j] = geometryFactory.createPolygon(sequence);
          }
        }
      }
    }
  }
}
