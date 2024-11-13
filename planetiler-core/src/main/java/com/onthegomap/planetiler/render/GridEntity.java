package com.onthegomap.planetiler.render;

import com.onthegomap.planetiler.geo.MutableCoordinateSequence;
import org.geotools.geometry.jts.JTSFactoryFinder;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

/**
 * 全局通用网格化对象，提升性能
 */
public class GridEntity {

  private static final int EXTENT = 4096;

  // 创建GeometryFactory实例
  private static final GeometryFactory geometryFactory = JTSFactoryFinder.getGeometryFactory();

  private int gridSize;

  private int gridWidth;

  private Geometry[][] vectorGrid;

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
    this.vectorGrid = generateVectorGrid();
  }

  private Geometry[][] generateVectorGrid() {
    Geometry[][] grid = new Geometry[EXTENT][EXTENT];
    for (int i = 0; i < EXTENT; i += gridWidth) {
      for (int j = 0; j < EXTENT; j += gridWidth) {
        if (gridWidth == 1) {
          grid[i][j] = geometryFactory.createPoint(new Coordinate(i, j));
        } else {
          // 创建地理点
          MutableCoordinateSequence sequence = new MutableCoordinateSequence();
          int left = i;
          int up = j;
          // 避免溢出，
          int right = Math.min((i + gridWidth), EXTENT);
          int down = Math.min((j + gridWidth), EXTENT);
          sequence.addPoint(left, up);
          sequence.addPoint(left, down);
          sequence.addPoint(right, down);
          sequence.addPoint(right, up);
          sequence.addPoint(left, up);
          if (left == right && up == down) {
            grid[i][j] = geometryFactory.createPoint(new Coordinate(left, up));
          } else {
            grid[i][j] = geometryFactory.createPolygon(sequence);
          }
        }
      }
    }

    return grid;
  }
}
