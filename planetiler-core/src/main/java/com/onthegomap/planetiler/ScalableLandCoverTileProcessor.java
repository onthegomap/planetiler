package com.onthegomap.planetiler;

import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.TileCoord;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryFactory;

public class ScalableLandCoverTileProcessor implements ForwardingProfile.LayerPostProcesser,
  ForwardingProfile.TilePostProcessor {

  private static final int MAX_ZOOM = 14;
  private static final int RASTERIZE_ZOOM_THRESHOLD = 11;
  private static final GeometryFactory geometryFactory = new GeometryFactory();
  private static final int TILE_SIZE = 4096;

  @Override
  public String name() {
    return "land_cover";
  }

  @Override
  public List<VectorTile.Feature> postProcess(int zoom, List<VectorTile.Feature> items) throws GeometryException {
    if (zoom > MAX_ZOOM) {
      return items;
    }

    List<VectorTile.Feature> processedFeatures = new ArrayList<>();

    for (VectorTile.Feature feature : items) {
      VectorTile.Feature processedFeature = processFeature(feature, zoom);
      if (processedFeature != null) {
        processedFeatures.add(processedFeature);
      }
    }

    return processedFeatures;
  }

  @Override
  public Map<String, List<VectorTile.Feature>> postProcessTile(TileCoord tileCoord,
    Map<String, List<VectorTile.Feature>> layers) throws GeometryException {
    if (!layers.containsKey(name())) {
      return layers;
    }

    List<VectorTile.Feature> features = layers.get(name());
    List<VectorTile.Feature> processedFeatures = postProcess(tileCoord.z(), features);

    Map<String, List<VectorTile.Feature>> result = new HashMap<>(layers);
    result.put(name(), processedFeatures);
    return result;
  }

  private VectorTile.Feature processFeature(VectorTile.Feature feature, int zoom) throws GeometryException {
    Geometry geometry = feature.geometry().decode();

    if (zoom == MAX_ZOOM) {
      return feature;
    } else if (zoom <= RASTERIZE_ZOOM_THRESHOLD) {
      geometry = rasterizeGeometry(geometry, zoom, feature.id());
    } else {
      geometry = simplifyGeometry(geometry, zoom);
    }

    if (geometry.isEmpty()) {
      return null;
    }

    return feature.copyWithNewGeometry(geometry);
  }

  private Geometry rasterizeGeometry(Geometry geometry, int zoom, long featureId) {
    boolean[][] grid = new boolean[TILE_SIZE][TILE_SIZE];

    // 填充网格
    for (int x = 0; x < TILE_SIZE; x++) {
      for (int y = 0; y < TILE_SIZE; y++) {
        Geometry pixel = createPixel(x, y, TILE_SIZE);
        if (geometry.intersects(pixel)) {
          grid[x][y] = true;
        }
      }
    }

    // 将网格转换回几何形状
    return convertGridToGeometry(grid);
  }

  private Geometry createPixel(int x, int y, int gridSize) {
    double size = 1.0 / gridSize;
    return geometryFactory.toGeometry(new org.locationtech.jts.geom.Envelope(
      x * size, (x + 1) * size, y * size, (y + 1) * size
    ));
  }

  private Geometry convertGridToGeometry(boolean[][] grid) {
    List<Geometry> pixels = new ArrayList<>();
    for (int x = 0; x < grid.length; x++) {
      for (int y = 0; y < grid[x].length; y++) {
        if (grid[x][y]) {
          pixels.add(createPixel(x, y, grid.length));
        }
      }
    }
    return geometryFactory.buildGeometry(pixels).union();
  }

  private Geometry simplifyGeometry(Geometry geometry, int zoom) {
    double tolerance = calculateSimplificationTolerance(zoom);
    return org.locationtech.jts.simplify.TopologyPreservingSimplifier.simplify(geometry, tolerance);
  }

  private double calculateSimplificationTolerance(int zoom) {
    return 1.0 / (1 << zoom);
  }

  // 新增：处理多个要素重叠的情况
  private VectorTile.Feature resolveOverlappingFeatures(List<VectorTile.Feature> overlappingFeatures) {
    // 这里实现一个策略来决定哪个要素应该"获胜"
    // 例如，可以选择面积最大的要素，或者根据某些属性进行优先级排序
    // 为了示例，这里简单地选择第一个要素
    VectorTile.Feature feature = overlappingFeatures.get(0);
    return feature;
  }
}
