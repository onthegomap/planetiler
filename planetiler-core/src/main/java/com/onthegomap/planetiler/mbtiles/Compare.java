package com.onthegomap.planetiler.mbtiles;

import static com.onthegomap.planetiler.VectorTile.decode;
import static com.onthegomap.planetiler.util.Gzip.gunzip;

import com.google.common.collect.Sets;
import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.config.Arguments;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.TileCoord;
import java.nio.file.Path;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.locationtech.jts.geom.Geometry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A utility to compare two mbtiles files.
 * <p>
 * See {@link VectorTileFeatureForCmp} for comparison rules. The results planetiler produces are not necessarily stable,
 * so sometimes a few feature may be different in one tile. Also POI coordinates may sometimes differ slightly.
 * <p>
 * => The tool helps to see if two mbtiles files are mostly identical.
 *
 */
public class Compare {

  private static final Logger LOGGER = LoggerFactory.getLogger(Compare.class);

  public static void main(String[] args) throws Exception {


    Arguments arguments = Arguments.fromArgs(args);
    String dbPath0 = arguments.getString("bench_mbtiles0", "the first mbtiles file", null);
    String dbPath1 = arguments.getString("bench_mbtiles1", "the second mbtiles file", null);
    boolean failOnFeatureDiff = arguments.getBoolean("bench_fail_on_feature_diff", "fail on feature diff", false);

    try (
      var db0 = Mbtiles.newReadOnlyDatabase(Path.of(dbPath0));
      var db1 = Mbtiles.newReadOnlyDatabase(Path.of(dbPath1))
    ) {
      long tilesCount0 = getTilesCount(db0);
      long tilesCount1 = getTilesCount(db1);
      if (tilesCount0 != tilesCount1) {
        throw new IllegalArgumentException(
          "expected tiles count to be equal but tilesCount0=%d tilesCount1=%d".formatted(tilesCount0, tilesCount1)
        );
      }

      int lastPercentage = -1;
      long processedTileCounter = 0;
      long tilesWithDiffs = 0;
      try (var statement = db0.connection().prepareStatement("select tile_column, tile_row, zoom_level from tiles")) {
        var rs = statement.executeQuery();
        while (rs.next()) {
          processedTileCounter++;
          int x = rs.getInt("tile_column");
          int y = rs.getInt("tile_row");
          int z = rs.getInt("zoom_level");
          TileCoord coord = TileCoord.ofXYZ(x, (1 << z) - 1 - y, z);

          int currentPercentage = (int) (processedTileCounter * 100 / tilesCount0);
          if (lastPercentage != currentPercentage) {
            LOGGER.info("processed {}%", currentPercentage);
          }
          lastPercentage = currentPercentage;

          var features0 = decode(gunzip(db0.getTile(coord)))
            .stream()
            .map(VectorTileFeatureForCmp::fromActualFeature)
            .collect(Collectors.toSet());
          var features1 = decode(gunzip(db1.getTile(coord)))
            .stream()
            .map(VectorTileFeatureForCmp::fromActualFeature)
            .collect(Collectors.toSet());

          if (!features0.equals(features1)) {
            ++tilesWithDiffs;
            boolean featureCountMatches = features0.size() == features1.size();
            var msg = """
              <<<
              feature diff on coord %s - featureCountMatches: %b (%d vs %d)

              additional in db0
              ---
              %s

              additional in db1
              ---
              %s
              >>>
              """.formatted(
              coord, featureCountMatches, features0.size(), features1.size(),
              getDiffJoined(features0, features1, "\n"),
              getDiffJoined(features1, features0, "\n"));

            if (failOnFeatureDiff) {
              throw new RuntimeException(msg);
            } else {
              LOGGER.warn(msg);
            }
          }
        }
      }

      LOGGER.info("totalTiles={} tilesWithDiffs={}", processedTileCounter, tilesWithDiffs);
    }
  }

  private static long getTilesCount(Mbtiles db) throws SQLException {
    try (var statement = db.connection().createStatement()) {
      var rs = statement.executeQuery("select count(*) from tiles_shallow");
      rs.next();
      return rs.getLong(1);
    } catch (Exception e) {
      try (var statement = db.connection().createStatement()) {
        var rs = statement.executeQuery("select count(*) from tiles");
        rs.next();
        return rs.getLong(1);
      }
    }
  }

  private static <T> String getDiffJoined(Set<T> s0, Set<T> s1, String delimiter) {
    return Sets.difference(s0, s1).stream().map(Object::toString).collect(Collectors.joining(delimiter));
  }

  /**
   * Wrapper around {@link VectorTile.Feature} to compare vector tiles.
   * <ul>
   * <li>{@link VectorTile.Feature#id()} won't be compared
   * <li>{@link VectorTile.Feature#layer()} will be compared
   * <li>{@link VectorTile.Feature#geometry()} gets normalized for comparing
   * <li>{@link VectorTile.Feature#attrs()} will be compared except for the rank attribute since the value produced by
   * planetiler is not stable and differs on every run (at least for parks)
   * <li>{@link VectorTile.Feature#group()} will be compared
   * </ul>
   */
  private record VectorTileFeatureForCmp(
    String layer,
    Geometry normalizedGeometry,
    Map<String, Object> attrs,
    long group
  ) {
    static VectorTileFeatureForCmp fromActualFeature(VectorTile.Feature f) {
      try {
        var attrs = new HashMap<>(f.attrs());
        attrs.remove("rank");
        return new VectorTileFeatureForCmp(f.layer(), f.geometry().decode().norm(), attrs, f.group());
      } catch (GeometryException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
