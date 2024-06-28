package com.onthegomap.planetiler.mbtiles;

import static com.onthegomap.planetiler.util.Gzip.gunzip;

import com.onthegomap.planetiler.VectorTile;
import com.onthegomap.planetiler.geo.GeometryException;
import com.onthegomap.planetiler.geo.TileCoord;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Supplier;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.geom.Geometry;
import org.locationtech.jts.geom.GeometryCollection;
import org.locationtech.jts.geom.Polygon;

/**
 * A utility to verify the contents of an mbtiles file.
 * <p>
 * {@link #verify(Mbtiles)} does a basic set of checks that the schema is correct and contains a "name" attribute and at
 * least one tile. Other classes can add more tests to it.
 */
public class Verify {

  private static final String GOOD = "\u001B[32m✓\u001B[0m";
  private static final String BAD = "\u001B[31m✕\u001B[0m";

  private final List<Check> checks = new ArrayList<>();
  private final Mbtiles mbtiles;

  private Verify(Mbtiles mbtiles) {
    this.mbtiles = mbtiles;
  }

  public static void main(String[] args) throws IOException {
    try (var mbtiles = Mbtiles.newReadOnlyDatabase(Path.of(args[0]))) {
      var result = Verify.verify(mbtiles);
      result.print();
      result.failIfErrors();
    }
  }

  /**
   * Returns the number of features in a layer inside a lat/lon bounding box with a geometry type and attributes.
   *
   * @param db       the mbtiles file
   * @param layer    the layer to check
   * @param zoom     zoom level of tiles to check
   * @param attrs    partial set of attributes to filter features
   * @param envelope lat/lon bounding box to limit check
   * @param clazz    {@link Geometry} subclass to limit
   * @return number of features found
   * @throws GeometryException if an invalid geometry is encountered
   */
  public static int getNumFeatures(Mbtiles db, String layer, int zoom, Map<String, Object> attrs, Envelope envelope,
    Class<? extends Geometry> clazz) throws GeometryException {
    int num = 0;
    try (var tileCoords = db.getAllTileCoords()) {
      while (tileCoords.hasNext()) {
        var tileCoord = tileCoords.next();
        Envelope tileEnv = new Envelope();
        tileEnv.expandToInclude(tileCoord.lngLatToTileCoords(envelope.getMinX(), envelope.getMinY()));
        tileEnv.expandToInclude(tileCoord.lngLatToTileCoords(envelope.getMaxX(), envelope.getMaxY()));
        if (tileCoord.z() == zoom) {
          byte[] data = db.getTile(tileCoord);
          for (var feature : decode(data)) {
            if (layer.equals(feature.layer()) && feature.tags().entrySet().containsAll(attrs.entrySet())) {
              Geometry geometry = feature.geometry().decode();
              num += getGeometryCounts(geometry, clazz);
            }
          }
        }
      }
    }
    return num;
  }

  private static int getGeometryCounts(Geometry geom, Class<? extends Geometry> clazz) {
    int count = 0;
    if (geom instanceof GeometryCollection geometryCollection) {
      for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
        count += getGeometryCounts(geometryCollection.getGeometryN(i), clazz);
      }
    } else if (clazz.isInstance(geom)) {
      count = 1;
    }
    return count;
  }

  private static List<VectorTile.Feature> decode(byte[] zipped) {
    try {
      return VectorTile.decode(gunzip(zipped));
    } catch (IOException e) {
      throw new UncheckedIOException(e);
    }
  }

  /**
   * Returns a verification result of a basic set of checks on an mbtiles file:
   * <ul>
   * <li>has a metadata and tiles table</li>
   * <li>has a name metadata attribute</li>
   * <li>has at least one tile</li>
   * <li>all vector tile geometries are valid</li>
   * </ul>
   */
  public static Verify verify(Mbtiles mbtiles) {
    Verify result = new Verify(mbtiles);
    result.checkBasicStructure();
    return result;
  }

  private static boolean isValid(Geometry geom) {
    if (geom instanceof Polygon polygon) {
      return polygon.isSimple();
    } else if (geom instanceof GeometryCollection geometryCollection) {
      for (int i = 0; i < geometryCollection.getNumGeometries(); i++) {
        if (!isValid(geom.getGeometryN(i))) {
          return false;
        }
      }
    }
    return true;
  }

  /**
   * Adds a check to this verification result per zoom-level that succeeds if at least {@code minCount} features are
   * found matching the provided criteria.
   *
   * @param bounds       lat/lon bounding box to limit check
   * @param layer        the layer to check
   * @param tags         partial set of attributes to filter features
   * @param minzoom      min zoom level of tiles to check
   * @param maxzoom      max zoom level of tiles to check
   * @param minCount     minimum number of required features
   * @param geometryType {@link Geometry} subclass to limit matches to
   */
  public void checkMinFeatureCount(Envelope bounds, String layer, Map<String, Object> tags, int minzoom, int maxzoom,
    int minCount, Class<? extends Geometry> geometryType) {
    for (int z = minzoom; z <= maxzoom; z++) {
      checkMinFeatureCount(bounds, layer, tags, z, minCount, geometryType);
    }
  }

  /**
   * Adds a check to this verification result that succeeds if at least {@code minCount} features are found matching the
   * provided criteria.
   *
   * @param bounds       lat/lon bounding box to limit check
   * @param layer        the layer to check
   * @param tags         partial set of attributes to filter features
   * @param zoom         zoom level of tiles to check
   * @param minCount     minimum number of required features
   * @param geometryType {@link Geometry} subclass to limit matches to
   */
  public void checkMinFeatureCount(Envelope bounds, String layer, Map<String, Object> tags, int zoom, int minCount,
    Class<? extends Geometry> geometryType) {
    checkWithMessage("at least %d %s %s features at z%d".formatted(minCount, layer, tags, zoom), () -> {
      try {
        int count = getNumFeatures(mbtiles, layer, zoom, tags, bounds, geometryType);
        return count >= minCount ? Optional.empty() : Optional.of("found " + count);
      } catch (GeometryException e) {
        return Optional.of("error: " + e);
      }
    });
  }

  /** Logs verification results. */
  public void print() {
    for (Check check : checks) {
      check.error.ifPresentOrElse(
        error -> System.out.println(BAD + " " + check.name + ": " + error),
        () -> System.out.println(GOOD + " " + check.name)
      );
    }
  }

  /** Exits with a nonzero exit code if there were any failures. */
  public void failIfErrors() {
    long errors = numErrors();
    System.out.println(errors + " errors");
    if (errors > 0) {
      System.exit(1);
    }
  }

  private void checkBasicStructure() {
    check("contains name attribute", () -> mbtiles.metadata().toMap().containsKey("name"));
    check("contains at least one tile", () -> mbtiles.getAllTileCoords().stream().findAny().isPresent());
    checkWithMessage("all tiles are valid", () -> {
      List<String> invalidTiles = mbtiles.getAllTileCoords().stream()
        .flatMap(coord -> checkValidity(coord, decode(mbtiles.getTile(coord))).stream())
        .toList();
      return invalidTiles.isEmpty() ? Optional.empty() :
        Optional.of(invalidTiles.size() + " invalid tiles: " + invalidTiles.stream().limit(5).toList());
    });
  }

  private Optional<String> checkValidity(TileCoord coord, List<VectorTile.Feature> features) {
    for (var feature : features) {
      try {
        Geometry geometry = feature.geometry().decode();
        if (!isValid(geometry)) {
          return Optional.of(coord + "/" + feature.layer());
        }
      } catch (GeometryException e) {
        return Optional.of(coord + " error decoding " + feature.layer() + "feature");
      }
    }
    return Optional.empty();
  }

  private void checkWithMessage(String name, Supplier<Optional<String>> check) {
    try {
      checks.add(new Check(name, check.get()));
    } catch (Throwable e) {
      checks.add(new Check(name, Optional.of(e.toString())));
    }
  }

  private void check(String name, Supplier<Boolean> check) {
    checkWithMessage(name, () -> check.get() ? Optional.empty() : Optional.of("false"));
  }

  public List<Check> results() {
    return checks;
  }

  public long numErrors() {
    return checks.stream().filter(check -> check.error.isPresent()).count();
  }

  public record Check(String name, Optional<String> error) {}
}
